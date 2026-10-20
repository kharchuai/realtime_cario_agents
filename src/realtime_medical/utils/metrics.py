"""
Thu thập và tổng hợp metrics phục vụ benchmark pipeline realtime.

Hỗ trợ:
    - Counter: số bản ghi, số lỗi, số lần fallback.
    - Gauge: kích thước queue, buffer, mức sử dụng tài nguyên.
    - Observation: các mẫu latency, kích thước batch.
    - Trace: nối các giai đoạn xử lý bằng record_id/event_id.

Mỗi process ghi một file JSONL riêng. Các thread trong cùng process
có thể dùng chung một MetricsRecorder.

Dữ liệu được flush sau mỗi lần ghi. Cơ chế này tạo thêm chi phí I/O
và không bảo đảm lưu bền vững khi máy mất điện.

Module chưa tự tính data loss, recovery time hoặc end-to-end latency.
Các chỉ số đó cần benchmark runner đối chiếu trace và đầu ra thực tế.
"""

from __future__ import annotations

import json
import math
import os
import threading
import time

from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator

def percentile(values: list[float], percent: float):
    if not values:
        return None

    ordered = sorted(values)
    position = (len(ordered) - 1) *  percent / 100
    lower = math.floor(position)
    upper = math.ceil(position)

    if lower == upper:
        return ordered[lower]

    weight = position - lower
    return(
        ordered[lower]*(1-weight) + ordered[upper]*weight
    )

class MetricsRecorder:
    """
    Ghi metrics và trace vào JSONL, an toàn với nhiều thread.

    File đầu ra có dạng:
        <output_dir>/<run_id>/<component>-<pid>.jsonl

    Attributes:
        run_id: Định danh lượt benchmark.
        component: Thành phần phát sinh metrics.
        enabled: Cho phép bật/tắt instrumentation.
        path: Đường dẫn file đầu ra; None khi tắt instrumentation.

    Notes:
        Mỗi process phải sử dụng collector/file riêng.
        record_id và event_id thuộc trace, không dùng làm metric labels.
        Lỗi ghi file được đưa lên caller để tránh bỏ mất dữ liệu đo
        mà benchmark không biết.
    """

    def __init__(self, output_dir: str | Path, run_id:str, component: str, enabled:  bool = True):
        """
        Khởi tạo collector và mở file JSONL mới.

        Args:
            output_dir: Thư mục gốc chứa kết quả benchmark.
            run_id: Tên lượt chạy, không chứa ký tự phân cách đường dẫn.
            component: Tên thành phần, ví dụ producer hoặc consumer.
            enabled: False để bỏ qua việc ghi metrics.

        Raises:
            ValueError: run_id hoặc component không hợp lệ.
            FileExistsError: File cho component/process đã tồn tại.
            OSError: Không thể tạo thư mục hoặc mở file.

        Notes:
            Không nối vào file cũ để tránh trộn các lượt benchmark.
        """

        for value in (run_id, component):
            if (
                not value
                or value in {".", ".."}
                or any(
                    char in value for char in '/\\<>:"|?*\0'
                )
            ):
                raise ValueError("Invalid run_id/component")

        self.run_id = run_id
        self.component = component
        self.enabled = enabled
        self._lock = threading.Lock()
        self._closed = False
        self._file = None
        self.path: Path | None = None

        if enabled:
            directory = Path(output_dir) / run_id
            directory.mkdir(parents = True, exist_ok=True)

            self.path = directory / (f"{component}-{os.getpid()}.jsonl")
            self._file = self.path.open("x", encoding = "utf-8", buffering = 1)

    def _write(self, kind:str, name: str, **fields):
        """
        Ghi một dòng JSONL cùng metadata của lượt benchmark.

        Args:
            kind: Loại dữ liệu: counter, gauge, observation hoặc trace.
            name: Tên metric hoặc giai đoạn trace.
            **fields: Nội dung bổ sung có thể chuyển thành JSON.

        Raises:
            RuntimeError: Collector đã đóng.
            TypeError: Nội dung không thể chuyển thành JSON.
            ValueError: Nội dung chứa NaN hoặc giá trị vô hạn.
            OSError: Ghi hoặc flush file thất bại.

        Notes:
            time_ns là thời gian hệ thống tại lúc tạo bản ghi đo.
            Lock bảo vệ thao tác ghi giữa các thread.
            Khi enabled=False, hàm không ghi dữ liệu.
        """
        if not self.enabled:
            return

        row = {
            "run_id": self.run_id,
            "component": self.component,
            "pid": os.getpid(),
            "time_ns": time.time_ns(),
            "kind": kind,
            "name": name,
            **fields,
        }

        line = json.dumps(
            row,
            ensure_ascii  = False,
            allow_nan = False,
            separators = (",", ":")
        )

        with self._lock:
            if self._closed:
                raise RuntimeError("MetricsRecorder has already been used")

            self._file.write(line + "\n")
            self._file.flush()

    @staticmethod
    def _finite(value) -> float:
        """
        Chuyển giá trị sang float và kiểm tra tính hữu hạn.

        Args:
            value: Giá trị cần kiểm tra.

        Returns:
            Giá trị float hữu hạn.

        Raises:
            TypeError: Giá trị không thể chuyển sang float.
            ValueError: Giá trị không hợp lệ, NaN hoặc vô hạn.
        """

        result = float(value)

        if not math.isfinite(result):
            raise ValueError("Metric must be a finite number")
        return result

    def increment(self, name:str, amount: int = 1, labels: dict | None = None ):
        """
        Ghi một lượng tăng của counter.

        Args:
            name: Tên counter, ví dụ delivery_success_total.
            amount: Lượng tăng là số nguyên không âm.
            labels: Nhãn phân loại có số lượng giá trị giới hạn.

        Raises:
            ValueError: amount không phải số nguyên không âm.

        Notes:
            Counter tổng được tính bằng cách cộng các lượng tăng.
            Không dùng record_id hoặc event_id làm labels.
        """
        if (
            isinstance(amount, bool)
            or not isinstance(amount, int)
            or amount < 0
        ):
            raise ValueError("Counter increment must be an integer >= 0")
        self._write("counter", name,  value = amount, labels = dict(labels or {}))

    def gauge(self, name: str, value: float, labels: dict| None = None):
        """
        Ghi giá trị tức thời của một metric.

        Args:
            name: Tên gauge, ví dụ agent_queue_depth.
            value: Giá trị hữu hạn tại thời điểm lấy mẫu.
            labels: Nhãn phân loại metric.

        Notes:
            Gauge có thể tăng hoặc giảm.
            Cần lấy mẫu định kỳ để quan sát diễn biến theo thời gian.
        """
        self._write("gauge", name, value = self._finite(value), labels = dict(labels or {}))

    def observe(self, name:str, value:float, labels:dict | None = None):
        """
        Ghi một mẫu để tính phân bố thống kê sau lượt chạy.

        Args:
            name: Tên metric, nên chứa đơn vị như latency_ms.
            value: Giá trị mẫu hữu hạn.
            labels: Nhãn phân loại metric.

        Notes:
            Mỗi lần gọi tương ứng một mẫu.
            summarize_metrics tính mean, min, max và percentile.
        """
        self._write("observation", name, value = self._finite(value), labels = dict(labels or {}))

    def trace(self,  stage: str, record_id: str, event_id: str | None = None, **details):
        """
        Ghi mốc xử lý để nối các bước của cùng một bản ghi.

        Args:
            stage: Giai đoạn, ví dụ consumer_received hoặc db_committed.
            record_id: ID ổn định của bản ghi, giữ nguyên khi retry.
            event_id: ID sự kiện cảnh báo hoặc agent nếu có.
            **details: Metadata bổ sung có thể chuyển thành JSON.

        Raises:
            ValueError: record_id rỗng.

        Notes:
            Trace giữa nhiều máy yêu cầu đồng bộ đồng hồ.
            Không đưa thông tin định danh bệnh nhân vào details.
        """
        if not record_id:
            raise ValueError("Trace requires record_id")

        self._write("trace", stage, record_id = record_id, event_id = event_id, details = details)

    @contextmanager
    def timer(self, name:str, labels: dict|None = None) -> Iterator[None]:
        """
        Đo thời gian thực thi một khối lệnh, tính bằng milliseconds.

        Args:
            name: Tên observation, ví dụ ml_inference_latency_ms.
            labels: Nhãn bổ sung; nhãn status được hàm tự gán.

        Yields:
            Quyền thực thi khối lệnh bên trong with.

        Notes:
            Dùng perf_counter_ns để đo khoảng thời gian trong process.
            Ghi status=ok nếu hoàn tất, status=error nếu có ngoại lệ.
            Ngoại lệ từ khối lệnh được đưa lên caller.
            Lỗi ghi metrics trong finally có thể thay thế ngoại lệ gốc.
            Khi tắt instrumentation, không lấy mẫu thời gian.

        Example:
            with metrics.timer("rules_latency_ms"):
                risk_level, reasons = rule_engine.evaluate(vitals)
        """
        if not self.enabled:
            yield
            return

        started = time.perf_counter_ns()
        status = "ok"

        try:
            yield
        except:
            status = "error"
            raise
        finally:
            elapsed_ms =  (
                time.perf_counter_ns() - started
            ) / 1_000_000

            sample_labels = dict(labels or {})
            sample_labels["status"] = status

            self.observe(name, elapsed_ms, sample_labels)

    def close(self) -> None:
        """
        Đóng file metrics và ngừng tiếp nhận dữ liệu đo.

        Notes:
            Có thể gọi nhiều lần.
            Caller cần ngừng các thread ghi metrics trước khi đóng.
            Không thực hiện fsync để bảo đảm dữ liệu trên thiết bị lưu.
        """
        with self._lock:
            if self._closed:
                return

            self._closed = True

            if self._file is not None:
                self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

def summarize_metrics(paths: Iterable[str| Path], run_id:str ) -> dict:
    """
    Tổng hợp các file metrics thuộc cùng một lượt benchmark.

    Args:
        paths: Các đường dẫn JSONL; đường dẫn trùng được loại bỏ.
        run_id: Định danh lượt chạy cần tổng hợp.

    Returns:
        Dictionary gồm:
            run_id: Định danh lượt chạy.
            counters: Tổng lượng tăng theo metric và labels.
            distributions: Số mẫu, mean, min, max, p50, p95, p99.
            gauges: Số mẫu, min, max và mẫu mới nhất theo timestamp.
            trace_count: Số dòng trace đọc được.
            truncated_lines: Số dòng cuối không hoàn tất.
            measurement_complete: Không phát hiện dòng bị cắt.

    Raises:
        ValueError: JSON lỗi, run_id không khớp hoặc kind không hợp lệ.
        OSError: Không thể đọc file đầu vào.

    Notes:
        Percentile được tính từ toàn bộ mẫu cùng tên và labels.
        Không lấy trung bình percentile giữa các process.
        Gauge cùng tên và labels được gộp; thêm nhãn component
        nếu cần phân biệt từng thành phần.
        Các observation được giữ trong RAM, phù hợp benchmark ngắn.
        Trace chưa được đối chiếu để tính mất dữ liệu hoặc latency.
        measurement_complete không chứng minh mọi dữ liệu đo đã có:
        process có thể chết trước khi kịp ghi một mẫu.
    """

    counters = defaultdict(int)
    observations = defaultdict(list)
    gauges = {}
    trace_count = 0
    truncated_lines = 0

    def metric_key(row: dict) -> str:
        """
        Tạo khóa nhóm từ tên metric và labels theo thứ tự ổn định.

        Args:
            row: Bản ghi metrics đã đọc từ JSONL.

        Returns:
            Tên metric, kèm labels nếu có.
        """
        labels =  row.get("labels",  {})
        if not labels:
            return row["name"]

        label_text =  json.dumps(
            labels,
            sort_keys =  True,
            ensure_ascii = False,
            separators = (",",  ":"),
        )
        return f"{row['name']} {label_text}"

    unique_paths = dict.fromkeys(
        Path(path).resolve() for path in paths
    )

    for path in unique_paths:
        with path.open("r", encoding = "utf-8")  as source:
            for line_number, line in enumerate(source,  start = 1):
                if not line.endswith("\n"):
                    truncated_lines += 1
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(f"Invalid JSONL: {path}:{line_number}") from error

                if row.get("run_id") != run_id:
                    raise  ValueError(f"File contains a different run_id: {path}:{line_number}")

                kind = row["kind"]

                if kind == "trace":
                    trace_count += 1
                    continue

                key = metric_key(row)
                value = row["value"]

                if kind == "counter":
                    counters[key] += value
                elif kind == "observation":
                    observations[key].append(value)
                elif kind == "gauge":
                    if key not in gauges:
                        gauges[key] = {
                            "sample":0,
                            "min":value,
                            "max":value,
                            "last":value,
                            "last_time_ns":  row["time_ns"]
                        }
                    item = gauges[key]
                    item["sample"] +=  1
                    item["min"] = min(item["min"],  value)
                    item["max"] = max(item["max"],  value)

                    if  row["time_ns"] >=  item["last_time_ns"]:
                        item["last"] = value
                        item["last_time_ns"] = row["time_ns"]
                else:
                    raise ValueError(f"Invalid metric kind: {kind}")
    distributions=  {}

    for key, values in observations.items():
        distributions[key] = {
            "samples": len(values),
            "mean": sum(values) / len(values),
            "min":  min(values),
            "max": max(values),
            "p50": percentile(values, 50),
            "p95": percentile(values, 95),
            "p99": percentile(values, 99),
        }
    return {
        "run_id": run_id,
        "counters": dict(counters),
        "distributions": distributions,
        "gauges": gauges,
        "trace_count": trace_count,
        "truncated_lines": truncated_lines,
        "measurement_complete": truncated_lines == 0,
    }

class AuditedMetricsRecorder(MetricsRecorder):
    """Cô lập lỗi ghi; ghi trạng thái đo riêng lúc mở/đóng collector.

    Sau lỗi ghi đầu tiên, không ghi tiếp JSONL vì có thể có dòng dở.
    Health sidecar không phải bảo đảm chống mất điện; thiếu hoặc chưa
    closed phải được runner coi là kết quả chưa hoàn tất.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._health_lock = threading.Lock()
        self._attempted = 0
        self._written = 0
        self._dropped = 0
        self._write_failures = 0
        self._failed = False
        self._close_failed = False
        self._last_error = None
        self._health_path = (
            self.path.with_suffix(".health.json") if self.path else None
        )
        if self.enabled:
            try:
                self._save_health(closed=False)
            except Exception:
                super().close()
                raise

    def _save_health(self, closed):
        """Ghi snapshot độc lập bằng replace, không qua chính collector."""
        data = {
            "run_id": self.run_id,
            "component": self.component,
            "data_file": self.path.name,
            "closed": closed,
            "attempted_rows": self._attempted,
            "written_rows": self._written,
            "dropped_rows": self._dropped,
            "write_failures": self._write_failures,
            "close_failed": self._close_failed,
            "last_error": self._last_error,
        }
        temporary = self._health_path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(data, ensure_ascii=False), encoding="utf-8"
        )
        temporary.replace(self._health_path)

    def _write(self, kind, name, **fields):
        """Đếm lỗi/drop mà không phát sinh exception vào logic nghiệp vụ."""
        if not self.enabled:
            return
        with self._health_lock:
            self._attempted += 1
            if self._failed or self._closed:
                self._dropped += 1
                return
            try:
                super()._write(kind, name, **fields)
            except Exception as error:
                self._write_failures += 1
                self._dropped += 1
                self._failed = True
                self._last_error = type(error).__name__
            else:
                self._written += 1

    def close(self):
        """Đóng JSONL và ghi health cuối; sidecar lỗi thì đóng báo thất bại."""
        with self._health_lock:
            try:
                super().close()
            except Exception as error:
                self._close_failed = True
                self._last_error = type(error).__name__
            if self.enabled:
                self._save_health(closed=True)
