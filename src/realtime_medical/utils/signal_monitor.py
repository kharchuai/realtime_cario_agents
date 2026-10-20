"""Đo signal gap/age từng raw field ngay cả khi không có message mới."""

from __future__ import annotations

import hashlib
import threading
import time

from src.realtime_medical.utils.benchmark_hooks import valid_fields

class SignalMonitor:
    """Theo dõi từng ca/trường; không refresh state bằng forward-fill."""
    def __init__(self,  metrics, config, interval_s = 1.0):
        """
        Args:
            metrics: Collector consumer, dùng chung an toàn giữa các thread.
            config: AppConfig để kiểm tra raw fields.
            interval_s: Chu kỳ lấy mẫu wall-clock, phải > 0.
        """
        if interval_s <= 0:
            raise ValueError("interval_s must be > 0")
        self.metrics = metrics
        self.config = config
        self.interval_s = interval_s
        self._lock = threading.Lock()
        self._registered, self._last_valid = {}, {}
        self._stop = threading.Event()
        self._thread, self._error = None, None

    def register(self, surgery_id, fields):
        """Đăng ký ca/trường theo lịch nguồn trước reading đầu tiên.

        Args:
            surgery_id: Ca đang theo dõi.
            fields: Các trường thực sự kỳ vọng được đo.

        Notes:
            Đăng ký trước mới quan sát được trường hợp chưa từng có tín hiệu.
        """
        now_ns = time.time_ns()
        with self._lock:
            for field in fields:
                self._registered.setdefault((surgery_id, field), now_ns)

    def update_signal(self, surgery_id, vitals, source_ns):
        """Cập nhật từ raw sample hợp lệ, trước forward-fill.

        Args:
            surgery_id: Ca nguồn.
            vitals: Reading raw.
            source_ns: Timestamp nguồn; replay dùng _bench.t0_ns.

        Notes:
            Mẫu cũ/duplicate không làm tín hiệu cũ trông như mới.
            Nếu các trường đo khác thời điểm, mở rộng thành timestamp từng trường.
        """
        received_ns = time.time_ns()
        with self._lock:
            for field in valid_fields(vitals, self.config):
                key = (surgery_id, field)
                if key not in self._registered:
                    continue
                previous = self._last_valid.get(key)
                if previous is None or  source_ns > previous["source_ns"]:
                    self._last_valid[key] = {
                        "source_ns":source_ns,
                        "received_ns":received_ns,
                    }

    def sample_signals(self):
        """Ghi M2 độc lập với message: age, receive gap và never_seen.

        Notes:
            age = now − timestamp nguồn mới nhất.
            receive gap = now − lúc nhận mẫu nguồn mới nhất.
            never_seen dùng duration_without_sample_s, age/gap=None.
            Clock anomaly được ghi riêng, không âm thầm clamp về 0.
        """
        now_ns =  time.time_ns()
        with self._lock:
            registered = dict(self._registered)
            snapshot =  {k : dict(v) for k,v in self._last_valid.items()}
        for (surgery_id, field), started_ns in registered.items():
            item = snapshot.get((surgery_id, field))
            stream_id = hashlib.sha256(
                f"{self.metrics.run_id}|{surgery_id}|{field}".encode()
            ).hexdigest()
            age = (now_ns -  item["source_ns"]) / 1e9 if item else None
            gap  = (now_ns - item["received_ns"]) / 1e9 if item else None
            self.metrics.trace(
                "signal_sample", stream_id, at_ns=now_ns, field=field, surgery_id=surgery_id,
                status = (
                    "never_seen" if item is None else
                    "clock_anomaly" if age < 0 or gap < 0 else "seen"
                ),
                signal_age_s = age,
                receive_gap_s = gap,
                duration_without_sample_s = (
                    (now_ns - started_ns) / 1e9 if item is None else None
                ),
            )

    def start(self):
        """Chạy thread lấy mẫu một lần; close trước khi đóng collector."""
        if self._thread is not None:
            raise RuntimeError("SignalMonitor has already started")
        self._thread = threading.Thread(
            target=self._run, name = "benchmark-signal-monitor", daemon = True,
        )
        self._thread.start()

    def _run(self):
        """Lấy mẫu định kỳ và giữ lỗi để báo khi kết thúc run."""
        try:
            while not self._stop.is_set():
                self.sample_signals()
                self._stop.wait(self.interval_s)
        except Exception as error:
            self._error = error

    def close(self):
        """Dừng/join thread, ghi mẫu cuối và báo nếu giám sát thất bại."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        if self._error is not None:
            raise RuntimeError("SignalMonitor failed") from self._error
        self.sample_signals()
