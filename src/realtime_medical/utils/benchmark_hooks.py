"""Ghi expectation và trace cho benchmark vitals/alerts."""

from __future__ import annotations

import hashlib
import math
import os
import uuid
from datetime import  datetime, timezone
from pathlib import Path
from src.realtime_medical.utils.metrics import AuditedMetricsRecorder

VITAL_DEADLINE_S = 5.0
ALERT_DEADLINE_S = 5.0
CRITICAL_DETECTION_DEADLINE_S = 2.0

def make_metrics(component):
    """Mở collector tùy theo BENCH_ENABLED; mặc định không đo.

    Args:
        component: runner, producer, consumer hoặc fault.

    Returns:
        AuditedMetricsRecorder có enabled theo cấu hình môi trường.

    Raises:
        ValueError: Bật đo nhưng thiếu BENCH_RUN_ID.
        OSError: Không mở được file/thư mục.

    Notes:
        BENCH_ENABLED nhận 1/true/yes/on để bật, không phân biệt hoa thường.
        Khi tắt, không yêu cầu cấu hình run và không tạo file metrics.
        Khi bật, mặc định lưu tại tmp/benchmark trong repository.
        Run ID phải được truyền chung cho runner, producer và consumer.
        Tạo một lần cho mỗi vai trò, không tạo mỗi reading.
        UUID tránh trùng tên khi container restart và tái sử dụng PID.
        Caller đóng collector sau khi tất cả thread/callback đã dừng.
    """
    enabled = os.environ.get("BENCH_ENABLED", "0").strip().lower() in {
        "1", "true", "yes", "on",
    }
    if enabled:
        if not os.environ.get("BENCH_RUN_ID", "").strip():
            raise ValueError(
                "Missing BENCH_RUN_ID. Start the run through scripts/run_pipeline.py "
                "so runner and consumer share the same run ID."
            )
        if not os.environ.get("METRICS_DIR", "").strip():
            project_root = Path(__file__).resolve().parents[3]
            os.environ["METRICS_DIR"] = str(project_root / "tmp" / "benchmark")
    return AuditedMetricsRecorder(
        output_dir = os.environ["METRICS_DIR"] if enabled else ".",
        run_id = os.environ["BENCH_RUN_ID"] if enabled else "disabled",
        component = f"{component}-{uuid.uuid4().hex[:8]}",
        enabled = enabled,
    )

def record_id(run_id, surgery_id, recorded_at):
    """Tạo ID logic ổn định từ run, ca mổ và timestamp chuẩn hóa UTC.

    Args:
        run_id: ID chung của lượt chạy.
        surgery_id: Khóa ca mổ nguồn.
        recorded_at: ISO 8601 có timezone hoặc hậu tố Z.

    Returns:
        Chuỗi SHA-256; retry/replay cùng khóa logic cho cùng ID.

    Raises:
        ValueError: Timestamp lỗi hoặc thiếu timezone.

    Notes:
        Không dùng Kafka offset làm ID logic vì republish tạo offset mới.
    """
    dt = datetime.fromisoformat(str(recorded_at).replace("Z",  "+00:00"))
    if dt.utcoffset() is None:
        raise ValueError("recorded_at must include a timezone")

    normalized = dt.astimezone(timezone.utc).isoformat(timespec = "microseconds")
    raw = f"{run_id}|{surgery_id}|{normalized}"
    return hashlib.sha256(raw.encode()).hexdigest()

def logical_alert_id(rid):
    """Trả ID alert ổn định theo chính sách một alert tổng hợp trên mỗi reading.

    Args:
        rid: ID logic của reading nguồn.

    Returns:
        ID giữ nguyên khi retry cùng alert.
    """
    return f"{rid}:triage"

def valid_fields(vitals,config):
    """Liệt kê raw fields là số hữu hạn và qua artifact range đã cấu hình.

    Args:
        vitals: Reading raw trước forward-fill.
        config: AppConfig chứa streaming.artifact_filtering.

    Returns:
        List tên trường sinh hiệu hợp lệ.

    Notes:
        Chỉ áp dụng range hiện có, không thêm ngưỡng y khoa.
        Forward-fill không được tính là một mẫu cảm biến mới.
    """
    result = []
    for field in  config.canonical_fields.model_dump().values():
        value = vitals.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        if not math.isfinite(value):
            continue
        bounds = config.streaming.artifact_filtering.get(field)
        if bounds and not(bounds.valid_min <= value <= bounds.valid_max):
            continue
        result.append(field)
    return result

def expect_reading(metrics, surgery_id, vitals, *, t0_ns, critical = False, alert_expected = False, required_fields = ("hr", "spo2"), probe = False):
    """Ghi đầu vào kỳ vọng trước fault rồi trả payload có metadata benchmark.

    Args:
        metrics: Collector runner/controller độc lập với consumer.
        surgery_id: Ca mổ nguồn.
        vitals: Reading gốc trước khi drop, delay hoặc bỏ trường.
        t0_ns: Epoch nanoseconds thời điểm sinh mẫu hoặc lịch đã định.
        critical: Nhãn critical positive đã xác định độc lập với consumer.
        alert_expected: Có kỳ vọng alert cho reading này không.
        required_fields: Trường phải có theo hợp đồng nguồn.
        probe: Reading thuộc lịch probe đều để đo recovery.

    Returns:
        Dict mới với _bench.run_id và _bench.t0_ns; không sửa input.

    Raises:
        ValueError: Critical positive nhưng không kỳ vọng alert; timestamp lỗi.

    Notes:
        Gọi một lần trên mỗi reading logic. Retry dùng lại payload này.
        Với test nguồn bị kill, lập toàn bộ lịch expectation trước khi fault.
        Khi tắt đo, trả bản sao input và không thêm metadata benchmark.
    """
    if not metrics.enabled:
        return dict(vitals)
    if critical and not alert_expected:
        raise ValueError("Critical fixture must expect an alert")
    rid =  record_id(metrics.run_id, surgery_id, vitals["recorded_at"])

    metrics.trace(
        "expected_vital", rid,
        t0_ns = t0_ns,
        deadline_ns = t0_ns + int(VITAL_DEADLINE_S * 1e9),
        critical_deadline_ns = t0_ns + int(CRITICAL_DETECTION_DEADLINE_S * 1e9),
        critical = critical,
        required_fields = list(required_fields),
        probe = probe,
    )
    if alert_expected:
        metrics.trace(
            "expected_alert", rid, event_id = logical_alert_id(rid),
            t0_ns = t0_ns,
            deadline_ns = t0_ns + int(ALERT_DEADLINE_S * 1e9),
            probe = probe,
        )

    return {
        **vitals,
        "_bench": {"run_id": metrics.run_id, "t0_ns": t0_ns}
    }

def mark_vitals_persisted(metrics, config, rows, ack_ns):
    """Ghi ACK lưu vitals cho từng row sau khi lời gọi DB thành công.

    Args:
        metrics: Collector consumer.
        config: AppConfig để kiểm tra raw fields.
        rows: List (surgery_id, vitals) được DB xử lý thành công.
        ack_ns: Epoch nanoseconds lấy ngay khi lời gọi DB trả thành công.

    Notes:
        Gọi ở cả batch-success và từng row-success trong fallback.
        ON CONFLICT DO NOTHING cũng chứng minh reading đã tồn tại.
        Không gọi cho row lỗi hoặc chỉ vừa append buffer.
        Khi tắt đo, không tính ID hoặc kiểm tra fields.
    """
    if not metrics.enabled:
        return
    for surgery_id, vitals in rows:
        rid = record_id(metrics.run_id, surgery_id, vitals["recorded_at"])
        metrics.trace(
            "vitals_persisted", rid,
            at_ns = ack_ns,
            valid_fields = valid_fields(vitals, config),
        )

def  mark_alerts_persisted(metrics, alerts, ack_ns):
    """Ghi ACK lưu alerts sau khi lời gọi DB thành công.

    Args:
        metrics: Collector consumer.
        alerts: List alert dict đã ghi thành công.
        ack_ns: Epoch nanoseconds ngay sau lời gọi DB.

    Notes:
        Retry giữ cùng logical_alert_id. Analyzer lấy ACK đầu tiên.
        Duplicate vật lý phải được đếm từ DB, không chỉ từ trace.
        Khi tắt đo, không tính ID hoặc đọc metadata của alert.
    """
    if not metrics.enabled:
        return
    for alert in alerts:
        rid = record_id(metrics.run_id, alert["surgery_id"], alert["recorded_at"])
        metrics.trace(
            "alert_persisted", rid,
            event_id = logical_alert_id(rid),
            at_ns = ack_ns,
        )
