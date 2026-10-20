"""Fault injection có chủ ý, chỉ bật trong môi trường benchmark."""

from __future__ import  annotations

import subprocess
import time
import uuid

def pause_fault(metrics, container, duration_s = 30):
    """Pause/unpause container và ghi mốc fault cho M7/M8/M9.

    Args:
        metrics: Collector controller bên ngoài container bị pause.
        container: medical_consumer, medical_postgres hoặc medical_kafka.
        duration_s: Số giây pause, không âm.

    Returns:
        fault_id dùng ghép requested/applied/restore.

    Raises:
        ValueError: duration_s âm.
        subprocess.SubprocessError: Docker lỗi hoặc timeout.

    Notes:
        Nguồn/probe phải tiếp tục chạy độc lập.
        finally thử unpause; chỉ ghi restored khi lệnh trả thành công.
    """
    if duration_s < 0:
        raise ValueError("duration_s must be >= 0")
    fid  = f"pause-{container}-{uuid.uuid4().hex[:8]}"
    metrics.trace("fault_requested", fid, target = container)
    try:
        subprocess.run(
            ["docker", "pause", container], check = True, timeout = 15
        )
        metrics.trace("fault_applied", fid, target = container)
        time.sleep(duration_s)
    finally:
        metrics.trace("fault_restore_requested", fid, target = container)
        subprocess.run(
            ["docker", "unpause", container], check= True, timeout = 15
        )
        metrics.trace("fault_restored", fid, target = container)
    return fid

def install_alert_ack_lose(db_client):
    """Cài fault một lần: insert alert commit rồi caller nhận exception.

    Args:
        db_client: DataBaseClient trong đúng process consumer benchmark.

    Returns:
        Hàm restore không tham số để gỡ monkey-patch.

    Notes:
        Cài sau tạo consumer, trước start_listening.
        Consumer hiện retry alerts, có thể tạo duplicate; kiểm tra M6 bằng DB.
        Cài trên host không sửa object consumer đang chạy trong Docker.
    """
    real_insert = db_client.insert_alerts_batch
    injected = False

    def insert_then_lose_ack(alerts):
        """ Insert thật; lần có alerts đầu tiên giả mất ACK sau commit """
        nonlocal injected
        result = real_insert(alerts)
        if alerts and not injected:
            injected = True
            raise ConnectionError("Injected: commmit succeeded, ACK lost")
        return result

    def restore():
        """ "Khôi phục insert_alerts_batch gốc sau lượt thử."""
        db_client.insert_alerts_batch = real_insert

    db_client.insert_alerts_batch = insert_then_lose_ack
    return restore
