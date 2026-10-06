
# =========================================================================
# Module: sensor_simulator.py
#
# Tác dụng: "Máy giả lập cảm biến" - đọc dữ liệu sinh hiệu có sẵn từ 1 file
# CSV mẫu, rồi phát lại (replay) từng dòng vào Kafka với độ trễ giữa các
# dòng để mô phỏng luồng dữ liệu thời gian thực, dùng để test toàn bộ hệ
# thống (producer -> Kafka -> consumer -> RuleEngine/MLTriageModel) mà
# không cần thiết bị đo thật.
#
# THIẾT KẾ ĐỒNG BỘ VỚI config.yaml:
#   File CSV mẫu được xem như dữ liệu "vendor" (giống thiết bị đo thật), nên
#   việc map tên cột CSV -> key chuẩn hoá dùng THẲNG config.source_mapping.fields
#   (section 2 trong config.yaml) - không tự tạo thêm 1 dict ánh xạ riêng
#   trong code, tránh có 2 nguồn "chân lý" khác nhau cho cùng 1 việc.
#
#   Nhờ vậy nếu sau này đổi vendor, đổi tên cột dữ liệu, hoặc đổi key chuẩn
#   hoá ("hr" -> "heart_rate_bpm" chẳng hạn), CHỈ CẦN SỬA config.yaml -
#   không cần sửa file này.
#
#   Yêu cầu: tên cột trong file CSV mẫu phải khớp với key bên trái của
#   source_mapping.fields (ví dụ "Solar8000/HR", "Solar8000/PLETH_SPO2"...).
#   Nếu dùng file CSV có tên cột khác, cần cập nhật lại source_mapping.fields
#   trong config.yaml cho khớp - đây vẫn là sửa yaml, không phải sửa code.
# =========================================================================

import pandas as pd
import time 
import logging
import threading

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

from src.realtime_medical.config import AppConfig, BASE_DIR
from src.realtime_medical.streaming import VitalsProducer


logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)


def _build_vitals_row(row:pd.Series, config: AppConfig) -> dict:
    """
    Build 1 dict vitals đúng theo key chuẩn hoá của hệ thống, dựa trên
    dữ liệu 1 dòng CSV.

    KHÔNG hardcode tên cột/key ở đây - toàn bộ ánh xạ "tên cột dữ liệu gốc
    -> key chuẩn hoá" lấy trực tiếp từ config.source_mapping.fields (section
    2 trong config.yaml), vì đây chính là section được sinh ra cho việc này
    (map dữ liệu vendor/CSV -> canonical). Đổi vendor hoặc đổi tên cột trong
    tương lai chỉ cần sửa config.yaml, file này không cần sửa gì.

    Cột nào không tồn tại trong CSV hoặc bị rỗng (NaN) sẽ được bỏ qua.
    """
    vitals = {}
    for raw_col, canonical_key in config.source_mapping.fields.items():
        if raw_col not in row or pd.isna(row[raw_col]):
            continue
        vitals[canonical_key] = float(row[raw_col])
    return vitals

def replay_data(config: AppConfig, data_path: str,surgery_id:int, patient_id:int, delay_seconds:  float = 1.0) -> None:
    """
    Read data from file csv and replay into kafka
    simulating streaming data
    """    
    
    producer = VitalsProducer(config)
    logger.info(f"[surgery = {surgery_id}] Loading sample data from {data_path}")
    
    try: 
        df = pd.read_csv(data_path)
        logger.info(f"[surgery = {surgery_id}] Loaded successfully{len(df)} rows of data")
        
        for index, row in df.iterrows():
            try:  
                vitals = _build_vitals_row(row,  config)
                if not vitals:
                    logger.warning(f"[surgery_id = {surgery_id}] Row {index}:  NO valid field, skip")
                    continue
                
                vitals["recorded_at"] = datetime.now(timezone.utc).isoformat()
                
                logger.info(f"[surgery_id ={surgery_id}] Push data {vitals}")
                producer.send_vital_sign(surgery_id, patient_id, vitals)
                
                time.sleep(delay_seconds)
                
            except (ValueError, TypeError, KeyError) as e:
                logger.error(f"[Surgery = {surgery_id}] Skip row {index} because of error:  {e}")
                continue
            
    except FileNotFoundError:
        logger.error(f"[Surgery = {surgery_id}] File not found at: {data_path}")
        
    except KeyboardInterrupt:
        logger.info(f"[Surgery = {surgery_id}] Received command from user to stop data transmission")
        
    finally:
        producer.flush()
        logger.info(f"[Surgery = {surgery_id}] Kafka connection closed safety")
        
        
@dataclass
class ReplaySource:
    """1 nguồn phát lại độc lập = 1 ca mổ (1 file CSV, 1 surgery_id, 1 patient_id)."""
 
    surgery_id: int
    patient_id: str
    data_path: str
    delay_seconds: float = 1.0
 
 
def replay_multiple(config: AppConfig, sources: List[ReplaySource]) -> None:
    """
    Chạy song song nhiều ReplaySource cùng lúc (mỗi ca mổ 1 thread riêng,
    mỗi thread có VitalsProducer riêng) - dùng để giả lập nhiều bệnh nhân
    gửi dữ liệu đồng thời, thay vì tuần tự từng người một.
    """
    threads = []
    for src in sources:
        t = threading.Thread(
            target=replay_data,
            kwargs={
                "config": config,
                "data_path": src.data_path,
                "surgery_id": src.surgery_id,
                "patient_id": src.patient_id,
                "delay_seconds": src.delay_seconds,
            },
            name=f"replay-surgery-{src.surgery_id}",
        )
        threads.append(t)
        t.start()
 
    for t in threads:
        t.join()
 
 
if __name__ == "__main__":
    config = AppConfig.load_config()
 
    # Ví dụ chạy đơn lẻ 1 ca mổ - surgery_id/patient_id PHẢI khớp bản ghi
    # đã có sẵn trong bảng surgeries/patient (ràng buộc FK), xem seed.sql
    # nếu bạn cần dữ liệu mẫu để test.
    sample_file_path = BASE_DIR / "data" / "sample_patients_vitals.csv"
    replay_data(
        config,
        data_path=str(sample_file_path),
        surgery_id=1,
        patient_id="1",
        delay_seconds=0.5,
    )
 
    # Ví dụ chạy song song nhiều ca mổ (bỏ comment để dùng):
    # sources = [
    #     ReplaySource(surgery_id=1, patient_id="1", data_path=str(BASE_DIR / "data" / "case_1.csv")),
    #     ReplaySource(surgery_id=2, patient_id="2", data_path=str(BASE_DIR / "data" / "case_2.csv")),
    # ]
    # replay_multiple(config, sources)