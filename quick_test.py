"""
Quick smoke test — kiểm tra 3 điểm dễ vỡ nhất trong pipeline hiện có,
KHÔNG cần Kafka/Postgres đang chạy thật:
  1. Config load được, source_mapping khớp với CSV thật
  2. RuleEngine chạy đúng logic (bình thường vs nguy hiểm)
  3. Producer đóng gói payload đúng, Consumer parse lại đúng (round-trip)
"""
import sys
import json
import pandas as pd

from src.realtime_medical.config import AppConfig
from src.realtime_medical.generator.sensor_simulator import _build_vitals_row
from src.realtime_medical.triage.rules import RuleEngine

FAILS = []

def check(name, condition):
    status = "OK " if condition else "FAIL"
    print(f"[{status}] {name}")
    if not condition:
        FAILS.append(name)


# ---------- 1. Config + mapping với CSV thật ----------
config = AppConfig.load_config("configs/config.yaml")
check("Config load thành công", config.project_name == "realtime-medical-monitoring")
check("Tên bảng khớp schema (vital_signs)", config.database.table == "vital_signs")

df = pd.read_csv("E:\\dowload\\Serminal\\data_raw\\out\\case_48_numerics.csv", nrows=10)  # đổi path nếu file để chỗ khác
row = df.iloc[0]
vitals = _build_vitals_row(row, config)
check("Map được ít nhất 1 field từ CSV", len(vitals) > 0)
check("Field 'hr' có mặt sau khi map", "hr" in vitals)
print("   -> vitals mẫu:", vitals)


# ---------- 2. RuleEngine ----------
engine = RuleEngine(config.triage_rules)

normal_vitals = {"hr": 75, "spo2": 98, "sbp": 120, "dbp": 80, "mbp": 90, "rr": 16, "bt": 37.0}
risk, reasons = engine.evaluate(normal_vitals)
check("Vitals bình thường -> risk LOW", risk == "LOW" and reasons == [])

critical_vitals = {"hr": 160, "spo2": 85, "sbp": 200, "dbp": 80, "mbp": 90, "rr": 16, "bt": 37.0}
risk, reasons = engine.evaluate(critical_vitals)
check("Vitals nguy hiểm -> risk CRITICAL", risk == "CRITICAL")
check("Có lý do cảnh báo cụ thể", len(reasons) > 0)
print("   -> lý do:", reasons)


# ---------- 3. Producer/Consumer payload round-trip ----------
# Không tạo VitalsProducer thật (tránh phụ thuộc broker) — copy đúng logic
# đóng gói payload từ producer.py (sau khi sửa) để kiểm tra round-trip.
def fake_send_vital_sign(surgery_id, patient_id, vital_data):
    payload = {**vital_data, "surgery_id": surgery_id, "patient_id": patient_id, "produced_at": 123.0}
    return json.dumps(payload)

raw = fake_send_vital_sign(surgery_id=1, patient_id="1", vital_data=vitals | {"recorded_at": "2026-09-13T10:00:00"})
received = json.loads(raw)

# đúng những gì consumer.py thật sự làm khi nhận message
surgery_id = received.pop("surgery_id", None)
patient_id = received.pop("patient_id", "UNKNOWN")

check("Consumer lấy được surgery_id", surgery_id == 1)
check("Consumer lấy được patient_id", patient_id == "1")
check("recorded_at còn nguyên trong vitals_data", received.get("recorded_at") is not None)


# ---------- Kết luận ----------
print("\n" + "=" * 50)
if FAILS:
    print(f"❌ {len(FAILS)} bước FAIL:", FAILS)
    sys.exit(1)
else:
    print("✅ Tất cả bước logic-level đều OK.")