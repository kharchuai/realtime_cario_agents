"""
scripts/inspect_rule_triggers.py — Đếm rule nào trong RuleEngine trigger
nhiều nhất trên dữ liệu thật, để kiểm tra xem tỷ lệ nhãn cao / feature RR
chiếm ưu thế có phải do 1 ngưỡng rule đang quá nhạy hay không.

CHỈ ĐỌC: data/clinical_data.csv, data/streaming/*.csv
Chạy: python scripts/inspect_rule_triggers.py
"""
import os
import sys
from collections import Counter

import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.realtime_medical.config import AppConfig
from src.realtime_medical.streaming.windowing import CANONICAL_FIELDS
from src.realtime_medical.triage.rules import RuleEngine

# Tái sử dụng đúng hàm đã có trong train_model.py, không viết lại logic
from scripts_not.train_model import load_case_numerics, scan_available_cases, CLINICAL_CSV, STREAMING_DIR


def main():
    config = AppConfig.load_config("configs/config.yaml")
    rule_engine = RuleEngine(config.triage_rules)

    clinical_df = pd.read_csv(CLINICAL_CSV)
    clinical_ids = set(clinical_df["caseid"])
    available_ids = scan_available_cases(STREAMING_DIR)
    case_ids = sorted(clinical_ids & available_ids)

    reason_counter = Counter()
    total_points = 0
    total_critical_points = 0

    for caseid in case_ids:
        raw_df = load_case_numerics(STREAMING_DIR, caseid, config)
        if raw_df is None:
            continue
        for _, row in raw_df.iterrows():
            vitals = {f: row.get(f) for f in CANONICAL_FIELDS}
            risk, reasons = rule_engine.evaluate(vitals)
            total_points += 1
            if risk == "CRITICAL":
                total_critical_points += 1
            for r in reasons:
                # Gom theo tên rule (bỏ phần số liệu cụ thể trong ngoặc)
                key = r.split("(")[0].strip()
                reason_counter[key] += 1

    print(f"Tong so diem du lieu da quet: {total_points}")
    print(f"Ty le diem bi danh gia CRITICAL: {total_critical_points / total_points:.3f}\n")
    print("Rule nao trigger nhieu nhat (co the trung nhau 1 diem nhieu rule):")
    for reason, count in reason_counter.most_common():
        pct = count / total_points * 100
        print(f"  {count:7d}  ({pct:5.2f}%)  {reason}")


if __name__ == "__main__":
    main()