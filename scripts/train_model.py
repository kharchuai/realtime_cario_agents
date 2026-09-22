"""
scripts/train_model.py — Build dataset huấn luyện từ data/ có sẵn.

CHỈ ĐỌC:
  - data/clinical_data.csv
  - data/streaming/case_*_numerics.csv
  - src/realtime_medical/ (windowing.py, rules.py, config.py) — tái sử dụng
    logic tính feature, KHÔNG sửa các file này, đảm bảo train/serve khớp nhau

CHỈ GHI:
  - data/processed/dataset.parquet

Không đụng configs/, db/, models/ hay bất kỳ file production nào khác.
Chạy: python scripts/train_model.py
"""

import glob
import os
import re
import sys

import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from src.realtime_medical.config import AppConfig
from src.realtime_medical.streaming.windowing import build_offline_windows, CANONICAL_FIELDS
from src.realtime_medical.triage.rules import RuleEngine

CLINICAL_CSV = "data/clinical_data.csv"
STREAMING_DIR = "data/streaming"
OUT_PATH = "data/processed/dataset.parquet"
KNOWN_DEPARTMENTS = ["General surgery", "Thoracic surgery", "Urology",  "Gynecology"]

def scan_available_cases(streaming_dir: str) -> set:
    """Quét data/streaming/, lấy đúng caseid nào THỰC SỰ có file."""
    pattern = os.path.join(streaming_dir, "case_*_numerics.csv")
    ids = set()
    for  path in  glob.glob(pattern):
        m = re.search(r"case_(\d+)_numerics\.csv$",  os.path.basename(path))
        if m:
            ids.add(int(m.group(1)))
    return ids

def load_case_numerics(streaming_dir: str, caseid:  int, config: AppConfig):
    path = os.path.join(streaming_dir,  f"case_{caseid}_numerics.csv")
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    rename_map = config.source_mapping.fields
    df = df.rename(columns =  rename_map)
    missing = [c for c in CANONICAL_FIELDS if c not in df.columns]
    if missing:
        print(f"[bo qua] case {caseid} thieu cot {missing}")
        return None
    return df

def label_window(raw_df, window_end_elapsed,horizon_seconds, rule_engine):
    """Nhãn = 1 nếu trong (window_end, window_end + horizon] có điểm CRITICAL."""
    future = raw_df[
        (raw_df["time_sec"] >  window_end_elapsed) & 
        (raw_df["time_sec"] <= window_end_elapsed + horizon_seconds)
    ]
    if future.empty:
        return None
    for _, row in future.iterrows():
        vitals = {f: row.get(f) for f in CANONICAL_FIELDS}
        risk_level, _ =  rule_engine.evaluate(vitals)
        if risk_level == "CRITICAL":
            return 1
    return 0

def build_static_lookup(clinical_df:   pd.DataFrame) -> dict:
    lookup = {}
    for _,  row in clinical_df.iterrows():
        feat = {
            "age": row.get("age"), "sex_M": 1 if row.get("sex") == "M" else 0,
            "bmi": row.get("bmi"), "asa": row.get("asa"), "emop": row.get("emop"),
        }
        for dept in KNOWN_DEPARTMENTS:
            key = f"department_{dept.lower().replace(' ', '_')}"
            feat[key] = 1 if row.get("department") == dept else 0
        lookup[row["caseid"]] = feat
    return lookup

def main():
    config = AppConfig.load_config("configs/config.yaml")
    
    clinical_df  = pd.read_csv(CLINICAL_CSV)
    clinical_ids = set(clinical_df["caseid"])
    available_ids = scan_available_cases(STREAMING_DIR)
    
    missing = clinical_ids - available_ids
    print(f"Clinical data: {len(clinical_ids)} case")
    print(f"File streaming trong data/streaming/: {len(available_ids)} case")
    print(f"Thieu file cho {len(missing)} case (bo qua): {sorted(missing)[:10]}{'...' if len(missing) > 10 else ''}")
    if not available_ids:
        raise RuntimeError(f"Khong tim thay file nao trong {STREAMING_DIR}")
    
    static_lookup = build_static_lookup(clinical_df)
    rule_engine =  RuleEngine(config.triage_rules)
    horizon = config.model.prediction_horizon_seconds
    
    all_rows, skipped = [], 0
    for caseid in sorted(clinical_ids & available_ids) :
        raw_df = load_case_numerics(STREAMING_DIR, caseid, config)
        if raw_df is None:
            skipped += 1
            continue
        
        window_df = build_offline_windows(raw_df, config.streaming, surgery_id= caseid)
        if window_df.empty:
            continue
        
        labels  = [label_window(raw_df, w["window_end"],  horizon, rule_engine) for _, w in window_df.iterrows()]
        window_df  = window_df.assign(**static_lookup.get(caseid, {}))
        window_df["label"] = labels
        all_rows.append(window_df)
        
    if not all_rows:
        raise RuntimeError("Khong build duoc sample nao, kiem tra lai du lieu")
    dataset = pd.concat(all_rows, ignore_index= True)
    dataset = dataset.dropna(subset =  ["label"])
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    dataset.to_parquet(OUT_PATH)

    print(f"\nDa build {len(dataset)} sample tu {dataset['surgery_id'].nunique()} case "
        f"(bo qua {skipped} case thieu cot)")
    print(f"Ty le nhan Critical soon: {dataset['label'].mean():.3f}")
    print(f"Da ghi -> {OUT_PATH}")

if __name__ == "__main__":
    main()