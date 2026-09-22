"""scripts/check_bp_threshold.py — So ngưỡng sbp_low/dbp_low hiện tại với
phân phối thật trên toàn bộ case, trước khi quyết định có sửa hay không."""
import os, sys
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from src.realtime_medical.config import AppConfig
from scripts.train_model import load_case_numerics, scan_available_cases, CLINICAL_CSV, STREAMING_DIR

config = AppConfig.load_config("configs/config.yaml")
clinical_ids = set(pd.read_csv(CLINICAL_CSV)["caseid"])
case_ids = sorted(clinical_ids & scan_available_cases(STREAMING_DIR))

all_sbp, all_dbp = [], []
for caseid in case_ids:
    raw_df = load_case_numerics(STREAMING_DIR, caseid, config)
    if raw_df is None:
        continue
    all_sbp.append(raw_df["sbp"].dropna())
    all_dbp.append(raw_df["dbp"].dropna())

sbp = pd.concat(all_sbp)
dbp = pd.concat(all_dbp)

rule = config.triage_rules.blood_pressure
print(f"Nguong hien tai: sbp_low={rule.sbp_low}, dbp_low={rule.dbp_low}")
print(f"\nSBP thuc te ({len(sbp)} diem, {len(case_ids)} case):")
print(sbp.describe(percentiles=[.05, .10, .15, .25, .5]))
print(f"Ty le diem SBP <= nguong hien tai: {(sbp <= rule.sbp_low).mean():.3f}")

print(f"\nDBP thuc te ({len(dbp)} diem):")
print(dbp.describe(percentiles=[.05, .10, .15, .25, .5]))
print(f"Ty le diem DBP <= nguong hien tai: {(dbp <= rule.dbp_low).mean():.3f}")