"""
export_dataset.py — Build dataset đầy đủ từ TOÀN BỘ case đang có file streaming,
đối chiếu với clinical_data.csv, báo rõ case nào thiếu trước khi build.
"""
import glob
import os
import re

import pandas as pd

from src.realtime_medical.config import AppConfig
from feedback.train_model import build_training_dataset

NUMERICS_DIR = "streaming"       # thư mục chứa case_*_numerics.csv
CLINICAL_CSV = "clinical_data.csv"
OUT_PATH = "data/processed/dataset.parquet"

def scan_available_cases(numerics_dir: str) -> set:
    """Quét thư mục lấy đúng caseid nào THỰC SỰ có file, không giả định."""
    pattern = os.path.join(numerics_dir, "case_*_numerics.csv")
    ids = set()
    for path in glob.glob(pattern):
        m = re.search(r"case_(\d+)_numerics\.csv$", os.path.basename(path))
        if m:
            ids.add(int(m.group(1)))
    return ids

def main():
    config = AppConfig.load_config("configs/config.yaml")

    clinical_df = pd.read_csv(CLINICAL_CSV)
    clinical_ids = set(clinical_df["caseid"])
    available_ids = scan_available_cases(NUMERICS_DIR)

    missing_files = clinical_ids - available_ids
    extra_files = available_ids - clinical_ids   # có file nhưng không có trong clinical_data (bỏ qua, không lỗi)

    print(f"Clinical data: {len(clinical_ids)} case")
    print(f"File streaming tim thay: {len(available_ids)} case")
    print(f"Thieu file streaming cho {len(missing_files)} case (se bi bo qua): "
        f"{sorted(missing_files)[:10]}{'...' if len(missing_files) > 10 else ''}")
    if extra_files:
        print(f"Co {len(extra_files)} file streaming khong khop caseid nao trong clinical_data (bo qua)")

    if not available_ids:
        raise RuntimeError(f"Khong tim thay file nao trong {NUMERICS_DIR}, kiem tra lai duong dan")

    dataset = build_training_dataset(CLINICAL_CSV, NUMERICS_DIR, config)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    dataset.to_parquet(OUT_PATH)

    print(f"\nDa export {dataset.shape[0]} sample, {dataset.shape[1]} cot -> {OUT_PATH}")
    print(f"So case thuc te co trong dataset: {dataset['surgery_id'].nunique()}")
    print(f"Ty le nhan Critical soon: {dataset['label'].mean():.3f}")

if __name__ == "__main__":
    main()