import argparse
import json
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from src.realtime_medical.config import settings 

PARTIENT_COLUMNS = [
    "patient_id",
    "age",
    "sex",
    "height",
    "weight",
    "bmi",
]

SURGERY_COLUMNS = [
    "surgery_id",
    "patient_id",
    "asa",
    "emop",
    "department",
    "optype",
    "anestart",
    "aneend",
    "opstart",
    "opend",
    "death_inhosp",
    "clinical_metadata",
]

METADATA_COLUMNS = [
    "dx",
    "opname",
    "approach",
    "position",
    "ane_type",
    "preop_htn",
    "preop_dm",
    "icu_days",
]

def nullable_int(value):
    if pd.isna(value) or value == "":
        return None
    return int(float(value))

def nullable_float(value):
    if pd.isna(value) or value == "":
        return None
    return float(value)

def nullable_text(value):
    if pd.isna(value):
        return None
    value =  str(value).strip()
    return value or None

def build_metadata(row):
    metadata = {}
    
    for column in METADATA_COLUMNS:
        value = row.get(column)
        
        if pd.isna(value) or value == "":
            continue
        
        # Chuyển kiểu dữ liệu của pandas về kiểu JSON thông thường
        if hasattr(value, "item"):
            value = value.item()
            
        metadata[column] =  value
        
    return json.dumps(metadata, ensure_ascii=False)

def load_seed_data(csv_path, patient_limit):
    clinical_df = pd.read_csv(csv_path)
    
    required_columns  = {
        "caseid",
        "subjectid",
        "age",
        "sex",
        "height",
        "weight",
        "bmi",
        "asa",
        "emop",
        "department",
        "optype",
        "anestart",
        "aneend",
        "opstart",
        "opend",
        "death_inhosp",
    }
    
    missing_columns = required_columns - set(clinical_df.columns)
    
    if missing_columns:
        raise ValueError(
            f"CSV is missing required columns: {sorted(missing_columns)}"
        )
    
    if patient_limit <= 0:
        raise ValueError("--patients must be greater than 0")
    
    #  Giữ thứ tự xuất hiện trong CSV và chọn đúng N bệnh nhân duy nhất.
    selected_patient_ids = (
        clinical_df["subjectid"]
        .dropna()
        .drop_duplicates()
        .head(patient_limit)
        .tolist()
    )
    
    if len(selected_patient_ids) <  patient_limit:
        print(
            f"Warning: Requested {patient_limit} patients, but the data "
            f"only contains {len(selected_patient_ids)} patients."
        )
    
    selected_df =  clinical_df[
        clinical_df["subjectid"].isin(selected_patient_ids)
    ].copy()
    
    # Nếu một bệnh nhân xuất hiện nhiều lần, dùng thông tin nhân khẩu học
    # ở dòng đầu tiên để tạo một record patient.
    patient_df = selected_df.drop_duplicates(
        subset=["subjectid"],
        keep="first"
    )

    patients = []
    for _, row  in patient_df.iterrows():
        patients.append(
            {
                "patient_id": nullable_int(row["subjectid"]),
                "age": nullable_int(row["age"]),
                "sex": nullable_text(row["sex"]),
                "height": nullable_float(row["height"]),
                "weight": nullable_float(row["weight"]),
                "bmi": nullable_float(row["bmi"])
            }
        )        
    surgeries  = []
    for _, row in  selected_df.iterrows():
        surgeries.append(
            {
                "surgery_id": nullable_int(row["caseid"]),
                "patient_id": nullable_int(row["subjectid"]),
                "asa": nullable_int(row["asa"]),
                "emop": nullable_int(row["emop"]),
                "department": nullable_text(row["department"]),
                "optype": nullable_text(row["optype"]),
                "anestart": nullable_int(row["anestart"]),
                "aneend": nullable_int(row["aneend"]),
                "opstart": nullable_int(row["opstart"]),
                "opend": nullable_int(row["opend"]),
                "death_inhosp": nullable_int(row["death_inhosp"]),
                "clinical_metadata": build_metadata(row),                
            }
        )
        
    return patients, surgeries

def insert_seed_data(patients, surgeries):
    patient_query = text(
    """
    INSERT INTO patient(
        patient_id,
        age, 
        sex,
        height,
        weight,
        bmi
    )
    VALUES (
        :patient_id,
        :age,
        :sex,
        :height,
        :weight,
        :bmi
    )
    ON CONFLICT (patient_id) DO UPDATE SET
        age = EXCLUDED.age,
        sex = EXCLUDED.sex,
        height = EXCLUDED.height,
        weight = EXCLUDED.weight,
        bmi = EXCLUDED.bmi
    """
    )
    
    surgery_query = text (
        """
        INSERT INTO surgeries(
            surgery_id, 
            patient_id,
            asa,
            emop,
            department,
            optype,
            anestart,
            aneend,
            opstart,
            opend,
            death_inhosp,
            clinical_metadata
        )
        VALUES (
            :surgery_id, 
            :patient_id,
            :asa,
            :emop,
            :department,
            :optype,
            :anestart,
            :aneend,
            :opstart,
            :opend,
            :death_inhosp,
            CAST(:clinical_metadata AS JSONB)
        )
        ON CONFLICT (surgery_id) DO UPDATE SET
            patient_id = EXCLUDED.patient_id,
            asa = EXCLUDED.asa,
            emop = EXCLUDED.emop,
            department = EXCLUDED.department,
            optype = EXCLUDED.optype,
            anestart = EXCLUDED.anestart,
            aneend = EXCLUDED.aneend,
            opstart = EXCLUDED.opstart,
            opend = EXCLUDED.opend,
            death_inhosp = EXCLUDED.death_inhosp,
            clinical_metadata = EXCLUDED.clinical_metadata            
        """
    )
    
    engine = create_engine(settings.postgres_dsn)
    
    try:
        # Cùng một transaction: nếu insert surgeries lỗi thì patients
        # của lần chạy này cũng được rollback.
        with engine.begin() as connection:
            connection.execute(patient_query, patients)
            connection.execute(surgery_query,  surgeries)
    finally:
        engine.dispose()
        
def main():
    parser = argparse.ArgumentParser(description="Import patients and surgeries from clinical_data.csv")
    
    parser.add_argument(
        "--patients",
        type=int,
        required = True,
        help="Number of unique patients to import",
    )
    
    parser.add_argument(
        "--csv",
        type = Path,
        default = Path("data/clinical_data.csv"),
        help = "Path to clinical_data.csv",
    )
    
    args =  parser.parse_args()
    
    if not args.csv.exists():
        raise FileNotFoundError(f"File not found: {args.csv}")
    
    patients, surgeries = load_seed_data(
        csv_path= args.csv,
        patient_limit= args.patients,
    )
    insert_seed_data(patients, surgeries)

    print(f"Processed {len(patients)} patients.")
    print(f"Processed {len(surgeries)} surgeries.")

if  __name__ == "__main__":
    main()
