"""Kiểm tra trace, hash nguồn và DB của các lượt benchmark đã chạy."""
from __future__ import annotations

import argparse
from collections import Counter
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "test" / "benchmark_results"
sys.path.insert(0, str(ROOT))


def read_json(path):
    """Đọc JSON UTF-8, chấp nhận BOM."""
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def file_matches(path, expected_hash):
    """Kiểm tra hash hiện tại; None nghĩa là thiếu file/hash để đối chiếu."""
    path = Path(path)
    if not expected_hash or not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest() == expected_hash


def check_sources(directory):
    """Đối chiếu file hiện tại với provenance/inventory đã lưu khi chạy."""
    provenance = read_json(directory / "provenance.json") if (directory / "provenance.json").exists() else {}
    result = {"config_matches_current": file_matches(ROOT / "configs/config.yaml", provenance.get("config_sha256"))}
    from src.realtime_medical.config import AppConfig
    config = AppConfig.load_config()
    result["model_matches_current"] = file_matches(ROOT / config.model.path, provenance.get("model_sha256"))
    result["files"] = []
    if (directory / "input_inventory.json").exists():
        for row in read_json(directory / "input_inventory.json"):
            result["files"].append({
                "case": row["case"],
                "source_matches": file_matches(row["source"], row["source_sha256"]),
                "snapshot_matches": file_matches(row["snapshot"], row["snapshot_sha256"]),
            })
    return result


def check_trace(directory, run_id):
    """Đếm trace/lỗi log và kiểm tra collector đã đóng; không đánh giá SLO."""
    from src.realtime_medical.utils.benchmark_analysis import load_run
    data = load_run(directory, run_id)
    log_path = directory / "consumer.log"
    log = log_path.read_text(encoding="utf-8") if log_path.exists() else None
    errors = None if log is None else [line for line in log.splitlines() if "[ERROR]" in line]
    return {
        "expected_vitals": len(data["expected_vitals"]), "expected_alerts": len(data["expected_alerts"]),
        "telemetry_issues": data["telemetry_issues"],
        "stages": dict(Counter(r["name"] for r in data["rows"] if r["kind"] == "trace")),
        "ml_error_count": None if log is None else log.count("ML feature extraction or prediction failed"),
        "error_log_count": None if errors is None else len(errors),
        "error_examples": None if errors is None else errors[:5],
    }


def database_rows(directory, engine):
    """Lấy DB rows đúng manifest của run, không lẫn dữ liệu từ lượt khác."""
    from sqlalchemy import text
    query = text("""
        WITH e AS (
            SELECT DISTINCT surgery_id, recorded_at
            FROM jsonb_to_recordset(CAST(:keys AS jsonb))
            AS x(surgery_id integer, recorded_at timestamp)
        )
        SELECT e.surgery_id AS expected_surgery_id, e.recorded_at AS expected_recorded_at,
               v.surgery_id IS NOT NULL AS persisted, v.*
        FROM e LEFT JOIN vital_signs v ON
            e.surgery_id=v.surgery_id AND e.recorded_at=v.recorded_at
        ORDER BY e.surgery_id, e.recorded_at
    """)
    with engine.connect() as connection:
        return connection.execute(query, {"keys": json.dumps(read_json(directory / "db_keys.json"))}).mappings().all()


def audit_database(directory, engine, rows=None):
    """Đếm vitals, field NULL và alert trùng trong DB; chỉ đọc dữ liệu."""
    from src.realtime_medical.config import AppConfig
    from src.realtime_medical.utils.benchmark_analysis import duplicate_alerts_report
    if rows is None:
        rows = database_rows(directory, engine)
    fields = AppConfig.load_config().canonical_fields.model_dump().values()
    return {
        "expected_keys": len(rows), "persisted_vitals": sum(r["persisted"] for r in rows),
        "null_fields": {field: sum(r["persisted"] and r[field] is None for r in rows) for field in fields},
        "alerts": duplicate_alerts_report(engine, read_json(directory / "db_keys.json")),
    }


def check_rr(metadata, provenance, rows):
    """So rr trong DB với CSV snapshot sau drop-field; DB rr dùng kiểu INT."""
    import pandas as pd
    source = metadata.get("source_data_dir")
    column = next((raw for raw, field in provenance.get("mapping", {}).items() if field == "rr"), None)
    if not source or not column:
        return {"status": "not_checked", "reason": "Missing source_data_dir or RR mapping"}
    frames = {}
    mismatches = []
    compared = 0
    for row in rows:
        if not row["persisted"]:
            continue
        case, tick = row["expected_surgery_id"], row["time_sec"]
        if case not in frames:
            frames[case] = pd.read_csv(Path(source) / f"case_{case}_numerics.csv").set_index("time_sec")
        frame = frames[case]
        expected = frame.loc[tick, column] if column in frame else None
        if metadata.get("source_mode") == "probes":
            expected = 16
        drop_start = metadata.get("drop_start_s", 30)
        if metadata.get("drop_field") == "rr" and drop_start <= tick < drop_start + metadata["gap_seconds"]:
            expected = None
        expected = None if pd.isna(expected) else int(Decimal(str(expected)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        if expected != row["rr"]:
            mismatches.append({"case": case, "time_sec": tick, "expected_rr": expected, "db_rr": row["rr"]})
        compared += 1
    return {"status": "checked" if compared else "no_persisted_rows",
            "compared_rows": compared, "mismatches": len(mismatches), "examples": mismatches[:5]}


def main():
    """Ghi checks.json; chỉ kết nối PostgreSQL khi truyền --check-db."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite-id", required=True)
    parser.add_argument("--supplement-id")
    parser.add_argument("--check-db", action="store_true")
    parser.add_argument("--output", type=Path, help="Mặc định <suite>/checks.json")
    args = parser.parse_args()
    suites = [args.suite_id] + ([args.supplement_id] if args.supplement_id else [])
    output = args.output or RESULTS / args.suite_id / "checks.json"
    if output.suffix.lower() != ".json":
        parser.error("--output must be a .json file")
    engine = None
    if args.check_db:
        from sqlalchemy import create_engine
        from src.realtime_medical.config import AppConfig
        engine = create_engine(AppConfig.load_config().postgres_dsn)
    result = {"sources": {}, "runs": []}
    try:
        for suite in suites:
            directory = RESULTS / suite
            result["sources"][suite] = check_sources(directory)
            provenance = read_json(directory / "provenance.json") if (directory / "provenance.json").exists() else {}
            for metadata in read_json(directory / "suite.json"):
                run_dir = RESULTS / metadata["run_id"]
                run = {"run_id": metadata["run_id"], "trace": check_trace(run_dir, metadata["run_id"])}
                if engine is not None:
                    rows = database_rows(run_dir, engine)
                    run["database"] = audit_database(run_dir, engine, rows)
                    run["rr"] = check_rr(metadata, provenance, rows)
                else:
                    run["database"] = None
                    run["rr"] = {"status": "not_checked"}
                result["runs"].append(run)
    finally:
        if engine is not None:
            engine.dispose()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"{len(result['runs'])} runs -> {output}")


if __name__ == "__main__":
    from sqlalchemy.exc import OperationalError
    try:
        main()
    except OperationalError:
        sys.exit("Không kết nối được PostgreSQL. Kiểm tra DB hoặc chạy lại không có --check-db.")
