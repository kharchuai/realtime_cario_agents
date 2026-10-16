"""Gộp số liệu các lượt benchmark thành JSON và bảng Markdown."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "test" / "benchmark_results"
sys.path.insert(0, str(ROOT))


def read_json(path):
    """Đọc JSON, kể cả file có BOM từ PowerShell."""
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def signal_summary(report):
    """Gộp age/gap theo field; giữ None nếu chưa có mẫu hợp lệ."""
    streams = report.get("M2", {}).get("streams", [])
    fields = {}
    for field in sorted({s["field"] for s in streams}):
        rows = [s for s in streams if s["field"] == field]
        fields[field] = {
            "max_age_s": max((s["max_active_signal_age_s"] for s in rows
                              if s.get("max_active_signal_age_s") is not None), default=None),
            "max_gap_s": max((s["max_active_receive_gap_s"] for s in rows
                              if s.get("max_active_receive_gap_s") is not None), default=None),
            "streams_over_gap_limit": sum(s.get("active_receive_gap_over_limit_samples", 0) > 0 for s in rows),
            "clock_anomaly_samples": sum(s.get("clock_anomaly_samples", 0) for s in rows),
            "never_seen_cases": sorted(s["surgery_id"] for s in rows if s.get("active_samples", 0) > 0
                                       and s.get("active_never_seen_samples") == s["active_samples"]),
        }
    return fields


def summarize_run(metadata, report):
    """Lấy metrics đã tính; chỉ gộp signal theo field và throughput theo cửa sổ."""
    metrics = {key: value for key, value in report.items() if key.startswith("M")}
    metrics["M2"] = {"Name": report.get("M2", {}).get("Name"),
                     "signal_gap_limit_s": report.get("M2", {}).get("signal_gap_limit_s"),
                     "fields": signal_summary(report)}
    if "M10" in metrics:
        throughput = report["M10"]
        metrics["M10"] = {"Name": throughput["Name"], "validation": throughput.get("validation", {})}
        for stream in ("vitals", "alerts"):
            windows = throughput.get(stream, [])
            values = [w["throughput_per_s"] for w in windows]
            duration = sum(w["end_ns"] - w["start_ns"] for w in windows) / 1e9
            completed = sum(w["unique_completed"] for w in windows)
            metrics["M10"][stream] = {
                "windows": len(windows), "duration_s": duration,
                "mean_per_s": completed / duration if duration else None,
                "min_per_s": min(values, default=None), "max_per_s": max(values, default=None),
            }
    return {"metadata": metadata, "observed_end_ns": report["observed_end_ns"],
            "telemetry_issues": report.get("telemetry_issues", []), "metrics": metrics}


def fmt(value, *, percent=False):
    """Định dạng ô số; dữ liệu chưa có hiển thị N/A."""
    if value is None:
        return "N/A"
    if percent:
        return f"{value * 100:.2f}%"
    return f"{value:.3f}" if isinstance(value, float) else str(value)


def markdown(runs):
    """Xuất các bảng số liệu, không thêm đánh giá đạt/chưa đạt."""
    lines = ["# Số liệu benchmark", "",
             "V/A = vitals/alerts. N/A = chưa có kết quả. Latency: ms; RTO, drain, signal: giây.", "",
             "| Run | Nguồn | Bệnh nhân | Readings | Pause (s) | Drop field / thời gian (s) | Trace issues |",
             "|---|---|---:|---:|---:|---|---:|"]
    for run in runs:
        m = run["metadata"]
        lines.append(f"| {m['run_id']} | {m.get('source_mode', 'N/A')} | {m['patients']} | {m['expected_readings']} | {fmt(m.get('pause_seconds'))} | {m.get('drop_field') or 'N/A'} / {fmt(m.get('gap_seconds'))} | {len(run['telemetry_issues'])} |")
    for run in runs:
        m = run["metrics"]
        lines += ["", f"## {run['metadata']['run_id']}", "",
                  "| Metric | Luồng | Số liệu |", "|---|---|---|"]
        for metric in ("M1", "M3"):
            for stream in ("vitals", "alerts"):
                d = m[metric][stream]
                if metric == "M1":
                    value = (f"missing_at_deadline={d['missing_at_deadline']}; late={d['late']}; "
                             f"not_observed={d['not_observed']}; chưa tới hạn={d['deadline_not_reached']}")
                else:
                    value = (f"on_time={d['on_time']}/{d['eligible']}; "
                             f"rate={fmt(d['on_time_rate'], percent=True)}; "
                             f"miss_rate={fmt(d['deadline_miss_rate'], percent=True)}")
                lines.append(f"| {metric} | {stream} | {value} |")
        fields = m["M1"]["fields"]
        lines.append(f"| M1 | raw fields | {fields['valid_fields_on_time']}/{fields['required_fields']}; completeness={fmt(fields['completeness_rate'], percent=True)} |")
        for field, s in m["M2"]["fields"].items():
            lines.append(f"| M2 | {field} | age_max={fmt(s['max_age_s'])}; gap_max={fmt(s['max_gap_s'])}; streams vượt limit={s['streams_over_gap_limit']}; never_seen={len(s['never_seen_cases'])} cases |")
        for label, key in (("source→detection", "source_to_detection"), ("source→alert DB", "source_to_alert_persisted")):
            d = m["M4"][key]
            lines.append(f"| M4 | {label} | n={d['count']}; p50={fmt(d['p50_ms'])}; p95={fmt(d['p95_ms'])}; p99={fmt(d['p99_ms'])} |")
        d = m["M4"]["detection"]
        lines.append(f"| M4 | detection deadline | on_time={d['on_time']}; late={d['late']}; not_observed={d['not_observed']}; rate={fmt(d['on_time_rate'], percent=True)} |")
        d = m["M5"]["latency"]
        lines.append(f"| M5 | alert generated→ACK | n={d['count']}; p50={fmt(d['p50_ms'])}; p95={fmt(d['p95_ms'])}; p99={fmt(d['p99_ms'])}; unpersisted={m['M5']['unpersisted_alerts']} |")
        d = m.get("M6", {})
        lines.append(f"| M6 | alerts | rows={fmt(d.get('actual_alert_rows'))}; distinct={fmt(d.get('distinct_logical_alerts'))}; duplicate_excess={fmt(d.get('duplicate_excess'))} |")
        faults = m.get("M7_M8_M9_by_fault", [])
        if not faults:
            lines.append("| M7–M9 | fault | N/A |")
        for fault in faults:
            lines.append(f"| Fault | {fault['fault_id']} | applied_ns={fault['applied_ns']}; restored_ns={fmt(fault.get('restored_ns'))} |")
            for stream, d in fault.get("M7", {}).items():
                if isinstance(d, dict):
                    lines.append(f"| M7 | {stream} | on_time={d['on_time']}; late={d['late']}; not_observed={d['not_observed']}; rate={fmt(d['on_time_rate'], percent=True)} |")
            for stream, d in fault.get("M8", {}).items():
                if isinstance(d, dict):
                    lines.append(f"| M8 | {stream} | {d['status']}; baseline_healthy={d.get('baseline_healthy')}; candidate={fmt(d.get('recovery_candidate_s'))}; confirmed={fmt(d.get('recovery_confirmed_s'))}; sau restore={fmt(d.get('confirmed_after_restore_s'))}; observed_for={fmt(d.get('observed_for_s'))} |")
            for stream, d in fault.get("M9", {}).items():
                if isinstance(d, dict):
                    lines.append(f"| M9 | {stream} | backlog={d['backlog']}; remaining={d['remaining']}; drain={fmt(d['drain_time_s'])}; observed_for={fmt(d.get('observed_for_s'))} |")
        for stream in ("vitals", "alerts"):
            d = m.get("M10", {}).get(stream, {})
            v = m.get("M10", {}).get("validation", {}).get(stream, {})
            lines.append(f"| M10 | {stream} | output/s TB={fmt(d.get('mean_per_s'))}; min={fmt(d.get('min_per_s'))}; max={fmt(d.get('max_per_s'))}; input/s={fmt(v.get('scheduled_input_per_s'))}; backlog đầu/cuối={fmt(v.get('backlog_at_start'))}/{fmt(v.get('backlog_at_end'))} |")
        if run.get("db_check") is not None:
            d = run["db_check"]
            lines += ["", f"DB: vitals={d['persisted_vitals']}/{d['expected_keys']}; "
                      f"alerts={d['alerts']['actual_alert_rows']}; "
                      f"duplicate_excess={d['alerts']['duplicate_excess']}; "
                      f"NULL fields={d['null_fields']}."]
    lines += ["", "M1/M3 cùng cohort deadline. M2 chỉ xét lúc nguồn hoạt động. M4 detection hiện là mốc sau rules, chưa gồm agent.",
              "Percentile chỉ gồm mẫu có kết quả. Alerts của M10 gồm cả ML-only, còn input alerts kỳ vọng theo oracle.", ""]
    return "\n".join(lines)


def main():
    """Đọc suite bất kỳ và ghi summary.md cùng summary.json."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite-id", required=True)
    parser.add_argument("--supplement-id")
    parser.add_argument("--check-db", action="store_true")
    parser.add_argument("--output", type=Path, help="File .md; mặc định <suite>/summary.md")
    args = parser.parse_args()
    output = args.output or RESULTS / args.suite_id / "summary.md"
    if output.suffix.lower() != ".md":
        parser.error("--output must be a .md file")
    suites = [args.suite_id] + ([args.supplement_id] if args.supplement_id else [])
    runs = []
    for suite in suites:
        for metadata in read_json(RESULTS / suite / "suite.json"):
            directory = RESULTS / metadata["run_id"]
            runs.append(summarize_run(metadata, read_json(directory / "report.json")))
    if args.check_db:
        from sqlalchemy import create_engine
        from src.realtime_medical.config import AppConfig
        from scripts.review_benchmark import audit_database
        engine = create_engine(AppConfig.load_config().postgres_dsn)
        try:
            for run in runs:
                run["db_check"] = audit_database(RESULTS / run["metadata"]["run_id"], engine)
        finally:
            engine.dispose()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.with_suffix(".json").write_text(json.dumps({"suites": suites, "runs": runs}, ensure_ascii=False, indent=2), encoding="utf-8")
    output.write_text(markdown(runs), encoding="utf-8")
    print(f"{len(runs)} runs -> {output}, {output.with_suffix('.json')}")


if __name__ == "__main__":
    from sqlalchemy.exc import OperationalError
    try:
        main()
    except OperationalError:
        sys.exit("Không kết nối được PostgreSQL. Kiểm tra DB hoặc chạy lại không có --check-db.")
