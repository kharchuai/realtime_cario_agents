"""CLI phân tích một benchmark run đã dừng ghi trace."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
PROJECT_ROOT  = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.realtime_medical.utils.benchmark_analysis import (
    alert_persistence_report, backlog_drain, deadline_report, duplicate_alerts_report,
    field_completeness, load_run, observed_rto,
    probe_samples, source_latencies_ms, throughput_windows,
)

from src.realtime_medical.utils.metrics import percentile

def distribution(values):
     """Trả count/p50/p95/p99 milliseconds; cohort rỗng trả percentile None."""
     return {
        "count":  len(values),
        "p50_ms": percentile(values, 50),
        "p95_ms": percentile(values,  95),
        "p99_ms": percentile(values, 99)
     }


def throughput_validation(expected, first, sink_stage, start_ns, end_ns, observed_end_ns):
    """Đối chiếu output rate với input theo lịch, backlog và SLA trong cùng khoảng."""
    cohort = {rid: item for rid, item in expected.items() if start_ns <= item["t0_ns"] < end_ns}
    def pending_at(at_ns):
        return sum(item["t0_ns"] < at_ns and
                   (first.get((sink_stage, rid)) is None or first[(sink_stage, rid)] >= at_ns)
                   for rid, item in expected.items())
    return {
        "scheduled_inputs": len(cohort),
        "scheduled_input_per_s": len(cohort) / ((end_ns - start_ns) / 1e9),
        "backlog_at_start": pending_at(start_ns),
        "backlog_at_end": pending_at(end_ns),
        "deadline_results_for_inputs": deadline_report(cohort, sink_stage, observed_end_ns, first),
    }

def summariaze_m2(signals):
    summary = {}

    for field in sorted({row["field"] for row in signals}):
        rows = [row for row in signals if row["field"] == field]
        active = [row for row in rows if row["active_samples"] > 0]

        never_seen = sorted(
            row["surgery_id"]
            for row in active 
            if row["active_never_seen_samples"] == row["active_samples"]
        )

        ages = [
            row["max_active_signal_age_s"]
            for row in active
            if row["max_active_signal_age_s"] is not None
        ]

        gaps =[ 
            row["max_active_receive_gap_s"]
            for row in active
            if row["max_active_receive_gap_s"] is not None
        ]

        summary[field] = {
            "surgery_monitored": len(rows),
            "surgery_with_active_samples": len(active),
            "surgery_over_gap_limit": sum(
                row["active_receive_gap_over_limit_samples"] > 0 for row in active
            ),
            "over_limit_sample": sum(
                row["active_receive_gap_over_limit_samples"] for row in active
            ),
            "never_seen_surgeries": never_seen,
            "active_clock_anomaly_samples": sum(
                row["active_clock_anomaly_samples"] for row in active
            ),
            "max_signal_age_s": max(ages, default= None),
            "max_receive_gap_s": max(gaps,default= None),
        }
    return summary

def main():
    """Đọc run, tính metrics tách vitals/alerts và in JSON.

    Args:
        CLI: run-id, metrics-dir, tùy chọn steady interval và --check-db.

    Notes:
        M4/M7 cần critical ground truth; M8 cần probe=True.
        M6 cần --check-db. M10 cần chọn khoảng steady.
        Telemetry lỗi phải được đối chiếu trước kết luận mất dữ liệu.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required= True)
    parser.add_argument("--metrics-dir", required=True, type = Path)
    parser.add_argument("--steady-start-ns", type =int)
    parser.add_argument("--steady-end-ns", type =int)
    parser.add_argument("--signal-gap-limit-s", type = float, default= 10.0)
    parser.add_argument("--check-db", action="store_true")
    args = parser.parse_args()
    if (args.steady_start_ns is None) != (args.steady_end_ns is None):
        parser.error("Both steady-start-ns and steady-end-ns are required")

    run_dir = args.metrics_dir / args.run_id
    data = load_run(run_dir, args.run_id)
    first , end_ns = data["first"], data["observed_end_ns"]
    ev, ea = data["expected_vitals"], data["expected_alerts"]
    critical = {rid: e for rid, e in ev.items() if e["critical"]}
    critical_alerts = {rid: ea[rid] for rid in critical if rid in ea}

    report = {
        "run_id": args.run_id,
        "observed_end_ns": end_ns,
        "telemetry_issues": data["telemetry_issues"],
        "M1":{
            "Name":"Missing-at-Deadline / Data Completeness",
            "vitals": deadline_report(ev, "vitals_persisted", end_ns, first),
            "alerts": deadline_report(ea, "alert_persisted", end_ns, first),
            "fields": field_completeness(ev,  end_ns,  first, data["first_details"]),
        },
        "M3":{
            "Name":"On-time Service Availability / Deadline Miss",
            "vitals": deadline_report(ev, "vitals_persisted", end_ns, first),
            "alerts": deadline_report(ea, "alert_persisted", end_ns, first),
            "fields": field_completeness(ev,  end_ns,  first, data["first_details"]),
        },
        "M4":{
            "Name":"Critical Detection End-to-End",
            "detection": deadline_report(critical, "critical_detected", end_ns, first, deadline_key="critical_deadline_ns"),
            "source_to_detection": distribution(source_latencies_ms(critical, "critical_detected", end_ns, first)),
            #"source_to_alert_persisted": distribution(source_latencies_ms(critical_alerts, "alert_persisted", end_ns, first)),
            "critical_alert_deadlines": deadline_report(critical_alerts, "alert_persisted", end_ns, first),
        },
        "M7_M8_M9_by_fault": [],
    }

    fault_ids = sorted({rid for stage, rid in first if stage == "fault_applied"})
    for fid in fault_ids:
        applied = first[("fault_applied",fid)]
        restored = first.get(("fault_restored", fid))
        if applied >  end_ns:
            continue
        fault = {"fault_id": fid, "applied_ns": applied, "restored_ns": restored}
        if  restored is None or restored >  end_ns:
            fault["status"] = "fault_not_restored_within_observation"
            report["M7_M8_M9_by_fault"].append(fault)
            continue
        cohort = {
            rid:e for  rid, e in critical.items()
            if applied <= e["t0_ns"] < restored
        }

        fault["M7"] = {
            "Name":"Critical-Path Continuity Under Fault",
            "detection":deadline_report(
                cohort,  "critical_detected", end_ns, first, deadline_key=  "critical_deadline_ns"
            ),
            "persistence":  deadline_report(
                {rid: ea[rid] for rid in cohort if rid in ea},
                "alert_persisted",   end_ns, first,
            )
        }

        fault["M8"] =    {
            "Name": "Observed Recovery Time / RTO",
            "vitals": observed_rto(
                probe_samples(ev,  "vitals_persisted", end_ns, first), applied
            ),
            "alerts": observed_rto(
                probe_samples(ea, "alert_persisted", end_ns, first), applied
            ),
            "critical_detection": observed_rto(
                probe_samples(critical, "critical_detected", end_ns,  first, deadline_key= "critical_deadline_ns"), applied
            ),
        }
        for label, expectations, stage, deadline_key in (
            ("vitals", ev, "vitals_persisted", "deadline_ns"),
            ("alerts", ea, "alert_persisted", "deadline_ns"),
            ("critical_detection", critical, "critical_detected", "critical_deadline_ns"),
        ):
            samples = probe_samples(expectations, stage, end_ns, first, deadline_key=deadline_key)
            before = [(t, good) for t, good in samples if applied - 5_000_000_000 <= t < applied]
            baseline_rounds = {}
            for t, good in before:
                baseline_rounds[t] = baseline_rounds.get(t, True) and good
            result = fault["M8"][label]
            result["baseline_rounds"] = len(baseline_rounds)
            result["baseline_healthy"] = len(baseline_rounds) >= 5 and all(baseline_rounds.values())
            if result["status"] == "recovered":
                result["candidate_after_restore_s"] = result["recovery_candidate_s"] - (restored - applied) / 1e9
                result["confirmed_after_restore_s"] = result["recovery_confirmed_s"] - (restored - applied) / 1e9

        fault["M9"] = {
            "Name": "Backlog Drain Time",
            "vitals": backlog_drain(ev,  "vitals_persisted", restored , end_ns, first),
            "alerts": backlog_drain(ea, "alert_persisted", restored, end_ns, first)
        }
        report["M7_M8_M9_by_fault"].append(fault)

    signals = {}
    active_start_ns = min((e["t0_ns"] for e in ev.values()), default=None)
    active_end_ns = max((e["t0_ns"] for e in ev.values()), default=None)
    for row in data["rows"]:
        if row["kind"] != "trace" or row["name"] != "signal_sample":
            continue
        d =row["details"]
        if d.get("at_ns", row["time_ns"]) > end_ns:
            continue
        key = (row["record_id"], d["field"])
        item = signals.setdefault(key,{
            "stream_id":key[0], "surgery_id":d.get("surgery_id"), "field":key[1], "samples": 0,
            "never_seen_samples":  0, "clock_anomaly_samples": 0,
            "max_signal_age_s": None, "max_receive_gap_s": None,
            "max_duration_without_sample_s": None,
            "active_samples": 0,
            "active_never_seen_samples":0,
            "active_clock_anomaly_samples": 0,
            "receive_gap_over_limit_samples": 0,
            "active_samples": 0, "active_never_seen_samples": 0,
            "max_active_signal_age_s": None, "max_active_receive_gap_s": None,
            "active_receive_gap_over_limit_samples": 0,
        })
        item["samples"] += 1
        item["never_seen_samples"] += int(d["status"] == "never_seen")
        item["active_clock_anomaly_samples"] += int(d["status"] == "clock_anomaly")
        for source, target in (
            ("signal_age_s", "max_signal_age_s"),
            ("receive_gap_s", "max_receive_gap_s"),
            ("duration_without_sample_s", "max_duration_without_sample_s"),
        ):
            value =   d.get(source)
            if value is not None and value >= 0:
                current = item[target]
                item[target] = value if current is None else max(current, value)
        gap = d.get("receive_gap_s")
        item["receive_gap_over_limit_samples"] += int(gap is not None and gap > args.signal_gap_limit_s)
        sample_ns = d.get("at_ns", row["time_ns"])
        if active_start_ns is not None and active_start_ns <= sample_ns <= active_end_ns:
            item["active_samples"] += 1
            item["active_never_seen_samples"] += int(d["status"] == "never_seen")
            item["active_clock_anomaly_samples"] += int(d["status"] == "clock_anomaly")
            for source, target in (("signal_age_s", "max_active_signal_age_s"), ("receive_gap_s", "max_active_receive_gap_s")):
                value = d.get(source)
                if value is not None and value >= 0:
                    current = item[target]
                    item[target] = value if current is None else max(current, value)
            item["active_receive_gap_over_limit_samples"] += int(gap is not None and gap > args.signal_gap_limit_s)

    worst_streams = sorted(
        (
            row for row in signals.values()
            if row["active_samples"] > 0
            and row["active_receive_gap_over_limit_samples"] > 0
        ),
        key = lambda row:row["max_active_receive_gap_s"] or 0,
        reverse= True,
    )[:5]
    active_window_s =  (
        (active_end_ns - active_start_ns) / 1e9
        if active_start_ns is not None and active_end_ns is not None
        else None
    )

    report["M2"] = {
        "Name": "Signal Gap / Signal Age",
        "signal_gap_limit_s": args.signal_gap_limit_s,
        "active_window_s": active_window_s,
        "summary_by_field": summariaze_m2(list(signals.values())),
        "num_worst_steams": len(worst_streams),
        "worst_stream":[
            {
                "surgery_id": row["surgery_id"],
                "field": row["field"],
                "max_receive_gap_s":row["max_active_receive_gap_s"],
                "max_signal_age_s":  row["max_active_signal_age_s"],
                "over_limit_samples": row["active_receive_gap_over_limit_samples"],
            }
            for row in worst_streams
        ],
        "note": "A monitor in the same container cannot run while the container is paused or killed.",
    }

    m5 = alert_persistence_report(first, end_ns)
    report["M5"]  = {
        "Name": "Alert Persistence Latency",
        "latency":  distribution(m5["latency_ms"]),
        "unpersisted_alerts": m5["unpersisted_alerts"],
        "timestamp_anomalies": m5["timestamp_anomalies"],
    }

    if args.check_db:
        from sqlalchemy import  create_engine
        from src.realtime_medical.config import AppConfig

        manifest = json.loads(
            (run_dir / "db_keys.json").read_text(encoding = "utf-8")
        )
        engine  =  create_engine(AppConfig.load_config().postgres_dsn)
        try:
            report["M6"] = {
                "Name":"Duplicate Logical Alerts",
                "status": "checked",
                **duplicate_alerts_report(engine, manifest)
            }
        finally:
            engine.dispose()

    if args.steady_start_ns is not None:
        if not args.steady_start_ns < args.steady_end_ns <= end_ns:
            parser.error("Steady interval must precede the cutoff")
        report["M10"] ={
            "Name":  "Sustained Throughput",
            "status": "measured",
            "vitals": throughput_windows(
                first, "vitals_persisted",
                args.steady_start_ns, args.steady_end_ns,
            ),
            "alerts": throughput_windows(
                first, "alert_persisted",
                args.steady_start_ns, args.steady_end_ns,
            ),
            "validation": {
                "vitals": throughput_validation(ev, first, "vitals_persisted", args.steady_start_ns, args.steady_end_ns, end_ns),
                "alerts": throughput_validation(ea, first, "alert_persisted", args.steady_start_ns, args.steady_end_ns, end_ns),
            },
            "note": "Output rate at the tested load; maximum capacity has not been established."
        }

    order_metrics  = (
        "M1",
        "M2",
        "M3",
        "M4",
        "M5",
        "M6",
        "M7_M8_M9_by_fault",
        "M10",
    )
    report = {
        **{
            key: value
            for key,value in report.items()
            if key not in order_metrics
        },
        **{
            key: report[key]
            for key in order_metrics
            if key in report
        },
    }
    print(json.dumps(report, ensure_ascii=False, indent = 2))
if __name__ == "__main__":
    main()
