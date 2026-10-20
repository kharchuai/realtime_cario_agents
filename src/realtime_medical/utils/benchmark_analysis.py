"""Tính metrics sau benchmark từ expectation, trace và DB đối chiếu."""

from __future__ import   annotations
import json
from pathlib import Path

def load_run(run_dir, run_id):
    """Đọc JSONL và dựng expectation/index thời điểm trace đầu tiên.

    Args:
        run_dir: Thư mục METRICS_DIR/BENCH_RUN_ID.
        run_id: ID phải khớp mọi dòng.

    Returns:
        Dict rows, first, first_details, expected_vitals, expected_alerts,
        observed_end_ns và telemetry_issues.

    Raises:
        FileNotFoundError: Không có JSONL.
        ValueError: JSON lỗi, lẫn run, expectation mâu thuẫn.
        RuntimeError: Chưa có observation_ended.

    Notes:
        Dòng cuối bị cắt được bỏ và ghi vấn đề audit.
        Sidecar thiếu/chưa closed sau kill không tự chứng minh mất dữ liệu.
        Đọc khi writer đã dừng; đối chiếu DB nếu telemetry có vấn đề.
    """
    directory = Path(run_dir)
    paths = sorted(directory.glob("*.jsonl"))
    if not paths:
        raise FileNotFoundError(f"No JSONL files found in {directory}")
    rows, issues = [], []
    for path in paths:
        with path.open(encoding = "utf-8") as source:
            for number, line in enumerate(source, 1):
                if not line.endswith("\n"):
                    issues.append(f"Truncated final line: {path.name}")
                    continue
                row =json.loads(line)
                if row.get("run_id") != run_id:
                    raise ValueError(f"run_id mismatch: {path.name}:{number}")
                rows.append(row)
        health_path = path.with_suffix(".health.json")
        if not health_path.exists():
            issues.append(f"Missing sidecar: {health_path.name}")
            continue
        health = json.loads(health_path.read_text(encoding = "utf-8"))
        if health.get("run_id") != run_id:
            raise ValueError(f"Sidecar run_id mismatch: {health_path.name}")
        if (
            not health.get("closed") or health.get("dropped_rows", 0)
            or health.get("write_failures", 0) or health.get("close_failed")
        ):
            issues.append(f"Collector is incomplete or has failed: {path.name}")
    first, details, ev, ea, ends = {}, {}, {},  {}, []
    for row in rows:
        if row["kind"] != "trace":
            continue
        stage, rid = row["name"], row["record_id"]
        at_ns = row["details"].get("at_ns", row["time_ns"])
        key = (stage, rid)
        if key not in first or at_ns < first[key]:
            first[key], details[key]  = at_ns, row["details"]
        if stage in {"expected_vital", "expected_alert"}:
            target = ev if stage == "expected_vital" else ea
            if rid in target and target[rid] != row["details"]:
                raise ValueError(f"Conflicting expectation: {rid}")
            target[rid] = row["details"]
        if stage == "observation_ended":
            ends.append(at_ns)
    if not ends:
        raise RuntimeError("Runner has not recorded observation_ended")
    return {
        "rows":rows, "first":first, "first_details": details,
        "expected_vitals":ev, "expected_alerts": ea,
        "observed_end_ns": max(ends), "telemetry_issues": issues,
    }

def deadline_report(expected, sink_stage, observed_end_ns, first, deadline_key  = "deadline_ns"):
    """Tính M1/M3 missing-at-deadline và on-time trên cohort đủ deadline.

    Args:
        expected: Dict record_id → expectation details.
        sink_stage: vitals_persisted, alert_persisted hoặc critical_detected.
        observed_end_ns: Cutoff quan sát do runner ghi.
        first: Dict (stage, record_id) → thời điểm đầu tiên.
        deadline_key: deadline_ns hoặc critical_deadline_ns.

    Returns:
        Counts on_time, late, not_observed, deadline_not_reached; các rate.
        Cohort rỗng trả rate=None.

    Notes:
        not_observed là chưa có ACK trước cutoff, không khẳng định mất vĩnh viễn.
        Mẫu chưa đến deadline không nằm trong mẫu số.
        late và not_observed đều thiếu tại deadline.
    """
    counts = dict(on_time = 0, late = 0 , not_observed = 0, deadline_not_reached = 0)
    for rid, item in expected.items():
        deadline = item[deadline_key]
        if deadline >   observed_end_ns:
            counts["deadline_not_reached"] += 1
            continue
        done = first.get((sink_stage, rid))
        if done is None or done > observed_end_ns:
            counts["not_observed"] += 1
        elif done <= deadline:
            counts["on_time"] += 1
        else:
            counts["late"] += 1
    eligible = counts["on_time"] + counts["late"] + counts["not_observed"]
    missing = counts["late"] + counts["not_observed"]
    return {
        **counts, "eligible": eligible,
        "on_time_rate": counts["on_time"] / eligible if eligible else None,
        "deadline_miss_rate": missing / eligible if eligible else None,
        "missing_at_deadline": missing,
    }

def field_completeness(expected, observed_end_ns, first, first_details):
    """Tính M1: phần trường raw hợp lệ đã lưu trước deadline.

    Args:
        expected: Expectations có required_fields theo hợp đồng nguồn.
        observed_end_ns: Cutoff quan sát.
        first: Index thời điểm trace đầu tiên.
        first_details: Metadata của chính trace đầu tiên đó.

    Returns:
        required_fields, valid_fields_on_time, completeness_rate.

    Notes:
        Reading chưa lưu đúng hạn đóng góp 0 trường tại deadline.
        Không đếm dữ liệu forward-fill thành raw sample đầy đủ.
    """
    required_count, valid_count = 0, 0
    for rid, item in expected.items():
        if item["deadline_ns"] > observed_end_ns:
            continue
        required = set(item["required_fields"])
        required_count += len(required)
        key = ("vitals_persisted", rid)
        done = first.get(key)
        if done is not None and done <= item["deadline_ns"]:
            valid = set(first_details[key].get("valid_fields", []))
            valid_count += len(required & valid)
    return {
        "required_fields": required_count,
        "valid_fields_on_time": valid_count,
        "completeness_rate": valid_count/ required_count if required_count else None,
    }

def source_latencies_ms(expected, sink_stage, observed_end_ns, first):
    """Trả latency nguồn → sink của các mẫu hoàn thành, dùng cho M4.

    Args:
        expected: Cohort chứa t0_ns, thường là critical positives.
        sink_stage: critical_detected hoặc alert_persisted.
        observed_end_ns: Cutoff quan sát.
        first: Index trace đầu tiên.

    Returns:
        List milliseconds; báo miss count riêng bằng deadline_report.

    Raises:
        ValueError: Latency âm, cần kiểm tra clock/timeline nguồn.
    """
    samples = []
    for rid, item in  expected.items():
        done = first.get((sink_stage, rid))
        if done is None or done > observed_end_ns:
            continue
        if done < item["t0_ns"]:
            raise ValueError("Negative latency: Check clock/t0_ns")
        samples.append((done - item["t0_ns"]) / 1e6)
    return samples

def probe_samples(expected, sink_stage, observed_end_ns, first, deadline_key = "deadline_ns"):
    """Dựng chuỗi kết quả probe tại deadline để tính M8.

    Args:
        expected: Expectations có probe=True và lịch đều.
        sink_stage: Luồng cần đánh giá phục hồi.
        observed_end_ns: Chỉ xét probe đã đến deadline.
        first: Index ACK/detection đầu tiên.
        deadline_key: Deadline phù hợp với luồng.

    Returns:
        List (decision_ns, good) đã sort; decision_ns là deadline probe.

    Notes:
        Probe thiếu đầu ra trở thành False, không bị loại khỏi chuỗi.
        Với critical_detected chỉ truyền critical probes.
    """
    samples =  []
    for rid, item in expected.items():
        deadline = item[deadline_key]
        if not item.get("probe") or deadline > observed_end_ns:
            continue
        done = first.get((sink_stage, rid))
        samples.append((deadline, done is not None and done <= deadline))
    return sorted(samples)

def observed_rto(samples, fault_start_ns, consecutive = 5):
    """Tính M8 bằng các vòng probe tốt liên tiếp sau failure quan sát được.

    Args:
        samples: Chuỗi đầy đủ từ probe_samples.
        fault_start_ns: Mốc fault_applied.
        consecutive: Số vòng deadline tốt liên tiếp để xác nhận recovery.

    Returns:
        Status và candidate/confirmed seconds từ onset nếu recovered.
        Không có failure trả no_outage_observed, không tự gán RTO=0.

    Notes:
        Baseline trước fault phải khỏe. Probe theo lịch đều.
        Không recovered trước cutoff là quan sát bị giới hạn, không phải RTO=0.
    """
    if consecutive < 1:
        raise ValueError("consecutive must be >= 1")
    # 40 bệnh nhân cùng tick phải cùng đạt; 5 bệnh nhân tốt trong một giây
    # không tương đương 5 vòng probe liên tiếp.
    rounds = {}
    for decision_ns, good in samples:
        if decision_ns >= fault_start_ns:
            rounds[decision_ns] = rounds.get(decision_ns, True) and good
    relevant = sorted(rounds.items())
    outage_seen, streak = False, []
    for decision_ns,  good in relevant:
        if not good:
            outage_seen = True
            streak.clear()
            continue
        if not  outage_seen:
            continue
        streak.append(decision_ns)
        if len(streak) == consecutive:
            return {
                "status": "recovered",
                "recovery_candidate_s": (streak[0] - fault_start_ns)  / 1e9,
                "recovery_confirmed_s": (streak[-1] - fault_start_ns) / 1e9,
            }
    return {
        "status": (
            "not_recovered_within_observation" if outage_seen
            else "no_outage_observed" if relevant else
            "no_mature_probes"
        ),
        "observed_for_s":((relevant[-1][0] - fault_start_ns) / 1e9 if relevant else None),
    }

def alert_persistence_report(first, observed_end_ns):
    """Tính M5: alert_generated → alert_persisted; báo cả chưa lưu.

    Args:
        first: Index trace đầu tiên keyed theo ID reading.
        observed_end_ns: Cutoff quan sát.

    Returns:
        latency_ms, unpersisted_alerts và timestamp_anomalies.

    Notes:
        Retry không làm mới thời điểm generation.
        Missing generation trace khiến cohort không đầy đủ; xem audit.
    """
    samples, pending, anomalies = [], 0, 0
    for (stage, rid), generated_ns in first.items():
        if stage!= "alert_generated" or generated_ns > observed_end_ns:
            continue
        done = first.get(("alert_persisted", rid))
        if done is None or done > observed_end_ns:
            pending += 1
        elif done < generated_ns:
            anomalies += 1
        else:
            samples.append((done - generated_ns) / 1e6)
    return {
        "latency_ms": samples,
        "unpersisted_alerts": pending,
        "timestamp_anomalies": anomalies,
    }


def backlog_drain(expected, sink_stage, restored_ns, observed_end_ns, first):
    """Tính M9 cho tập công việc outstanding tại lúc khôi phục.

    Args:
        expected: Cohort vitals hoặc alerts.
        sink_stage: Stage hoàn thành tương ứng.
        restored_ns: Mốc fault_restored.
        observed_end_ns: Cutoff, phải >= restore.
        first: Index trace hoàn thành đầu tiên.

    Returns:
        backlog, remaining, drain_time_s, observed_for_s.
        drain_time_s=None nếu còn công việc chưa hoàn thành.

    Notes:
        Đây là cohort cố định tại restore, không đồng nghĩa tổng backlog
        toàn hệ thống về mức nền khi traffic mới vẫn tiếp tục.
    """
    if observed_end_ns <   restored_ns:
        raise ValueError("Cutoff must be >= restore")
    outstanding = [
        rid for rid,  item in expected.items()
        if item["t0_ns"] <= restored_ns and(
            first.get((sink_stage, rid)) is None
            or first[(sink_stage, rid)] > restored_ns
        )
    ]
    completed, remaining = [], 0
    for rid in outstanding:
        done = first.get((sink_stage, rid))
        if done is None or done > observed_end_ns:
            remaining += 1
        else:
            completed.append(done)
    return {
        "backlog": len(outstanding),
        "remaining": remaining,
        "drain_time_s": (
            None if remaining else
            (max(completed) - restored_ns) / 1e9 if completed else 0.0
        ),
        "observed_for_s":  (observed_end_ns - restored_ns) / 1e9,
    }

def throughput_windows(first, sink_stage, start_ns, end_ns, window_s = 10):
    """Tính M10 bằng unique logical completions trong cửa sổ cố định.

    Args:
        first: Index earliest ACK; retry không tăng năng suất.
        sink_stage: vitals_persisted hoặc alert_persisted.
        start_ns: Đầu steady-state sau warm-up.
        end_ns: Cuối khoảng đo.
        window_s: Độ dài cửa sổ, phải > 0.

    Returns:
        List cửa sổ đủ độ dài với throughput_per_s; giữ cửa sổ zero.

    Notes:
        Phải đối chiếu input rate, backlog và SLA trước kết luận capacity.
    """
    if window_s <= 0:
        raise ValueError("window_s must be > 0")
    width_ns = int(window_s * 1e9)
    if width_ns <  1:
        raise ValueError("windows are too small")
    completions = [t for (stage, _rid), t in first.items() if stage == sink_stage]
    result, left = [], start_ns
    while left + width_ns <=  end_ns:
        right = left + width_ns
        count = sum(left <= t < right for t in completions )
        result.append({
            "start_ns": left,
            "end_ns":right,
            "unique_completed": count,
            "throughput_per_s": count / window_s,
        })
        left = right
    return result

def duplicate_alerts_report(engine, manifest):
    """Tính M6 từ DB thực tế, giới hạn theo khóa nguồn của run.

    Args:
        engine: SQLAlchemy Engine đến DB benchmark.
        manifest: List dict surgery_id, recorded_at. Timestamp UTC naive
            ISO string để khớp schema TIMESTAMP hiện tại.

    Returns:
        actual_alert_rows, distinct_logical_alerts, duplicate_excess,
        duplicate_row_rate và duplicate_groups.

    Notes:
        Một alert logic trên mỗi reading. Không group theo reason/risk/serial ID.
        Đọc sau khi writer dừng; snapshot không khôi phục timestamp commit.
    """
    from sqlalchemy import text

    query = text("""
        WITH expected_keys AS  (
            SELECT DISTINCT surgery_id, recorded_at
            FROM jsonb_to_recordset(CAST(:manifest AS jsonb))
            AS x(surgery_id integer, recorded_at timestamp)
            )
        SELECT a.surgery_id, a.recorded_at, COUNT(*) AS n
        FROM triage_alerts a
        JOIN expected_keys e ON e.surgery_id = a.surgery_id
                            AND e.recorded_at = a.recorded_at
        GROUP BY   a.surgery_id, a.recorded_at
    """)
    with engine.connect() as connection:
        groups = connection.execute(
            query, {"manifest": json.dumps(manifest)}
        ).mappings().all()
    total = sum(int(row["n"]) for row in groups)
    excess = sum(max(int(row["n"]) -1, 0 ) for row in groups)
    return {
        "actual_alert_rows": total,
        "distinct_logical_alerts": len(groups),
        "duplicate_excess": excess,
        "duplicate_row_rate": excess / total if total else None,
        "duplicate_groups": [
            {
                "surgery_id": int(row["surgery_id"]),
                "recorded_at": row["recorded_at"].isoformat(),
                "actual_rows": int(row["n"]),
            }
            for row in groups if int(row["n"]) > 1
        ],
    }
