"""Chạy các kịch bản benchmark và lưu trace, log, report.json."""
from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import hashlib
import uuid
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.realtime_medical.utils.benchmark_analysis import load_run
from src.realtime_medical.utils.fault_injection import pause_fault
from src.realtime_medical.utils.metrics import AuditedMetricsRecorder

RESULTS = ROOT / "test" / "benchmark_results"


def save_json(path, data):
    """Ghi cấu hình hoặc kết quả của lượt chạy."""
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def normal_environment(env):
    """Bỏ các biến benchmark trước khi chạy consumer thường."""
    normal = {key: value for key, value in env.items()
              if not key.startswith("BENCH_") and key != "METRICS_DIR"}
    normal.update(BENCH_ENABLED="0", BENCH_ALERT_ACK_LOSS="0")
    return normal


def first_source_time(run_dir):
    """Tìm t0 đầu tiên; bỏ dòng cuối nếu runner còn đang ghi dở."""
    times = []
    for path in run_dir.glob("runner-*.jsonl"):
        for line in path.read_text(encoding="utf-8").splitlines(keepends=True):
            if not line.endswith("\n"):
                continue
            row = json.loads(line)
            if row.get("name") == "expected_vital":
                times.append(row["details"]["t0_ns"])
    return min(times, default=None)


def command(args, env, *, log=None, timeout=120):
    """Chạy lệnh trong project, giữ stdout/stderr vào log nếu được chỉ định."""
    result = subprocess.run(args, cwd=ROOT, env=env, capture_output=True,
                            text=True, encoding="utf-8", errors="replace", timeout=timeout)
    if log:
        log.write_text(result.stdout + result.stderr, encoding="utf-8")
    if result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {args[:5]}; see {log}")
    return result.stdout


def write_fixtures(directory, cases, ticks):
    """Tạo CSV tổng hợp đủ 7 fields; không sửa CSV nguồn của người dùng."""
    directory.mkdir(parents=True, exist_ok=True)
    from src.realtime_medical.config import AppConfig
    config = AppConfig.load_config()
    values = {"hr": 80, "spo2": 85, "sbp": 120, "dbp": 75, "mbp": 90, "rr": 16, "bt": 37.0}
    columns = ["time_sec", *config.source_mapping.fields]
    for case in cases:
        with (directory / f"case_{case}_numerics.csv").open("w", newline="", encoding="utf-8") as output:
            writer = csv.writer(output)
            writer.writerow(columns)
            for tick in range(ticks):
                writer.writerow([tick, *(values[key] for key in config.source_mapping.fields.values())])


def prepare_replay_windows(source, destination, cases, ticks, anchor_field="rr"):
    """Chụp cửa sổ CSV thật từ field anchor đầu, chỉ dịch time_sec về 0.

    Giữ các hàng trống, cột vắng và giá trị raw; không forward-fill/resample.
    Hàng toàn trống vẫn được runner bỏ như pipeline thường, ghi rõ số lượng.
    """
    import pandas as pd
    from src.realtime_medical.config import AppConfig
    from src.realtime_medical.utils.benchmark_hooks import valid_fields
    config = AppConfig.load_config()
    destination.mkdir(parents=True, exist_ok=True)
    inventory = []
    for case in cases:
        path = source / f"case_{case}_numerics.csv"
        df = pd.read_csv(path)
        columns = [name for name in config.source_mapping.fields if name in df]
        present = df[columns].notna().any(axis=1)
        indices = df.index[present]
        if not len(indices):
            raise ValueError(f"No nonempty readings in {path}")
        first = int(indices[0])
        anchor_used = "first_valid"
        if anchor_field != "first_valid":
            raw_names = [name for name, field in config.source_mapping.fields.items()
                         if field == anchor_field and name in df]
            for raw_name in raw_names:
                anchor_valid = df[raw_name].notna()
                bounds = config.streaming.artifact_filtering.get(anchor_field)
                if bounds:
                    anchor_valid &= df[raw_name].between(bounds.valid_min, bounds.valid_max)
                anchor_indices = df.index[anchor_valid]
                if len(anchor_indices):
                    first = int(anchor_indices[0])
                    anchor_used = anchor_field
                    break
        window = df.iloc[first:first + ticks].copy()
        if len(window) != ticks:
            raise ValueError(f"Not enough rows for {ticks}-row window: {path}")
        origin = float(window["time_sec"].iloc[0])
        window["time_sec"] = window["time_sec"] - origin
        if window["time_sec"].duplicated().any() or not window["time_sec"].is_monotonic_increasing:
            raise ValueError(f"Invalid timeline in {path}")
        nonempty = window[columns].notna().any(axis=1)
        counts = {field: 0 for field in config.canonical_fields.model_dump().values()}
        for _, row in window.loc[nonempty].iterrows():
            raw = {canonical: float(row[column]) for column, canonical in config.source_mapping.fields.items()
                   if column in row and pd.notna(row[column])}
            for field in valid_fields(raw, config):
                counts[field] += 1
        output = destination / path.name
        window.to_csv(output, index=False)
        inventory.append({
            "case": case, "source": str(path), "snapshot": str(output),
            "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "snapshot_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
            "source_rows": len(df), "trimmed_prefix_rows": first,
            "original_start_time_sec": origin, "requested_anchor": anchor_field,
            "anchor_used": anchor_used, "window_rows": len(window),
            "window_end_time_sec": float(window.time_sec.iloc[-1]),
            "nonempty_rows": int(nonempty.sum()),
            "empty_rows_excluded_from_expectations": int((~nonempty).sum()),
            "missing_columns": [name for name in config.source_mapping.fields if name not in df],
            "valid_raw_fields": counts,
        })
    return inventory


def wait_assigned(run_dir, env, *, enabled=True, timeout=90):
    """Chờ assignment Kafka thực sự, tránh mất đầu run vì offset reset latest."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if enabled:
            for path in run_dir.glob("consumer-*.jsonl"):
                if '"consumer_assigned"' in path.read_text(encoding="utf-8"):
                    return
        else:
            logs = command(["docker", "compose", "logs", "--no-color", "consumer"], env)
            if "KAFKA_ASSIGNED" in logs:
                return
        state = command(["docker", "inspect", "--format", "{{.State.Running}}", "medical_consumer"], env)
        if state.strip() != "true":
            raise RuntimeError("Consumer stopped before Kafka assignment")
        time.sleep(1)
    raise TimeoutError("Consumer did not receive Kafka assignment")


def run_scenario(name, suite_id, base_env, cases, fixtures, ticks, *,
                 source_mode="probes", pause_seconds=20.0, gap_seconds=20.0, drop_field="hr"):
    """Phát probe 1 Hz hoặc replay timeline CSV; fault độc lập với nguồn."""
    run_id = f"{suite_id}_{name}"
    from src.realtime_medical.config import AppConfig
    fields = AppConfig.load_config().canonical_fields.model_dump().values()
    pause = name == "consumer_pause"
    run_dir = RESULTS / run_id
    run_dir.mkdir(parents=True)
    env = dict(base_env, BENCH_ENABLED="1", BENCH_RUN_ID=run_id,
               METRICS_DIR=str(RESULTS), BENCH_CASES=",".join(map(str, cases)),
               BENCH_SIGNAL_FIELDS=",".join(fields),
               BENCH_CRITICAL_PROBES="1" if source_mode == "probes" else "0",
               BENCH_REFERENCE_RULES="1" if source_mode == "replay" else "0",
               BENCH_SCHEDULED_REPLAY="1" if source_mode == "replay" else "0",
               BENCH_ALERT_ACK_LOSS="1" if name == "ack_loss" else "0",
               BENCH_DROP_FIELD=drop_field if name == "signal_gap" else "",
               BENCH_DROP_START_TICK="30", BENCH_DROP_END_TICK=str(30 + gap_seconds),
               KAFKA_TOPIC_VITALS=run_id.lower(), KAFKA_BROKER_URL="localhost:9092",
               POSTGRES_HOST="localhost", POSTGRES_PORT="5433", PYTHONIOENCODING="utf-8")
    env["ML_INFERENCE_N_JOBS"] = "1"
    print(f"START {run_id}: {len(cases)} patients, {ticks} ticks", flush=True)
    command(["docker", "compose", "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server",
             "kafka:29092", "--create", "--if-not-exists", "--topic", env["KAFKA_TOPIC_VITALS"],
             "--partitions", "3", "--replication-factor", "1"], env, log=run_dir / "topic.log")
    runner = None
    runner_log = None
    try:
        command(["docker", "compose", "up", "-d", "--no-deps", "--force-recreate", "consumer"], env,
                log=run_dir / "startup.log")
        wait_assigned(run_dir, env)
        runner_log = (run_dir / "runner.log").open("w", encoding="utf-8")
        runner_args = [sys.executable, "-B", "scripts/run_pipeline.py", "--cases", *map(str, cases),
                                   "--workers", str(len(cases)), "--data-dir", str(fixtures),
                                   "--max-rows", str(ticks), "--observe-seconds", "12", "--verify-timeout", "100"]
        if source_mode == "replay":
            runner_args.append("--realtime")
        runner = subprocess.Popen(runner_args,
                                  cwd=ROOT, env=env, stdout=runner_log, stderr=subprocess.STDOUT)
        if pause:
            deadline = time.monotonic() + 45
            started = None
            while started is None and time.monotonic() < deadline:
                started = first_source_time(run_dir)
                time.sleep(0.2)
            if started is None:
                raise RuntimeError("No expectations before fault")
            while time.time_ns() < started + 35_000_000_000:
                if runner.poll() is not None:
                    raise RuntimeError("Runner exited before fault")
                time.sleep(0.2)
            with AuditedMetricsRecorder(RESULTS, run_id, f"fault-{uuid.uuid4().hex[:8]}") as metrics:
                print(f"PAUSE medical_consumer for {pause_seconds:g} seconds", flush=True)
                pause_fault(metrics, "medical_consumer", pause_seconds)
        code = runner.wait(timeout=ticks + 150)
        runner_log.close()
        runner_log = None
        if code:
            raise RuntimeError(f"Runner exited {code}; see {run_dir / 'runner.log'}")
    finally:
        if runner is not None and runner.poll() is None:
            runner.terminate()
            runner.wait(timeout=30)
        if runner_log is not None:
            runner_log.close()
        command(["docker", "compose", "stop", "-t", "30", "consumer"], env, log=run_dir / "stop.log")
        command(["docker", "compose", "logs", "--no-color", "consumer"], env, log=run_dir / "consumer.log")

    data = load_run(run_dir, run_id)
    start_ns = min(item["t0_ns"] for item in data["expected_vitals"].values())
    last_ns = max(item["t0_ns"] for item in data["expected_vitals"].values())
    steady_start = start_ns + 10_000_000_000
    steady_end = start_ns + (last_ns - start_ns) // 10_000_000_000 * 10_000_000_000
    stdout = command([sys.executable, "-B", "scripts/analyze_benchmark.py", "--run-id", run_id,
                      "--metrics-dir", str(RESULTS), "--check-db",
                      "--steady-start-ns", str(steady_start), "--steady-end-ns", str(steady_end)], env,
                     log=run_dir / "analyzer.log")
    report = json.loads(stdout)
    save_json(run_dir / "report.json", report)
    metadata = {"run_id": run_id, "scenario": name, "patients": len(cases), "ticks": ticks,
                "expected_readings": len(data["expected_vitals"]), "source_start_ns": start_ns,
                "source_last_ns": last_ns,
                "source_mode": source_mode, "pause_seconds": pause_seconds if pause else None,
                "gap_seconds": gap_seconds if name == "signal_gap" else None,
                "drop_field": drop_field if name == "signal_gap" else None,
                "drop_start_s": 30 if name == "signal_gap" else None,
                "source_data_dir": str(fixtures),
                "steady_start_ns": steady_start, "steady_end_ns": steady_end,
                "ml_inference_n_jobs": 1,
                "report": str(run_dir / "report.json")}
    save_json(run_dir / "scenario.json", metadata)
    print(f"DONE {name}: vitals={report['M3']['vitals']['on_time_rate']:.3%}, "
          f"alerts={report['M3']['alerts']['on_time_rate']:.3%}, duplicates={report['M6']['duplicate_excess']}, "
          f"audit_issues={len(report['telemetry_issues'])}", flush=True)
    return metadata


def normal_smoke(base_env, cases, fixtures, output):
    """Kiểm tra main và replay realtime cùng tắt metrics với đủ bệnh nhân."""
    env = dict(normal_environment(base_env),
               KAFKA_BROKER_URL="localhost:9092", POSTGRES_HOST="localhost",
               POSTGRES_PORT="5433", PYTHONIOENCODING="utf-8", ML_INFERENCE_N_JOBS="1")
    command(["docker", "compose", "up", "-d", "--no-deps", "--force-recreate", "consumer"], env,
            log=output / "normal_startup.log")
    wait_assigned(output, env, enabled=False)
    command([sys.executable, "-B", "scripts/run_pipeline.py", "--cases", *map(str, cases),
             "--workers", str(len(cases)), "--data-dir", str(fixtures), "--max-rows", "5",
             "--realtime", "--verify-timeout", "90"], env, log=output / "normal_runner.log", timeout=120)
    command(["docker", "compose", "logs", "--no-color", "consumer"], env, log=output / "normal_consumer.log")
    import pandas as pd
    from src.realtime_medical.config import AppConfig
    config = AppConfig.load_config()
    readings = 0
    for case in cases:
        sample = pd.read_csv(fixtures / f"case_{case}_numerics.csv", nrows=5)
        columns = [column for column in config.source_mapping.fields if column in sample]
        readings += int(sample[columns].notna().any(axis=1).sum())
    print(f"DONE normal mode: BENCH_ENABLED=0, {len(cases)} patients, {readings} nonempty readings", flush=True)
    return readings


def main():
    """Chạy các kịch bản đã chọn rồi khôi phục consumer thường."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite-id", default=datetime.now(timezone.utc).strftime("bench_%Y%m%dT%H%M%SZ"))
    parser.add_argument("--scenarios", nargs="+", choices=["baseline", "consumer_pause", "ack_loss", "signal_gap"],
                        default=["baseline", "consumer_pause", "ack_loss", "signal_gap"])
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--patients", type=int, default=40)
    parser.add_argument("--ticks", type=int, help="Maximum CSV rows per case; probes emit at 1 Hz")
    parser.add_argument("--source-mode", choices=["probes", "replay"], default="probes")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data" / "streaming")
    parser.add_argument("--window-anchor", default="rr",
                        help="First valid field sample, or first_valid; missing field falls back to first nonempty row")
    parser.add_argument("--pause-seconds", type=float, default=20.0)
    parser.add_argument("--gap-seconds", type=float, default=20.0)
    parser.add_argument("--drop-field", default="hr")
    args = parser.parse_args()
    if not 30 <= args.patients <= 50:
        parser.error("patients must be between 30 and 50")
    if args.ticks is not None and args.ticks < 70:
        parser.error("ticks must be >= 70 to provide enough probes before and after the fault")
    if not 0 < args.pause_seconds <= 60 or not 0 < args.gap_seconds <= 60:
        parser.error("pause/gap duration must be >0 and <=60 seconds")
    if "consumer_pause" in args.scenarios and 35 + args.pause_seconds + 10 >= (args.ticks or 100):
        parser.error("Need at least 10 seconds of source after fault restoration")
    if "signal_gap" in args.scenarios and 30 + args.gap_seconds + 10 >= (args.ticks or 70):
        parser.error("Need at least 10 seconds of source after signal restoration")
    if len(args.scenarios) != len(set(args.scenarios)):
        parser.error("scenarios must not contain duplicates")
    if not args.suite_id or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in args.suite_id):
        parser.error("suite-id may only contain letters, digits, _ and -")
    from src.realtime_medical.config import AppConfig
    config = AppConfig.load_config()
    fields = config.canonical_fields.model_dump().values()
    if args.drop_field not in fields:
        parser.error("drop-field must be a configured canonical field")
    if args.window_anchor != "first_valid" and args.window_anchor not in fields:
        parser.error("window-anchor must be a canonical field or first_valid")
    output = RESULTS / args.suite_id
    if output.exists():
        parser.error("suite-id already exists; use a new ID")
    output.mkdir(parents=True)
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    cases = list(range(1, args.patients + 1))
    fixtures = output / "fixtures"
    if args.source_mode == "replay":
        inventory = prepare_replay_windows(args.data_dir.resolve(), fixtures, cases,
                                           args.ticks or 100, args.window_anchor)
        save_json(output / "input_inventory.json", inventory)
    else:
        write_fixtures(fixtures, cases, max(100, args.ticks or 0))
    from sqlalchemy import create_engine, text
    provenance = {
        "source_mode": args.source_mode, "patients": args.patients, "ticks": args.ticks,
        "pause_seconds": args.pause_seconds, "gap_seconds": args.gap_seconds,
        "drop_field": args.drop_field, "window_anchor": args.window_anchor,
        "config_sha256": hashlib.sha256((ROOT / "configs/config.yaml").read_bytes()).hexdigest(),
        "model_sha256": hashlib.sha256((ROOT / config.model.path).read_bytes()).hexdigest(),
        "mapping": config.source_mapping.fields,
    }
    save_json(output / "provenance.json", provenance)
    engine = create_engine(config.postgres_dsn)
    try:
        with engine.connect() as connection:
            counts = connection.execute(text("SELECT count(*), count(DISTINCT patient_id) FROM surgeries WHERE surgery_id BETWEEN 1 AND :last"), {"last": args.patients}).one()
            if tuple(counts) != (args.patients, args.patients):
                raise RuntimeError(f"Need {args.patients} seeded distinct patients; got {tuple(counts)}")
    finally:
        engine.dispose()
    if not args.skip_build:
        print("Building consumer image...", flush=True)
        command(["docker", "compose", "build", "consumer"], env, log=output / "build.log", timeout=600)
    runs = []
    try:
        for name in args.scenarios:
            runs.append(run_scenario(name, args.suite_id, env, cases, fixtures,
                                     args.ticks or (100 if name in {"baseline", "consumer_pause"} else 70),
                                     source_mode=args.source_mode, pause_seconds=args.pause_seconds,
                                     gap_seconds=args.gap_seconds, drop_field=args.drop_field))
            save_json(output / "suite.json", runs)
        normal_count = normal_smoke(env, cases, fixtures, output)
        save_json(output / "normal_mode.json", {"status": "passed", "patients": len(cases),
                                              "readings": normal_count, "planned_rows": len(cases) * 5})
    finally:
        # Không để fault/benchmark active sau khi kết thúc hoặc lỗi.
        normal = normal_environment(env)
        command(["docker", "compose", "up", "-d", "--no-deps", "--force-recreate", "consumer"], normal,
                log=output / "restore.log")
    print(f"SUITE COMPLETE: {output}", flush=True)


if __name__ == "__main__":
    main()
