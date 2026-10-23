from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
import json
import os
import math

import heapq 

from concurrent.futures  import ThreadPoolExecutor, FIRST_COMPLETED, wait
from  dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from  pathlib import Path

import pandas as pd
from confluent_kafka.admin import AdminClient
from sqlalchemy import bindparam, create_engine, text

# Cho phép chạy trực tiếp:
# python scripts/run_pipeline.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.realtime_medical.config import AppConfig
from src.realtime_medical.generator.sensor_simulator import _build_vitals_row
from src.realtime_medical.streaming.producer import VitalsProducer
from src.realtime_medical.utils.benchmark_hooks import expect_reading,  make_metrics,  VITAL_DEADLINE_S, ALERT_DEADLINE_S, CRITICAL_DETECTION_DEADLINE_S
from src.realtime_medical.utils.benchmark_reference import reference_critical
from src.realtime_medical.utils.measured_run import MeasureRun

bench_enabled = (
    os.environ.get("BENCH_ENABLED", "0").strip().lower()
    in {"1", "true", "yes", "on"}
)

log_dir = PROJECT_ROOT / "logs" / "pipeline"
log_dir.mkdir(parents=True, exist_ok=True)
handlers = [
    logging.StreamHandler(sys.stdout),
    logging.FileHandler(log_dir / "runner.log", mode="a", encoding="utf-8"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(threadName)s - %(message)s",
    handlers=handlers,
    force=True,
)
logger = logging.getLogger("multi_patient_pipeline")

@dataclass(frozen = True)
class ReplaySource:
    surgery_id: int
    patient_id: int
    csv_path: Path

@dataclass
class  ReplayResult:
    surgery_id: int
    patient_id: int
    csv_path: Path
    accepted: int = 0
    skipped_empty: int = 0
    skipped_duplicate_timestamp: int = 0
    rejected: int = 0
    db_keys: list[dict] = field(default_factory= list)


def parse_args() ->  argparse.Namespace:
    parser = argparse.ArgumentParser(
        description = (
            "Replay multiple case_<id>_numerics.csv files concurrently "
            "to Kafka and verify the data in PostgreSQL."
        )
    )

    parser.add_argument(
        "--cases",
        type = int,
        nargs= "+",
        required =  True,
        help = "List of case/surgery IDs, e.g.: --cases 1 2 3 4 5",
    )

    parser.add_argument(
        "--data-dir",
        type= Path,
        default = PROJECT_ROOT/ "data"/ "streaming",
        help =  "Directory containing case_<id>_numerics.csv"
    )

    parser.add_argument(
        "--workers",
        type = int,
        default = 0,
        help =(
            "Number of files to replay concurrently. "
            "Defaults to the number of cases, up to 10."
        ),
    )

    parser.add_argument(
        "--max-rows",
        type = int,
        default = 200,
        help = "Maximum rows per CSV. Use 0 to replay the entire file."
    )

    parser.add_argument(
        "--delay",
        type = float,
        default = 0,
        help = "Delay between messages, in seconds.",
    )

    parser.add_argument(
        "--verify-timeout",
        type = float,
        default = 120,
        help = "Maximum time to wait for the consumer to persist data to the database.",
    )

    parser.add_argument(
        "--realtime",
        action = "store_true",
        help = "Emit data according to CSV time_sec, preserving the intervals between readings."
    )
    parser.add_argument(
        "--observe-seconds",
        type=float,
        default=0.0,
        help=(
            "Minimum observation period after source emission completes; "
            "only applies when benchmarking is enabled."
        ),
    )
    parser.add_argument(
        "--fault",
        choices= ["consumer_pause"],
        default=  None,
    )
    parser.add_argument("--fault-at", type = float, default= 300)
    parser.add_argument("--fault-duration", type = float, default= 10)
    parser.add_argument(
        "--critical-probes",
        action = "store_true",
        help = "Use scheduled, synthesized critical probes to evaluate M8."
    )
    return parser.parse_args()

def wait_for_database(engine, timeout:float = 30) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None

    while  time.monotonic() < deadline:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise TimeoutError(
        f"PostgreSQL is not ready after {timeout} seconds: {last_error}"
    )

def check_kafka_topic(config:  AppConfig, timeout: float = 15) -> None:
    admin = AdminClient(
        {"bootstrap.servers": config.kafka_broker_url}
    )

    metadata = admin.list_topics(timeout=timeout)
    topic_metadata = metadata.topics.get(config.kafka_topic_vitals)

    if topic_metadata is None:
        raise RuntimeError(
            f"Kafka topic {config.kafka_topic_vitals!r} does not exist"
        )

    logger.info(
        "Kafka topic %s is ready with %d partitions",
        config.kafka_topic_vitals,
        len(topic_metadata.partitions),
    )

def build_source(
    engine,
    case_ids: list[int],
    data_dir: Path,
) -> list[ReplaySource]:
    if len(case_ids) != len(set(case_ids)):
        raise ValueError("The --cases list contains duplicate surgery_id values")

    sources: list[ReplaySource] =  []

    query = text("""
        SELECT patient_id
        FROM surgeries
        WHERE surgery_id = :surgery_id
    """)

    with engine.connect() as connection:
        for surgery_id in case_ids:
            row = connection.execute(
                query,
                {"surgery_id": surgery_id},
            ).first()

            if row is None:
                raise RuntimeError(
                    f"Could not find surgery_id={surgery_id} in the database. "
                    "Seed patient/surgery data first."
                )

            csv_path = (
                data_dir / f"case_{surgery_id}_numerics.csv"
            ).resolve()

            if not csv_path.is_file():
                raise FileNotFoundError(
                    f"CSV not found for surgery_id={surgery_id}: "
                    f"{csv_path}"
                )

            sources.append(
                ReplaySource(
                    surgery_id = surgery_id,
                    patient_id = int(row.patient_id),
                    csv_path = csv_path,
                )
            )
    return sources

def replay_one_source(
    sources: ReplaySource,
    config: AppConfig,
    producer: VitalsProducer,
    replay_base_time: datetime,
    max_rows: int,
    delay: float,
    realtime: bool = False,
    runner_metrics = None,
):
    result = ReplayResult(
        surgery_id = sources.surgery_id,
        patient_id = sources.patient_id,
        csv_path =sources.csv_path,
    )

    logger.info(
        "Preparing surgery=%s patient=%s file=%s",
        sources.surgery_id,
        sources.patient_id,
        sources.csv_path.name,
    )

    
    probe_mode = (runner_metrics.enabled and os.environ.get("BENCH_CRITICAL_PROBES", "0") == "1")
    probe_period_ns = 1_000_000_000
    # Chung một lịch cho mọi bệnh nhân; 1 tick = 1 vòng probe toàn hệ thống.
    probe_base_wall_ns = int(replay_base_time.timestamp() * 1e9)
    probe_base_mono_ns = time.monotonic_ns() + probe_base_wall_ns - time.time_ns()
    probe_index = 0
    next_send_mono_ns = time.monotonic_ns()
    nrows = max_rows if max_rows > 0 else None

    csv_chunks = pd.read_csv(
        sources.csv_path,
        nrows = nrows,
        chunksize = 1000,
    )

    seen_timestamps:  set[datetime] = set()

    for chunk in csv_chunks:
        for row_index, row in chunk.iterrows():
            vitals = _build_vitals_row(row, config)

            if not vitals:
                result.skipped_empty += 1
                continue
            raw_time_sec = row.get("time_sec")

            if  pd.isna(raw_time_sec):
                time_sec = float(row_index)
            else:
                time_sec = float(raw_time_sec)

            # Dùng timeline gốc của CSV thay vì datetime.now()
            # cho từng message.

            recorded_at = replay_base_time + timedelta(seconds = time_sec)

            # PK của DB là (surgery_id, recorded_at).
            if recorded_at in seen_timestamps:
                result.skipped_duplicate_timestamp += 1
                continue

            seen_timestamps.add(recorded_at)

            vitals["time_sec"] = time_sec
            vitals["recorded_at"] = recorded_at.isoformat(timespec = "microseconds")

            if probe_mode:
                due_mono_ns = (probe_base_mono_ns + probe_index *  probe_period_ns)
            elif realtime:
                due_mono_ns = (probe_base_mono_ns + int(time_sec * 1_000_000_000))
            else:
                due_mono_ns =  next_send_mono_ns
            yield  due_mono_ns

            source_ns = time.time_ns()

            if (runner_metrics.enabled and not probe_mode and (realtime or os.environ.get("BENCH_SCHEDULED_REPLAY") =="1")):
                source_ns = int(recorded_at.timestamp() * 1_000_000_000)
            if probe_mode:
                source_ns = (probe_base_wall_ns + probe_index * probe_period_ns)
                vitals["recorded_at"] = datetime.fromtimestamp(source_ns / 1e9, tz=  timezone.utc).isoformat(timespec= "microseconds")
                vitals["time_sec"] = (probe_index * probe_period_ns / 1e9)
                probe_index += 1
            critical_reference = False
            alert_reference = False
            probe_reference = False
            if probe_mode:
                vitals.update({
                    "hr":80,
                    "spo2":85,
                    "sbp":120,
                    "dbp":75,
                    "mbp":90,
                    "rr":16,
                    "bt":37.0,
                })
                critical_reference = True
                alert_reference = True
                probe_reference = True
            elif runner_metrics.enabled:
                critical_reference = reference_critical(vitals, config)
                alert_reference = critical_reference
            if runner_metrics.enabled:
                # Giữ oracle trước fault; trường thiếu tự nhiên không được tự điền.
                drop_field = os.environ.get("BENCH_DROP_FIELD", "")
                drop_start = float(os.environ.get("BENCH_DROP_START_TICK", "30"))
                drop_end = float(os.environ.get("BENCH_DROP_END_TICK", "50"))
                current_tick = probe_index - 1 if probe_mode else time_sec
                if drop_field and drop_start <= current_tick < drop_end:
                    vitals.pop(drop_field, None)
            payload = expect_reading(
                runner_metrics, sources.surgery_id, vitals,
                t0_ns=source_ns,
                critical = critical_reference,
                alert_expected= alert_reference,
                required_fields=tuple(config.canonical_fields.model_dump().values()),
                probe =  probe_reference,
            )
            dt = datetime.fromisoformat(vitals["recorded_at"].replace("Z", "+00:00"))
            if runner_metrics.enabled:
                result.db_keys.append({
                    "surgery_id": sources.surgery_id,
                    "recorded_at": dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec="microseconds")
                })
            accepted = producer.send_vital_sign(
                surgery_id = sources.surgery_id,
                patient_id = str(sources.patient_id),
                vital_data = payload,
            )


            if accepted:
                result.accepted += 1
            else:
                result.rejected += 1

            if result.accepted % 1000 == 0 and result.accepted > 0:
                logger.info("surgery=%s enqueued %d messages",sources.surgery_id,result.accepted)

            if not probe_mode and not realtime:
                next_send_mono_ns = (time.monotonic_ns() + int(delay * 1_000_000_000))

    logger.info(
        "Producer completed surgery=%s: "
        "accepted=%d empty=%d duplicate_time=%d rejected=%d",
        sources.surgery_id,
        result.accepted,
        result.skipped_empty,
        result.skipped_duplicate_timestamp,
        result.rejected,
    )

    return result

def advance_replay(steps):
     """Chạy đến hạn chờ tiếp theo, hoặc trả kết quả khi hết CSV."""
     try:
         return next(steps), None
     except StopIteration as finished:
         return None, finished.value

def get_persisted_counts(engine, surgery_ids:list[int], replay_started_at: datetime) -> dict[int, int]:
    query = text(
        """
        SELECT surgery_id,  COUNT(*) AS row_count
        FROM vital_signs
        WHERE surgery_id IN :surgery_ids
            AND recorded_at >= :replay_started_at
        GROUP BY surgery_id
        """
    ).bindparams(
        bindparam("surgery_ids", expanding = True)
    )

    counts = {surgery_id: 0 for surgery_id in surgery_ids}

    with engine.connect() as connection:
        rows = connection.execute(
            query,
            {
                "surgery_ids": surgery_ids,
                "replay_started_at": replay_started_at.astimezone(timezone.utc).replace(tzinfo=None)
            },
        )

        for row in rows:
            counts[int(row.surgery_id)] = int(row.row_count)

    return counts

def wait_for_database_results(engine, expected: dict[int, int], replay_started_at: datetime, timeout:float,) -> dict[int, int]:
    deadline = time.monotonic() + timeout
    surgery_ids = list(expected)
    actual = {surgery_id:0 for surgery_id in surgery_ids}

    while time.monotonic() < deadline:
        actual = get_persisted_counts(
            engine,
            surgery_ids,
            replay_started_at,
        )

        progress = ", ".join(
            f"{surgery_id}={actual[surgery_id]}/{expected[surgery_id]}"
            for surgery_id in surgery_ids
        )
        logger.info("DB progress: %s", progress)

        if all(
            actual[surgery_id] >= expected[surgery_id]
            for surgery_id in surgery_ids
        ):
            return actual

        time.sleep(2)

    return actual


def run_replay(args, measured = None) -> int:
    """Phát CSV và kiểm tra DB, ghi metrics khi BENCH_ENABLED được bật."""
    if args.observe_seconds<  0:
        raise ValueError("--observe_second must > 0")
    if args.delay < 0:
        raise ValueError("--delay must > 0 ")


    config = AppConfig.load_config()
    engine = create_engine(
        config.postgres_dsn,
        pool_pre_ping = True,
    )

    producer = None
    runner_metrics = None
    try:
        wait_for_database(engine)
        check_kafka_topic(config)

        sources = build_source(
            engine = engine,
            case_ids =args.cases,
            data_dir = args.data_dir,
        )

        worker_count = args.workers

        if worker_count <= 0:
            worker_count = min(len(sources), 10)

        worker_count = min(worker_count, len(sources))

        logger.info(
            "Starting replay of %d surgeries with %d workers",
            len(sources),
            worker_count,
        )

        # Schema hiện tại dùng TIMESTAMP không timezone,
        # do đó chuyển UTC về dạng naive để so sánh DB ổn định.

        replay_started_at = datetime.now(timezone.utc)

        producer = VitalsProducer(config)
        runner_metrics = make_metrics("runner")

        results: list[ReplayResult] = []
        worker_errors: list[str] = []

        streams = [(
            source,
            replay_one_source(
                sources= source,
                config =config,
                producer= producer,
                replay_base_time= replay_started_at,
                max_rows= args.max_rows,
                delay = args.delay,
                realtime= args.realtime,
                runner_metrics= runner_metrics,
            ),
        ) for source in sources]
        ready  = []
        if measured is not None:
            measured.start_source(replay_started_at)

        with ThreadPoolExecutor(
            max_workers= worker_count,
            thread_name_prefix= "replay"
        ) as executor:
            pending = {
                executor.submit(advance_replay,  steps): (source, steps)
                for source, steps in streams
            }
            while pending or ready:
                while ready and ready[0][0] <= time.monotonic_ns():
                    _,_, sources, steps = heapq.heappop(ready)

                    future = executor.submit(advance_replay, steps)
                    pending[future] = (sources, steps)

                producer.producer.poll(0)

                if not pending:
                    remaining = (ready[0][0] - time.monotonic_ns()) / 1_000_000_000
                    time.sleep(min(0.05, max(0.0, remaining)))
                    continue

                timeout = 0.05
                if ready:
                    remaining = (ready[0][0] - time.monotonic_ns()) / 1_000_000_000
                    timeout = min(timeout, max(0.0, remaining))
                done, _ =  wait(
                    pending, 
                    timeout= timeout,
                    return_when= FIRST_COMPLETED,
                )

                for future in done:
                    source, steps = pending.pop(future)

                    try:
                        due_mono_ns , result = future.result()
                    except Exception as exc:
                        logger.exception(
                            "Replay failed surgery_id=%s",
                            source.surgery_id,
                        )
                        worker_errors.append(f"surgery_id = {source.surgery_id}: {exc}")
                        continue
                    if due_mono_ns is None:
                        results.append(result)
                    else:
                        heapq.heappush(
                            ready,
                            (
                                due_mono_ns,
                                source.surgery_id,
                                source,
                                steps,
                            ),
                        )

        producer.flush()
        if measured is not None:
            measured.finish_source()
        observation_until = None
        if runner_metrics.enabled:
            runner_metrics.trace("source_completed", "run", at_ns=time.time_ns())
            run_dir= (Path(os.environ["METRICS_DIR"]) / runner_metrics.run_id)
            run_dir.mkdir(parents= True,  exist_ok= True)
            observation_until = None

            minimum_observation = max(VITAL_DEADLINE_S, ALERT_DEADLINE_S, CRITICAL_DETECTION_DEADLINE_S )
            observation_until = time.monotonic() + max(args.observe_seconds, minimum_observation)
            manifest = [key for result in results for key in result.db_keys]
            (run_dir / "db_keys.json").write_text(
                json.dumps(manifest, ensure_ascii= False, indent = 2),
                encoding = "utf-8",
            )

        if worker_errors:
            raise RuntimeError("Replay workers failed: "+ "; ".join(worker_errors))

        expected = {
            result.surgery_id: result.accepted for result in  results
        }

        if not any(expected.values()):
            raise RuntimeError ("No valid messages were enqueued to Kafka")

        actual = wait_for_database_results(
            engine = engine,
            expected = expected,
            replay_started_at = replay_started_at,
            timeout = args.verify_timeout,
        )

        if observation_until is not None:
            while True:
                remaining = observation_until - time.monotonic()

                if remaining <= 0:
                    break

                time.sleep(min(0.5, remaining))

        failed_cases = {
            surgery_id: {
                "expected": expected_count,
                "actual": actual.get(surgery_id, 0),
            }
            for surgery_id, expected_count in expected.items()
            if actual.get(surgery_id, 0 ) < expected_count
        }

        logger.info("REPLAY RESULTS")
        logger.info("-" * 72)

        for result in sorted(
            results, key = lambda item: item.surgery_id,
        ):
            logger.info(
                f"surgery={result.surgery_id:<4} "
                f"patient={result.patient_id:<6} "
                f"kafka={result.accepted:<6} "
                f"db={actual.get(result.surgery_id, 0):<6} "
                f"empty={result.skipped_empty:<6} "
                f"rejected={result.rejected:<4}"
            )

        if failed_cases:
            raise RuntimeError(
                f"CSV -> DB data is incomplete: {failed_cases}. "
                "Check consumer logs and the buffer flush interval."
            )

        logger.info(
            "E2E PASS: %d surgeries, %d total vital signs",
            len(results),
            sum(actual.values()),
        )

        return 0

    finally:
        if measured is not None:
            measured.finish_source()
        try:
            if producer is not None:
                producer.close()
        finally:
            try:
                if runner_metrics is not None:
                    try:
                        if runner_metrics.enabled:
                            runner_metrics.trace("observation_ended", "run", at_ns = time.time_ns())
                    finally:
                        runner_metrics.close()
            finally:
                engine.dispose()

def main(*, fault=False) -> int:
    """Chạy replay với metrics tùy chọn; fault cần controller riêng."""
    args = parse_args()
    if fault:
        args.fault = args.fault or "consumer_pause"
    if args.fault:
        if not bench_enabled:
            raise ValueError("Fault need BENCH_ENABLED = 1")
        if not args.realtime:
            raise ValueError("Faults over time require --realtime")
        if (
            not math.isfinite(args.fault_at)
            or args.fault_at < 0
            or not math.isfinite(args.fault_duration)
            or args.fault_duration <= 0
        ):
            raise ValueError("Invalid fault timing/duration")
        args.observe_seconds = max(args.observe_seconds, 60)

    if args.critical_probes and not bench_enabled:
        raise ValueError("Critical probes need BENCH_ENABLED = 1")
    if not bench_enabled:
        return run_replay(args)

    with MeasureRun(args) as measured:
        return run_replay(args, measured)


if __name__ == "__main__":
    raise SystemExit(main())
