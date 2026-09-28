from __future__ import annotations

import argparse
import logging
import sys
import threading
import time

from concurrent.futures  import ThreadPoolExecutor, as_completed
from  dataclasses import dataclass
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

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s %(levelname)s %(threadName)s - %(message)s",
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

def parse_args() ->  argparse.Namespace:
    parser = argparse.ArgumentParser(
        description = (
            "Replay đồng thời nhiều file case_<id>_numerics.csv "
            "vào Kafka và kiểm tra dữ liệu trong PostgreSQL."
        )
    )

    parser.add_argument(
        "--cases",
        type = int,
        nargs= "+",
        required =  True,
        help = "Danh sách case/surgery ID, ví dụ: --cases 1 2 3 4 5",
    )

    parser.add_argument(
        "--data-dir",
        type= Path,
        default = PROJECT_ROOT/ "data"/ "streaming",
        help =  "Thư mục chứa case_<id>_numerics.csv"
    )

    parser.add_argument(
        "--workers",
        type = int,
        default = 0,
        help =(
            "Số file replay đồng thời. "
            "Mặc định bằng số case nhưng tối đa 10."
        ),
    )

    parser.add_argument(
        "--max-rows",
        type = int,
        default = 200,
        help = "Số dòng tối đa mỗi CSV. Dùng 0 để chạy toàn bộ file."
    )

    parser.add_argument(
        "--delay",
        type = float,
        default = 0,
        help = "Thời gian chờ giữa hai message, tính bằng giây.",
    )

    parser.add_argument(
        "--verify-timeout",
        type = float,
        default = 120,
        help = "Thời gian tối đa chờ consumer ghi dữ liệu xuống DB.",
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
        f"PostgreSQL chưa sẵn sàng sau {timeout} giây: {last_error}"
    )

def check_kafka_topic(config:  AppConfig, timeout: float = 15) -> None:
    admin = AdminClient(
        {"bootstrap.servers": config.kafka_broker_url}
    )

    metadata = admin.list_topics(timeout=timeout)
    topic_metadata = metadata.topics.get(config.kafka_topic_vitals)

    if topic_metadata is None:
        raise RuntimeError(
            f"Kafka topic {config.kafka_topic_vitals!r} chưa tồn tại"
        )

    logger.info(
        "Kafka topic %s sẵn sàng với %d partition",
        config.kafka_topic_vitals,
        len(topic_metadata.partitions),
    )

def build_source(
    engine,
    case_ids: list[int],
    data_dir: Path,
) -> list[ReplaySource]:
    if len(case_ids) != len(set(case_ids)):
        raise ValueError("Danh sách --cases có surgery_id bị trùng")

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
                    f"Không tìm thấy surgery_id={surgery_id} trong DB. "
                    "Hãy seed patient/surgery trước."
                )

            csv_path = (
                data_dir / f"case_{surgery_id}_numerics.csv"
            ).resolve()

            if not csv_path.is_file():
                raise FileNotFoundError(
                    f"Không tìm thấy CSV cho surgery_id={surgery_id}: "
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
    start_event: threading.Event,
    max_rows: int,
    delay: float,
) -> ReplayResult:
    result = ReplayResult(
        surgery_id = sources.surgery_id,
        patient_id = sources.patient_id,
        csv_path =sources.csv_path,
    )

    logger.info(
        "Chuẩn bị surgery=%s patient=%s file=%s",
        sources.surgery_id,
        sources.patient_id,
        sources.csv_path.name,
    )

    start_event.wait()
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

            accepted = producer.send_vital_sign(
                surgery_id = sources.surgery_id,
                patient_id = str(sources.patient_id),
                vital_data = vitals,
            )


            if accepted:
                result.accepted += 1
            else:
                result.rejected += 1

            if result.accepted % 1000 == 0 and result.accepted > 0:
                logger.info("surgery=%s đã enqueue %d message",sources.surgery_id,result.accepted)

            if delay > 0:
                time.sleep(delay)

    logger.info(
        "Hoàn thành producer surgery=%s: "
        "accepted=%d empty=%d duplicate_time=%d rejected=%d",
        sources.surgery_id,
        result.accepted,
        result.skipped_empty,
        result.skipped_duplicate_timestamp,
        result.rejected,
    )

    return result

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
                "replay_started_at": replay_started_at,
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


def main() -> int:
    args = parse_args()

    if args.delay < 0:
        raise ValueError("--delay must > 0 ")


    config = AppConfig.load_config()
    engine = create_engine(
        config.postgres_dsn,
        pool_pre_ping = True,
    )

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
            "Bắt đầu replay %d ca mổ bằng %d worker",
            len(sources),
            worker_count,
        )

        # Schema hiện tại dùng TIMESTAMP không timezone,
        # do đó chuyển UTC về dạng naive để so sánh DB ổn định.

        replay_started_at = datetime.now(
            timezone.utc
        ).replace(tzinfo=None)

        start_event = threading.Event()

        # confluent_kafka.Producer hỗ trợ gọi từ nhiều thread.
        # Dùng chung một producer giúp tránh mở một kết nối Kafka/file.
        producer = VitalsProducer(config)

        results: list[ReplayResult] = []
        worker_errors: list[str] = []

        with ThreadPoolExecutor(
            max_workers = worker_count,
            thread_name_prefix = "replay",
        ) as executor:
            futures = {
                executor.submit(
                    replay_one_source,
                    source,
                    config,
                    producer,
                    replay_started_at,
                    start_event,
                    args.max_rows,
                    args.delay,
                ): source
                for source in sources
            }

            start_event.set()

            for future in  as_completed(futures):
                source = futures[future]

                try:
                    results.append(future.result())
                except Exception as exc:
                    logger.exception("Replay fail surgery_id = %s", source.surgery_id,)
                    worker_errors.append(f"surgery_id = {source.surgery_id}: {exc}")

        producer.flush()

        if worker_errors:
            raise RuntimeError("Có worker replay thất bại: "+ "; ".join(worker_errors))

        expected = {
            result.surgery_id: result.accepted for result in  results
        }

        if not any(expected.values()):
            raise RuntimeError ("Không có message hợp lệ nào được enqueue vào Kafka")

        actual = wait_for_database_results(
            engine = engine,
            expected = expected,
            replay_started_at = replay_started_at,
            timeout = args.verify_timeout,
        )

        failed_cases = {
            surgery_id: {
                "expected": expected_count,
                "actual": actual.get(surgery_id, 0),
            }
            for surgery_id, expected_count in expected.items()
            if actual.get(surgery_id, 0 ) < expected_count
        }

        print("\nKẾT QUẢ REPLAY")
        print("-" * 72)

        for result in sorted(
            results, key = lambda item: item.surgery_id,
        ):
            print(
                f"surgery={result.surgery_id:<4} "
                f"patient={result.patient_id:<6} "
                f"kafka={result.accepted:<6} "
                f"db={actual.get(result.surgery_id, 0):<6} "
                f"empty={result.skipped_empty:<6} "
                f"rejected={result.rejected:<4}"
            )

        if failed_cases:
            raise RuntimeError(
                f"CSV -> DB chưa đầy đủ: {failed_cases}. "
                "Kiểm tra log consumer và thời gian flush buffer."
            )

        logger.info(
            "E2E PASS: %d ca mổ, tổng %d vital signs",
            len(results),
            sum(actual.values()),
        )

        return 0

    finally:
        engine.dispose()

if __name__ == "__main__":
    raise SystemExit(main())
