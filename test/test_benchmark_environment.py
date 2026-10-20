"""Run configuration and reference labels without Kafka/PostgreSQL services."""

import io
import os
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts import run_pipeline
from src.realtime_medical.config import AppConfig, BASE_DIR
from src.realtime_medical.utils.benchmark_hooks import make_metrics


class BenchmarkEnvironmentTests(unittest.TestCase):
    def test_metrics_off_needs_no_run_configuration(self):
        with patch.dict(os.environ, {}, clear=True):
            metrics = make_metrics("runner")
            try:
                self.assertFalse(metrics.enabled)
                self.assertIsNone(metrics.path)
                self.assertNotIn("METRICS_DIR", os.environ)
            finally:
                metrics.close()

    def test_enabled_metrics_require_shared_run_id(self):
        for run_id in (None, "", " "):
            environment = {"BENCH_ENABLED": "1"}
            if run_id is not None:
                environment["BENCH_RUN_ID"] = run_id
            with self.subTest(run_id=run_id), patch.dict(os.environ, environment, clear=True):
                with self.assertRaisesRegex(ValueError, "run_pipeline.py"):
                    make_metrics("runner")

    def test_default_directory_is_shared_by_collectors(self):
        environment = {"BENCH_ENABLED": "1", "BENCH_RUN_ID": "shared-run"}
        with patch.dict(os.environ, environment, clear=True), patch(
            "src.realtime_medical.utils.benchmark_hooks.AuditedMetricsRecorder"
        ) as recorder:
            make_metrics("producer")
            make_metrics("runner")
            for call in recorder.call_args_list:
                self.assertEqual(call.kwargs["run_id"], "shared-run")
                self.assertEqual(Path(call.kwargs["output_dir"]), BASE_DIR / "tmp" / "benchmark")
            # run_replay uses the same environment for its DB manifest.
            self.assertEqual(Path(os.environ["METRICS_DIR"]), BASE_DIR / "tmp" / "benchmark")

    def test_container_directory_is_preserved(self):
        environment = {
            "BENCH_ENABLED": "1",
            "BENCH_RUN_ID": "shared-run",
            "METRICS_DIR": "/app/tmp/benchmark",
        }
        with patch.dict(os.environ, environment, clear=True), patch(
            "src.realtime_medical.utils.benchmark_hooks.AuditedMetricsRecorder"
        ) as recorder:
            make_metrics("consumer")
            self.assertEqual(recorder.call_args.kwargs["output_dir"], "/app/tmp/benchmark")

    def test_csv_reference_labels_are_automatic_and_preserve_readings(self):
        csv = (
            "time_sec,Solar8000/HR,Solar8000/PLETH_SPO2\n"
            "0,80,98\n"
            "1,80,85\n"
        )
        read_csv = run_pipeline.pd.read_csv
        config = AppConfig.load_config()
        for enabled in (False, True):
            with self.subTest(metrics=enabled):
                metrics = SimpleNamespace(enabled=enabled, run_id="shared-run", trace=Mock())
                producer = SimpleNamespace(send_vital_sign=Mock(return_value=True))
                start = threading.Event()
                start.set()
                with patch.dict(os.environ, {}, clear=True), patch.object(
                    run_pipeline.pd, "read_csv",
                    side_effect=lambda *args, **kwargs: read_csv(io.StringIO(csv), **kwargs),
                ):
                    result = run_pipeline.replay_one_source(
                        run_pipeline.ReplaySource(1, 1, Path("case_1_numerics.csv")),
                        config, producer, datetime.now(timezone.utc), start, 2, 0,
                        runner_metrics=metrics,
                    )
                self.assertEqual(result.accepted, 2)
                payloads = [call.kwargs["vital_data"] for call in producer.send_vital_sign.call_args_list]
                self.assertEqual([payload["spo2"] for payload in payloads], [98, 85])
                self.assertEqual(len(result.db_keys), 2 if enabled else 0)
                for payload in payloads:
                    self.assertEqual("_bench" in payload, enabled)
                vital_traces = [call for call in metrics.trace.call_args_list if call.args[0] == "expected_vital"]
                alert_traces = [call for call in metrics.trace.call_args_list if call.args[0] == "expected_alert"]
                if enabled:
                    self.assertEqual([call.kwargs["critical"] for call in vital_traces], [False, True])
                    self.assertEqual(len(alert_traces), 1)
                else:
                    metrics.trace.assert_not_called()


if __name__ == "__main__":
    unittest.main()
