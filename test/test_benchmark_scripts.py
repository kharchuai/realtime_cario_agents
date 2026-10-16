"""Kiểm tra tổng hợp số liệu và điều khiển suite, không chạy Docker."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts import run_benchmark_suite as suite
from scripts import summarize_benchmark as summary
from scripts import review_benchmark as review


class BenchmarkScriptTests(unittest.TestCase):
    def test_signal_without_valid_samples_stays_unknown(self):
        report = {"M2": {"streams": [{
            "field": "rr", "surgery_id": 15, "active_samples": 10,
            "active_never_seen_samples": 10, "active_receive_gap_over_limit_samples": 0,
            "max_active_signal_age_s": None, "max_active_receive_gap_s": None,
        }]}}
        field = summary.signal_summary(report)["rr"]
        self.assertIsNone(field["max_age_s"])
        self.assertIsNone(field["max_gap_s"])
        self.assertEqual(field["never_seen_cases"], [15])

    def test_throughput_uses_completed_count_and_elapsed_time(self):
        report = {"observed_end_ns": 100, "M10": {
            "Name": "Sustained Throughput", "vitals": [
                {"start_ns": 0, "end_ns": 10_000_000_000, "unique_completed": 200, "throughput_per_s": 20},
                {"start_ns": 10_000_000_000, "end_ns": 15_000_000_000, "unique_completed": 200, "throughput_per_s": 40},
            ], "alerts": [],
        }}
        result = summary.summarize_run({}, report)["metrics"]["M10"]
        self.assertAlmostEqual(result["vitals"]["mean_per_s"], 400 / 15)
        self.assertIsNone(result["alerts"]["mean_per_s"])

    def test_faults_and_censored_results_are_preserved(self):
        faults = [{"fault_id": "pause1", "M8": {"vitals": {
            "status": "not_recovered_within_observation", "observed_for_s": 60}}},
            {"fault_id": "pause2", "status": "fault_not_restored_within_observation"}]
        report = {"observed_end_ns": 100, "M7_M8_M9_by_fault": faults}
        result = summary.summarize_run({}, report)
        self.assertEqual(result["metrics"]["M7_M8_M9_by_fault"], faults)
        self.assertEqual(summary.fmt(None), "N/A")

    def test_normal_environment_clears_all_faults_without_mutating_input(self):
        original = {"BENCH_ENABLED": "1", "BENCH_RUN_ID": "old", "BENCH_DROP_FIELD": "rr",
                    "BENCH_ALERT_ACK_LOSS": "1", "METRICS_DIR": "old", "KAFKA_TOPIC_VITALS": "custom"}
        result = suite.normal_environment(original)
        self.assertEqual(result["BENCH_ENABLED"], "0")
        self.assertEqual(result["BENCH_ALERT_ACK_LOSS"], "0")
        self.assertNotIn("BENCH_DROP_FIELD", result)
        self.assertNotIn("METRICS_DIR", result)
        self.assertEqual(result["KAFKA_TOPIC_VITALS"], "custom")
        self.assertEqual(original["BENCH_ENABLED"], "1")

    def test_first_source_time_handles_partially_written_json(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as temp:
            directory = Path(temp)
            (directory / "runner-a.jsonl").write_text(
                json.dumps({"name": "expected_vital", "details": {"t0_ns": 200}}) + "\n" + '{"name":',
                encoding="utf-8")
            (directory / "runner-b.jsonl").write_text(
                json.dumps({"name": "expected_vital", "details": {"t0_ns": 100}}) + "\n", encoding="utf-8")
            self.assertEqual(suite.first_source_time(directory), 100)

    def test_rr_check_keeps_missing_values_and_counts_dropped_readings(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as temp:
            (Path(temp) / "case_1_numerics.csv").write_text(
                "time_sec,Solar8000/RR_CO2\n28,16\n30,6\n40,8\n42,\n", encoding="utf-8")
            metadata = {"source_mode": "replay", "source_data_dir": temp,
                        "drop_field": "rr", "gap_seconds": 10}
            provenance = {"mapping": {"Solar8000/RR_CO2": "rr"}}
            rows = [{"persisted": True, "expected_surgery_id": 1, "time_sec": tick, "rr": value}
                    for tick, value in [(28, 16), (30, None), (40, 8), (42, None)]]
            result = review.check_rr(metadata, provenance, rows)
            self.assertEqual(result["compared_rows"], 4)
            self.assertEqual(result["mismatches"], 0)
            rows[0]["rr"] = 99
            self.assertEqual(review.check_rr(metadata, provenance, rows)["mismatches"], 1)

    def test_pause_uses_own_recorder_and_leaves_environment_unchanged(self):
        report = {"M3": {stream: {"on_time_rate": 1.0} for stream in ("vitals", "alerts")},
                  "M6": {"duplicate_excess": 0}, "telemetry_issues": []}
        runner = Mock()
        runner.wait.return_value = 0
        runner.poll.return_value = 0
        expectations = {str(t): {"t0_ns": t * 1_000_000_000} for t in (0, 50, 99)}
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as temp:
            with patch.object(suite, "RESULTS", Path(temp)), \
                 patch.object(suite, "command", return_value=json.dumps(report)) as command, \
                 patch.object(suite, "wait_assigned"), \
                 patch.object(suite, "first_source_time", return_value=0), \
                 patch.object(suite, "load_run", return_value={"expected_vitals": expectations}), \
                 patch.object(suite.subprocess, "Popen", return_value=runner), \
                 patch.object(suite.time, "sleep"), \
                 patch.object(suite, "pause_fault") as pause:
                before = dict(suite.os.environ)
                result = suite.run_scenario("consumer_pause", "test", {}, [1], Path(temp), 100, pause_seconds=5)
                self.assertEqual(dict(suite.os.environ), before)
                self.assertEqual(pause.call_args.args[2], 5)
                self.assertEqual(pause.call_args.args[0].run_id, "test_consumer_pause")
                self.assertEqual(result["steady_end_ns"], 90_000_000_000)
                self.assertTrue(any("stop" in call.args[0] for call in command.call_args_list))


if __name__ == "__main__":
    unittest.main()
