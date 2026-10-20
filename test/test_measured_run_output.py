"""Regression tests for report placement without Docker or database services."""

import json
import shutil
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.realtime_medical.utils import measured_run


class MeasuredRunOutputTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.gettempdir()) / f"medical-report-test-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(self.clean_directory)
        root_patch = patch.object(measured_run, "ROOT", self.root)
        root_patch.start()
        self.addCleanup(root_patch.stop)

    def clean_directory(self):
        if self.root.resolve().parent != Path(tempfile.gettempdir()).resolve():
            raise ValueError("Refusing to remove a directory outside the test temporary root")
        shutil.rmtree(self.root)

    def make_run(self, fault=None):
        return measured_run.MeasureRun(SimpleNamespace(
            cases=[1, 2, 3], fault=fault, critical_probes=bool(fault),
            fault_at=300, fault_duration=20,
        ))

    def test_baseline_and_fault_reports_use_logs_and_keep_raw_traces(self):
        for fault in (None, "consumer_pause"):
            with self.subTest(fault=fault):
                run = self.make_run(fault)
                run.run_dir.mkdir(parents=True)
                trace = run.run_dir / "controller.jsonl"
                trace.write_text("raw trace\n", encoding="utf-8")
                (run.run_dir / "db_keys.json").write_text("[]", encoding="utf-8")
                result = SimpleNamespace(returncode=0, stdout=json.dumps({
                    "run_id": run.run_id, "M7_M8_M9_by_fault": [],
                }))
                with patch.object(measured_run.subprocess, "run", return_value=result) as analyze:
                    with self.assertLogs(measured_run.logger, level="INFO") as logged:
                        run._write_report(None)

                output = self.root / "logs" / run.run_id / "report.json"
                report = json.loads(output.read_text(encoding="utf-8"))
                self.assertEqual(report["execution"]["scenario"], fault or "baseline")
                self.assertEqual(report["execution"]["status"], "completed")
                self.assertEqual(trace.read_text(encoding="utf-8"), "raw trace\n")
                self.assertFalse((run.run_dir / "report.json").exists())
                self.assertFalse((output.parent / ".report.tmp").exists())
                self.assertFalse((self.root / "test" / "benchmark_results").exists())
                self.assertIn(str(output), logged.output[0])
                command = analyze.call_args.args[0]
                self.assertEqual(command[command.index("--metrics-dir") + 1], str(run.metrics_dir))
                self.assertIn("--check-db", command)

    def test_analyzer_failure_still_writes_failed_report_in_logs(self):
        run = self.make_run("consumer_pause")
        with patch.object(measured_run.subprocess, "run", side_effect=RuntimeError("analysis unavailable")):
            run._write_report(ValueError("replay interrupted"))

        report = json.loads((run.report_dir / "report.json").read_text(encoding="utf-8"))
        self.assertEqual(report["metrics_status"], "analysis_failed")
        self.assertEqual(report["execution"]["status"], "failed")
        self.assertEqual(report["execution"]["error"], "replay interrupted")
        self.assertIn("analysis unavailable", report["execution"]["issues"][0])

    def test_failed_atomic_replace_preserves_existing_report_and_cleans_temporary_file(self):
        run = self.make_run()
        run.report_dir.mkdir(parents=True)
        output = run.report_dir / "report.json"
        output.write_text('{"previous": true}', encoding="utf-8")
        result = SimpleNamespace(returncode=0, stdout=json.dumps({"run_id": run.run_id}))
        with patch.object(measured_run.subprocess, "run", return_value=result):
            with patch.object(Path, "replace", side_effect=OSError("cannot replace")):
                with self.assertRaisesRegex(OSError, "cannot replace"):
                    run._write_report(None)

        self.assertEqual(json.loads(output.read_text(encoding="utf-8")), {"previous": True})
        self.assertFalse((run.report_dir / ".report.tmp").exists())


if __name__ == "__main__":
    unittest.main()
