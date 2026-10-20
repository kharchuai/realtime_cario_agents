"""Quản lý một lượt đo; được gọi bởi run_pipeline.py."""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
import uuid
from datetime import  datetime
from pathlib import Path
from src.realtime_medical.utils.benchmark_hooks  import make_metrics
from src.realtime_medical.utils.fault_injection import pause_fault

ROOT = Path(__file__).resolve().parents[3]
logger = logging.getLogger(__name__)

class MeasureRun:
    def __init__(self, args):
        self.args =  args
        scenario = args.fault or "baseline"

        self.run_id = (
            f"{scenario}_{datetime.now():%Y%m%d_%H%M%S}_"
            f"{uuid.uuid4().hex[:8]}"
        )
        self.metrics_dir = ROOT / "tmp" /  "benchmark"
        self.run_dir = self.metrics_dir / self.run_id
        self.report_dir = ROOT / "logs" / self.run_id
        self.cancel =  threading.Event()
        self.thread = None
        self.metrics =  None
        self.consumer_requested = False
        self.source_started = False
        self.fault_id =  None
        self.issues =  []

    def _docker(self, *args):
        result = subprocess.run(
            ["docker", *args],
            cwd = ROOT,
            capture_output= True,
            text = True,
            encoding= "utf-8",
            errors= "replace",
            timeout= 120,
        )
        if result.returncode:
            raise RuntimeError(
                result.stderr.strip() or "Docker command failed"
            )

    def _wait_consumer(self):
        deadline = time.monotonic() + 120

        while time.monotonic() < deadline:
            for path in self.run_dir.glob("consumer-*.jsonl"):
                with path.open(encoding = "utf-8") as source:
                    for line in source:
                        if not line.endswith("\n"):
                            continue

                        row = json.loads(line)
                        if (
                            row.get("run_id") == self.run_id
                            and row.get("name") == "consumer_assigned"
                        ):
                            return
            time.sleep(0.25)
        raise TimeoutError("Consumer has not been assigned a partition; no data source has been initiated.")

    def __enter__(self):
        os.environ.update({
            "BENCH_ENABLED": "1",
            "BENCH_RUN_ID": self.run_id,
            "METRICS_DIR": str(self.metrics_dir),
            "BENCH_CASES": ",".join(map(str, self.args.cases)),
            "BENCH_CRITICAL_PROBES": (
                "1" if self.args.critical_probes else "0"
            ),
            "BENCH_ALERT_ACK_LOSS": "0",
            "BENCH_DROP_FIELD": "",
            "BENCH_SCHEDULED_REPLAY": "0",
            "ML_INFERENCE_N_JOBS": "1",
        })

        self.run_dir.mkdir(parents = True, exist_ok = True )

        try:
            self.metrics = make_metrics("controller")
            self.consumer_requested = True

            self._docker("compose", "up", "-d", "--no-deps","--force-recreate", "consumer",)
            self._wait_consumer()
            logger.info("RUN_ID = %s", self.run_id)
            return self
        except BaseException as error:
            self.__exit__(type(error), error, error.__traceback__)
            raise

    def start_source(self, base_time):
        self.source_started = True

        if not self.args.fault:
            return

        delay  = max(
            0.0,
            base_time.timestamp() + self.args.fault_at - time.time()
        )

        self.thread = threading.Thread(
            target= self._apply_fault,
            args = (delay,),
            name = "consummer-pause"
        )
        self.thread.start()

    def _apply_fault(self, delay):
        if self.cancel.wait(delay):
            return
        try:
            self.fault_id = pause_fault(
                self.metrics,
                "medical_consumer",
                duration_s= self.args.fault_duration,
            )
        except BaseException as error:
            self.issues.append(f"Fault controller: {error}")

    def finish_source(self):
        self.cancel.set()

        if self.thread is not None:
            self.thread.join()
    def _write_report(self, error):
        command = [
            sys.executable,
            "-B",
            str(ROOT / "scripts"/ "analyze_benchmark.py"),
            "--run-id", self.run_id,
            "--metrics-dir", str(self.metrics_dir)
        ]

        if (self.run_dir / "db_keys.json").is_file():
            command.append("--check-db")
        try:
            result = subprocess.run(
                command,
                cwd = ROOT,
                capture_output= True,
                text = True,
                encoding= "utf-8",
                env =  {
                    **os.environ,
                    "PYTHONIOENCODING":"utf-8",
                },
                timeout= 300,
            )

            if result.returncode:
                raise RuntimeError(result.stderr.strip() or "Analyzer failed")
            report = json.loads(result.stdout)

            if report.get("run_id") != self.run_id:
                raise ValueError("Analyzer returned a different run ID")
        except Exception as analysis_error:
            self.issues.append(f"Analysis: {analysis_error}")
            report = {
                "run_id": self.run_id,
                "metrics_status": "analysis_failed",
            }

        report["execution"] = {
            "scenario": self.args.fault or "baseline",
            "source_mode": ("critical_probes" if self.args.critical_probes else "csv"),
            "cases": self.args.cases,
            "fault_at_s":(self.args.fault_at if self.args.fault else None),
            "fault_duration_s": (self.args.fault_duration if self.args.fault else None),
            "status": ("failed" if error is not None or self.issues else "completed"),
            "error": str(error) if error is not None else None,
            "issues": list(self.issues),
        }

        report.setdefault("M6", {"status": "not_checked"})
        report.setdefault("M10", {"status": "steady_interval_not_selected"})
        self.report_dir.mkdir(parents=True, exist_ok=True)
        output = self.report_dir / "report.json"
        temporary = self.report_dir / ".report.tmp"

        try:
            temporary.write_text(
                json.dumps(report, ensure_ascii= False, indent=2),
                encoding = "utf-8",
            )
            temporary.replace(output)
        finally:
            temporary.unlink(missing_ok  = True)
        logger.info("REPORT = %s", output)

    def __exit__(self, exc_type, error, traceback):
        self.finish_source()

        if (
            self.args.fault
            and self.source_started
            and self.fault_id is None
        ):
            self.issues.append("Fault not yet complete; check the length of the source and the controller")

        if self.consumer_requested:
            try:
                self._docker("compose", "stop", "-t", "60", "consumer")
            except Exception as stop_error:
                self.issues.append(f"Consumer stop: {stop_error}")

        if self.metrics is not None:
            try:
                self.metrics.close()
            except Exception as close_error:
                self.issues.append(f"Collector close: {close_error}")

        self._write_report(error)

        if error is None and self.issues:
            raise RuntimeError("Measurement encountered an error; see execution in report.json")

        return False
