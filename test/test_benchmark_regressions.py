"""Regression cho deadline/completeness và recovery trên nhiều bệnh nhân."""
import unittest

from src.realtime_medical.utils.benchmark_analysis import (
    backlog_drain, deadline_report, field_completeness, observed_rto,
)


class BenchmarkRegressionTests(unittest.TestCase):
    def test_reference_labels_cover_missing_bp_and_rr_boundaries(self):
        from src.realtime_medical.config import AppConfig
        from src.realtime_medical.utils.benchmark_reference import reference_critical
        config = AppConfig.load_config()
        normal = {"hr":80,"spo2":98,"sbp":120,"dbp":75,"mbp":90,"rr":16,"bt":37.0}
        self.assertFalse(reference_critical(normal,config))
        self.assertTrue(reference_critical({**normal,"rr":config.triage_rules.respiratory_rate.high},config))
        self.assertTrue(reference_critical({**normal,"spo2":85},config))
        self.assertFalse(reference_critical({"hr":80,"spo2":98,"rr":16,"bt":37},config))
        self.assertFalse(reference_critical({**normal,"bt":34},config))
        self.assertTrue(reference_critical({**normal,"hr":115,"sbp":100},config))

    def test_recovery_needs_five_rounds_not_five_patients(self):
        samples = [(10, False)] * 40 + [(20, True)] * 40
        self.assertEqual(observed_rto(samples, 0)["status"], "not_recovered_within_observation")
        samples += [(t, True) for t in (30, 40, 50, 60) for _ in range(40)]
        report = observed_rto(samples, 0)
        self.assertEqual(report["status"], "recovered")
        self.assertEqual(report["recovery_candidate_s"], 20 / 1e9)
        self.assertEqual(report["recovery_confirmed_s"], 60 / 1e9)

    def test_one_failed_patient_breaks_recovery_round(self):
        samples = [(10, False)] + [(20, True)] * 40 + [(30, True)] * 39 + [(30, False)]
        samples += [(t, True) for t in (40, 50, 60, 70) for _ in range(40)]
        self.assertEqual(observed_rto(samples, 0)["status"], "not_recovered_within_observation")

    def test_no_outage_is_not_zero_rto(self):
        self.assertEqual(observed_rto([(t, True) for t in range(10)], 0)["status"], "no_outage_observed")

    def test_deadline_late_missing_and_immature_are_distinct(self):
        expected = {k: {"deadline_ns": d} for k, d in [("ok", 10), ("late", 10), ("lost", 10), ("immature", 30)]}
        result = deadline_report(expected, "sink", 20, {("sink", "ok"): 9, ("sink", "late"): 15})
        self.assertEqual((result["on_time"], result["late"], result["not_observed"], result["deadline_not_reached"]), (1, 1, 1, 1))
        self.assertEqual(result["eligible"], 3)
        self.assertEqual(result["missing_at_deadline"], 2)

    def test_raw_missing_field_and_late_reading_reduce_completeness(self):
        expected = {rid: {"deadline_ns": 10, "required_fields": ["hr", "spo2"]} for rid in ("partial", "late")}
        first = {("vitals_persisted", "partial"): 9, ("vitals_persisted", "late"): 15}
        details = {key: {"valid_fields": ["spo2"] if key[1] == "partial" else ["hr", "spo2"]} for key in first}
        result = field_completeness(expected, 20, first, details)
        self.assertEqual(result["completeness_rate"], 0.25)

    def test_incomplete_backlog_is_censored(self):
        expected = {"done": {"t0_ns": 1}, "missing": {"t0_ns": 2}, "new": {"t0_ns": 20}}
        result = backlog_drain(expected, "sink", 10, 30, {("sink", "done"): 12})
        self.assertEqual((result["backlog"], result["remaining"]), (2, 1))
        self.assertIsNone(result["drain_time_s"])


if __name__ == "__main__":
    unittest.main()
