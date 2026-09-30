import unittest 

from src.realtime_medical.config import AppConfig
from src.realtime_medical.triage.rules import RiskLevel, RuleEngine

class RuleEngineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = AppConfig.load_config("configs/config.yaml")
        cls.engine = RuleEngine(cls.config.triage_rules)

    @staticmethod
    def normal_vitals(**overrides):
        """
        Tạo một bản ghi sinh hiệu bình thường.

        overrides cho phép từng test chỉ thay đổi chỉ số cần kiểm tra.
        """
        vitals = {
            "hr": 75,
            "spo2": 98,
            "sbp": 120,
            "dbp": 80,
            "mbp": 90,
            "rr": 16,
            "bt": 37.0,
        }
        vitals.update(overrides)
        return vitals

    def assert_risk(self, vitals, expected_risk):
        risk, reasons = self.engine.evaluate(vitals)

        self.assertEqual(risk, expected_risk)
        return reasons

    def test_normal_vitals_returns_low_risk(self):
        risk, reasons  =self.engine.evaluate(self.normal_vitals())

        self.assertEqual(risk, RiskLevel.LOW.value)
        self.assertEqual(reasons, [])

    def test_empty_vitals_return_low_risk(self):
        risk,  reasons = self.engine.evaluate({})

        self.assertEqual(risk, RiskLevel.LOW.value)
        self.assertEqual(reasons, [])

    def test_missing_individual_fields_do_not_crash(self):
        vitals = self.normal_vitals()

        for field in list(vitals):
            with self.subTest(missing_feild = field):
                incomplete_vitals = vitals.copy()
                incomplete_vitals[field] = None

                risk, reasons = self.engine.evaluate(incomplete_vitals)

                self.assertEqual(risk, RiskLevel.LOW.value)
                self.assertEqual(reasons, [])

    def test_high_heart_rate_is_critical(self):
        threshold = self.config.triage_rules.heart_rate.high 

        reasons = self.assert_risk(
            self.normal_vitals(hr = threshold),
            RiskLevel.CRITICAL.value
        )

        self.assertTrue(any("heart rate" in reason.lower() for reason in reasons))

    def test_low_spo2_is_critical(self):
        threshold = self.config.triage_rules.spo2.critical 

        reasons = self.assert_risk(
            self.normal_vitals(spo2 = threshold),
            RiskLevel.CRITICAL.value
        )

        self.assertTrue(any("spo2" in reason.lower() for reason in reasons))

    def test_high_systolic_blood_pressure_is_critical(self):
        threshold = self.config.triage_rules.blood_pressure.sbp_high

        reasons = self.assert_risk(
            self.normal_vitals(sbp = threshold),
            RiskLevel.CRITICAL.value,
        )

        self.assertTrue(any("systolic" in reason.lower() for reason in reasons))

    def test_low_diastolic_blood_pressure_is_critical(self):
        threshold = self.config.triage_rules.blood_pressure.dbp_low

        reasons =  self.assert_risk(
            self.normal_vitals(dbp = threshold),
            RiskLevel.CRITICAL.value,
        )

        self.assertTrue(any("diastolic" in reason.lower() for reason in reasons))

    def test_low_mean_blood_presure_is_critical(self):
        threshold = self.config.triage_rules.blood_pressure.mbp_low

        reasons = self.assert_risk(
            self.normal_vitals(mbp = threshold),
            RiskLevel.CRITICAL.value,
        )

        self.assertTrue(any("mean blood pressure" in reason.lower() for reason in reasons))

    def test_high_respiratory_rate_is_critical(self):
        threshold = self.config.triage_rules.respiratory_rate.high

        reasons = self.assert_risk(
            self.normal_vitals(rr = threshold),
            RiskLevel.CRITICAL.value,
        )

        self.assertTrue(any("breathing" in reason.lower() for reason  in reasons))

    def test_low_respiratory_rate_is_critical(self):
        threshold = self.config.triage_rules.respiratory_rate.low

        reasons = self.assert_risk(
            self.normal_vitals(rr = threshold),
            RiskLevel.CRITICAL.value,
        )

        self.assertTrue(any("respiratory" in reason.lower() for reason in reasons))

    def test_high_temperature_is_critical(self):
        threshold = self.config.triage_rules.body_temperature.high

        reasons = self.assert_risk(
            self.normal_vitals(bt = threshold),
            RiskLevel.CRITICAL.value,
        )

        self.assertTrue(any("fever") in reason.lower() for reason in reasons)

    def test_low_temperature_is_medium(self):
        threshold = self.config.triage_rules.body_temperature.low

        reasons = self.assert_risk(
            self.normal_vitals(bt = threshold),
            RiskLevel.MEDIUM.value,
        )

        self.assertTrue(any("hypothermia" in reason.lower() for reason in reasons))

    def test_shock_index_above_threshold_is_critical(self):
        reasons = self.assert_risk(
            self.normal_vitals(hr = 110, sbp = 100),
            RiskLevel.CRITICAL.value,
        )

        self.assertTrue(any("shock index" in reason.lower() for reason in reasons))

    def test_shock_index_equal_threshold_is_not_triggered(self):
        risk, reasons = self.engine.evaluate(
            self.normal_vitals(hr = 90, sbp = 100)
        )

        self.assertEqual(risk, RiskLevel.LOW.value)
        self.assertEqual(reasons, [])

    def test_multiple_triggered_rules_return_highest_risk(self):
        vitals = self.normal_vitals(
            hr=160,
            spo2=85,
            sbp=200,
            rr=30,
            bt=34,
        )

        risk, reasons  =self.engine.evaluate(vitals)

        self.assertEqual(risk, RiskLevel.CRITICAL.value)
        self.assertGreaterEqual(len(reasons), 4)
        self.assertTrue(all(isinstance(reason, str) for reason in reasons))

if __name__ == "__main__":
    unittest.main()