import json
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd

from src.realtime_medical.config import AppConfig, BASE_DIR
from src.realtime_medical.triage.trigger_model import MLTriageModel


class ModelInferenceTests(unittest.TestCase):
    def test_saved_bundle_controls_feature_order(self):
        config = AppConfig.load_config()
        model = MLTriageModel(config.model)
        model.model["classifier"].n_jobs = 1
        row = pd.read_parquet(BASE_DIR / "data/processed/dataset.parquet").iloc[0]

        expected = model.model.predict_proba(
            pd.DataFrame([row[model.input_features]])
        )[0][1]
        actual = model.predict_risk(row.to_dict())

        self.assertEqual(len(model.input_features), 24)
        self.assertAlmostEqual(actual, expected)

    def test_consumer_scores_window_features_and_keeps_immediate_rules(self):
        from src.realtime_medical.streaming.consumer import VitalsConsumer

        class Message:
            def __init__(self, payload):
                self.payload = payload

            def error(self):
                return None

            def value(self):
                return json.dumps(self.payload).encode("utf-8")

        class Kafka:
            def __init__(self, _):
                start = datetime(2026, 9, 22, 10, 0, 0)
                self.messages = [
                    Message({
                        "surgery_id": 1,
                        "patient_id": "1",
                        "recorded_at": (start + timedelta(seconds=i)).isoformat(),
                        "hr": 160 if i == 0 else 75,
                        "spo2": 98,
                        "sbp": 120,
                        "dbp": 80,
                        "mbp": 90,
                        "rr": 16,
                        "bt": 37,
                    })
                    for i in range(6)
                ]

            def subscribe(self, _):
                pass

            def poll(self, timeout):
                if self.messages:
                    return self.messages.pop(0)
                raise KeyboardInterrupt

            def close(self):
                pass

        class Database:
            def __init__(self, _):
                self.vitals = []
                self.alerts = []

            def insert_vitals_batch(self, rows):
                self.vitals.extend(rows)

            def insert_alerts_batch(self, rows):
                self.alerts.extend(rows)

            def close(self):
                pass

        class StaticFeatures:
            def __init__(self, _):
                pass

            def get(self, surgery_id):
                return {"age": 60, "asa": 2, "bmi": 24, "sex_M": 1, "emop": 0}

        class Model:
            def __init__(self, _):
                self.received = []

            def predict_risk(self, features):
                self.received.append(features)
                return 0.8

            def is_alert_required(self, score):
                return score >= 0.35

        with (
            patch("src.realtime_medical.streaming.consumer.Consumer", Kafka),
            patch("src.realtime_medical.streaming.consumer.DataBaseClient", Database),
            patch("src.realtime_medical.streaming.consumer.StaticFeatureStore", StaticFeatures),
            patch("src.realtime_medical.streaming.consumer.MLTriageModel", Model),
        ):
            consumer = VitalsConsumer(AppConfig.load_config())
            consumer.start_listening()

        self.assertEqual(len(consumer.db_client.vitals), 6)
        self.assertEqual(len(consumer.ml_model.received), 1)
        self.assertIn("hr_mean", consumer.ml_model.received[0])
        self.assertEqual(consumer.ml_model.received[0]["age"], 60)
        self.assertEqual(len(consumer.db_client.alerts), 2)
        self.assertEqual(consumer.db_client.alerts[0]["risk_level"], "CRITICAL")
        self.assertIsNone(consumer.db_client.alerts[0]["model_score"])
        self.assertEqual(consumer.db_client.alerts[1]["risk_level"], "MEDIUM")
        self.assertEqual(consumer.db_client.alerts[1]["model_score"], 0.8)


if __name__ == "__main__":
    unittest.main()
