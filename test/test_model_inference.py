import json 
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import joblib
import pandas as pd

from src.realtime_medical.config import AppConfig, BASE_DIR
from src.realtime_medical.triage.trigger_model import MLTriageModel

class FakePipeline:
    """
    Model giả dùng để kiểm tra cách MLTriageModel chuẩn bị dữ liệu
    mà không cần gọi model thật.
    """
    def __init__(self, feature_names, probability = 0.8):
        self.feature_names_in_ = feature_names
        self.probability = probability
        self.received_frame = None

    def set_params(self, **params):
        self.params = params
        return self

    def predict_proba(self, frame):
        self.received_frame = frame.copy()

        return [[1.0 - self.probability, self.probability]]

class ModelInferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = AppConfig.load_config("configs/config.yaml")

    def test_real_model_bunfle_load_successfully(self):
        model = MLTriageModel(self.config.model)

        self.assertIsNotNone(model.model)
        self.assertIsInstance(model.input_features, (list, tuple))
        self.assertGreater(len(model.input_features), 0)
        self.assertEqual(
            len(model.input_features),
            len(set(model.input_features)),
        )

    def test_saved_bundle_contains_matching_feature_schema_and_horizon(self):
        bundle_path = BASE_DIR / self.config.model.path

        saved_bundle = joblib.load(bundle_path)

        self.assertIn("pipeline", saved_bundle)
        self.assertIn("feature_order", saved_bundle)

        self.assertEqual(
            list(saved_bundle["pipeline"].feature_names_in_),
            list(saved_bundle["feature_order"]),
        )
        self.assertEqual(
            saved_bundle["metadata"]["prediction_horizon_seconds"],
            self.config.model.prediction_horizon_seconds,
        )

    def test_real_model_returns_probability_between_zero_and_one(self):
        model = MLTriageModel(self.config.model)

        # Tránh model sử dụng nhiều CPU trong lúc chạy unit test.
        try:
            model.model["classifier"].n_jobs = 1
        except (KeyError,  TypeError, AttributeError):
            pass
        
        # Không cần dataset.parquet.
        # Tạo trực tiếp một dictionary có đủ feature mà model yêu cầu.
        features ={
            feature_name: 0.0 for feature_name in model.input_features
        }

        score = model.predict_risk(features)

        self.assertIsInstance(score, float)
        self.assertGreater(score, 0.0)
        self.assertLessEqual(score, 1.0)

    def test_predict_risk_uses_saved_feature_order(self):
        expected_order = [
            "hr_mean",
            "spo2_mean",
            "sbp_mean",
        ]

        fake_pipeline = FakePipeline(
            feature_names = expected_order,
            probability = 0.75
        )

        fake_bundle = {
            "pipeline": fake_pipeline,
            "feature_order": expected_order,
            "metadata":{
                "prediction_horizon_seconds": self.config.model.prediction_horizon_seconds
            },
        }

        with patch("src.realtime_medical.triage.trigger_model.joblib.load", return_value = fake_bundle):
            model = MLTriageModel(self.config.model)

            # Cố ý truyền dictionary không đúng thứ tự.
        features = {
            "sbp_mean": 120,
            "hr_mean": 80,
            "spo2_mean": 98,
        }

        score = model.predict_risk(features)

        self.assertEqual(score, 0.75)

        received_frame = fake_pipeline.received_frame

        self.assertIsNotNone(received_frame)
        self.assertEqual(list(received_frame.columns), expected_order,)

        self.assertEqual(float(received_frame.iloc[0]["hr_mean"]), 80.0,)
        self.assertEqual(float(received_frame.iloc[0]["spo2_mean"]), 98.0)
        self.assertEqual(float(received_frame.iloc[0]["sbp_mean"]),  120.0)

    def test_missing_feature_is_passed_as_nan_for_imputation(self):
        feature_order = [
            "hr_mean",
            "spo2_mean",
        ]

        fake_pipeline = FakePipeline(
            feature_names = feature_order,
            probability =  0.4,
        )

        fake_bundle = {
            "pipeline": fake_pipeline,
            "feature_order": feature_order,
            "metadata": {
                "prediction_horizon_seconds":
                    self.config.model.prediction_horizon_seconds,
            },
        }

        with patch("src.realtime_medical.triage.trigger_model.joblib.load",return_value = fake_bundle):
            model =  MLTriageModel(self.config.model)

        score =  model.predict_risk({
            "hr_mean": 85,
        })

        self.assertEqual(score, 0.4)
        self.assertEqual(
            float(fake_pipeline.received_frame.iloc[0]["hr_mean"]),
            85.0,
        )

        self.assertTrue(pd.isna(fake_pipeline.received_frame.iloc[0]["spo2_mean"]))

    def test_unknown_features_are_ignored(self):
        feature_order = [
            "hr_mean",
            "spo2_mean",
        ]

        fake_pipeline = FakePipeline(
            feature_names = feature_order,
            probability = 0.6,
        )

        fake_bundle = {
            "pipeline": fake_pipeline,
            "feature_order": feature_order,
            "metadata": {
                "prediction_horizon_seconds": self.config.model.prediction_horizon_seconds,
            },
        }

        with patch("src.realtime_medical.triage.trigger_model.joblib.load", return_value = fake_bundle,):
            model = MLTriageModel(self.config.model)

        model.predict_risk({
            "hr_mean": 80,
            "spo2_mean": 98,
            "unknown_feature": 999,
        })

        self.assertEqual(
            list(fake_pipeline.received_frame.columns),
            feature_order,
        )

        self.assertNotIn("unknow_feature", fake_pipeline.received_frame.columns,)

    def test_alert_is_required_when_score_equals_threshold(self):
        model = MLTriageModel(self.config.model)

        score = model.recall_threshold

        self.assertTrue(model.is_alert_required(score))

    def test_alert_is_not_required_below_threshold(self):
        model = MLTriageModel(self.config.model)

        score = model.recall_threshold - 0.01

        self.assertFalse(model.is_alert_required(score))
    
    def test_invalid_model_bundles_raise_value_error(self):
        valid_pipeline = FakePipeline(["hr_mean"])

        invalid_bundles = [
            ("bundle is not a dictionary", []),
            ("missing pipeline", 
                {"feature_oreder": ["hr_mean"]}),
            ("missing feature order",
                {"pipeline": valid_pipeline}),
            ("empty feature order",
                {"pipeline": valid_pipeline, "feature_order": []}),
            ("duplicate feature name", 
                {"feature_order": ["hr_mean", "hr_mean"]}),
            ("pipeline has no predict_proba",
                {"pipeline": object(), "feature_order": ["hr_mean"]}),
        ]

        for description, invalid_bundles in invalid_bundles:
            with self.subTest(case= description):
                with patch(
                    "src.realtime_medical.triage.trigger_model.joblib.load",
                    return_value = invalid_bundles
                ):
                    with self.assertRaises(ValueError):
                        MLTriageModel(self.config.model)

    def test_pipeline_feature_order_mismatch_raises_value_error(self):
        fake_pipeline = FakePipeline([
            "spo2_mean",
            "hr_mean",
        ])

        fake_bundle= {
            "pipeline": fake_pipeline,
            "feature_order": ["hr_mean", "spo2_mean"],
            "metadata": {},
        }

        with patch("src.realtime_medical.triage.trigger_model.joblib.load", return_value = fake_bundle):
            with self.assertRaises(ValueError):
                MLTriageModel(self.config.model)

    def test_consumer_uses_window_features_for_ml_prediction(self):
        """
        Kiểm tra tích hợp ở mức logic:
        Kafka message
            -> Consumer
            -> WindowAggregator
            -> ML model
            -> Alert buffer
            -> Database mock
        """

        from src.realtime_medical.streaming.consumer import VitalsConsumer

        class FakeMessage:
            def __init__(self, payload, offset):
                self.payload = payload
                self.message_offset = offset

            def topic(self):
                return "test-vitals"

            def partition(self):
                return 0

            def offset(self):
                return self.message_offset

            def error(self):
                return None

            def  value(self):
                return json.dumps(self.payload).encode("utf-8")
            
        class FakeKafkaConsumer:
            def __init__(self, kafka_config):
                self.subscribed_topics = []

                start_time = datetime.now(timezone.utc) - timedelta(seconds=5)

                self.messages = [
                    FakeMessage({
                        "surgery_id": 1,
                        "patient_id": "1",
                        "recorded_at": (
                            start_time + timedelta(seconds = index)
                        ).isoformat(),
                        "hr": 160 if index == 0 else 75,
                        "spo2": 98,
                        "sbp":120,
                        "dbp":80,
                        "mbp":90,
                        "rr":16,
                        "bt":37.0,
                    }, index)
                    for index in range(6)
                ]

            def subscribe(self, topics, on_assign=None):
                self.subscribed_topics = topics

            def poll(self, timeout):
                if self.messages:
                    return self.messages.pop(0)

                raise KeyboardInterrupt 
            
            def close(self):
                pass

        class FakeDatabase:
            def __init__(self, config):
                self.vitals = []
                self.alerts = []

            def insert_vitals_batch(self, rows):
                self.vitals.extend(rows)

            def insert_alerts_batch(self, rows):
                self.alerts.extend(rows)

            def close(self):
                pass 
        
        class FakeStaticFeatureStore:
            def __init__(self, database):
                pass

            def get(self, surgery_id):
                return {
                    "age": 60,
                    "asa": 2,
                    "bmi": 24,
                    "sex_M": 1,
                    "emop": 0,
                }

        class FakeMLModel:
            def __init__(self, config):
                self.received_features = []
                self.prediction_horizon_seconds = config.prediction_horizon_seconds
                self.model_version = "test-model"

            def predict_risk(self, features):
                self.received_features.append(features)
                return 0.8

            def is_alert_required(self, score):
                return score >= 0.35

        with (
            patch.dict(os.environ, {"BENCH_ENABLED": "0"}),
            patch("src.realtime_medical.streaming.consumer.Consumer", FakeKafkaConsumer),
            patch("src.realtime_medical.streaming.consumer.DataBaseClient", FakeDatabase),
            patch("src.realtime_medical.streaming.consumer.StaticFeatureStore", FakeStaticFeatureStore),
            patch("src.realtime_medical.streaming.consumer.MLTriageModel", FakeMLModel)
            ):
            consumer = VitalsConsumer(self.config)
            consumer.start_listening()
        self.assertEqual(len(consumer.db_client.vitals), 6)

        self.assertGreaterEqual(len(consumer.ml_model.received_features), 1)
        model_features = (consumer.ml_model.received_features[0])

        self.assertIn("hr_mean", model_features)

        self.assertEqual(model_features["age"], 60)
        self.assertEqual(model_features["asa"], 2)
        self.assertEqual(model_features["bmi"], 24)

        self.assertGreaterEqual(len(consumer.db_client.alerts), 2)
        risk_levels = (
            alert["risk_level"]
            for alert in consumer.db_client.alerts
        )

        self.assertIn("CRITICAL", risk_levels)
        self.assertIn("MEDIUM", risk_levels)

        model_alerts = [
            alert 
            for alert in consumer.db_client.alerts
            if alert["model_score"] is not None
        ]

        self.assertGreaterEqual(len(model_alerts), 1)
        self.assertEqual(model_alerts[0]["model_score"], 0.8)

if __name__ == "__main__":
    unittest.main()
