"""Load the trained bundle and score features in its saved training order."""

import logging
from pathlib import Path
import hashlib

import joblib
import pandas as pd

from src.realtime_medical.config import BASE_DIR, ModelConfig

logger = logging.getLogger(__name__)


class MLTriageModel:
    def __init__(self, config: ModelConfig):
        model_path = Path(config.path)
        if not model_path.is_absolute():
            model_path = BASE_DIR / model_path

        bundle = joblib.load(model_path)
        if not isinstance(bundle, dict) or "pipeline" not in bundle or "feature_order" not in bundle:
            raise ValueError(f"Invalid model bundle at {model_path}: pipeline and feature_order are required")

        self.model = bundle["pipeline"]
        self.input_features = bundle["feature_order"]
        if (
            not isinstance(self.input_features, (list, tuple))
            or not self.input_features
            or any(not isinstance(name, str) for name in self.input_features)
            or len(set(self.input_features)) != len(self.input_features)
            or not callable(getattr(self.model, "predict_proba", None))
        ):
            raise ValueError(f"Invalid model bundle at {model_path}: unusable pipeline or feature_order")

        trained_features = getattr(self.model, "feature_names_in_", None)
        if trained_features is not None and list(trained_features) != list(self.input_features):
            raise ValueError(f"Model feature order differs from bundle metadata at {model_path}")

        metadata = bundle.get("metadata", {})
        trained_horizon = metadata.get("prediction_horizon_seconds")

        if (
            isinstance(trained_horizon, bool)
            or not isinstance(trained_horizon, int)
            or trained_horizon <= 0
        ):
            raise ValueError( "Model thiếu prediction_horizon_seconds hợp lệ trong metadata")

        if trained_horizon != config.prediction_horizon_seconds:
            raise ValueError(
                "Khoảng dự đoán của model không khớp config: "
                f"model={trained_horizon}, "
                f"config={config.prediction_horizon_seconds}"
            )

        self.prediction_horizon_seconds = trained_horizon
        self.model_version = ("sha256:" + hashlib.sha256(model_path.read_bytes()).hexdigest())


        logger.info("Loaded model from %s with %d features", model_path, len(self.input_features))
        self.recall_threshold = 0.35

    def predict_risk(self, features: dict) -> float:
        """Predict from window and static features; the pipeline imputes missing values."""
        row = {name: features.get(name) for name in self.input_features}
        frame = pd.DataFrame([row], columns=self.input_features).astype(float)
        return float(self.model.predict_proba(frame)[0][1])

    def is_alert_required(self, risk_score: float) -> bool:
        return risk_score >= self.recall_threshold
