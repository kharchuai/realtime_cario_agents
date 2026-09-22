# =========================================================================
# Module: ml_model.py (MLTriageModel)
#
# Tác dụng: Dự đoán điểm rủi ro (risk score) của bệnh nhân dựa trên model
# machine learning đã train sẵn, dùng làm 1 trong 2 nguồn đánh giá triage
# (nguồn còn lại là RuleEngine dựa trên ngưỡng cố định).
#
# - __init__            : Load model (.pkl) từ đường dẫn config.path,
#                          đọc danh sách feature cần dùng từ
#                          config.input_features (đúng thứ tự lúc train).
# - predict_risk()       : Nhận vào 1 bản ghi vitals, build feature vector
#                          theo đúng thứ tự input_features, trả về xác suất
#                          rủi ro (0.0 - 1.0) từ model.predict_proba().
# - is_alert_required()  : So sánh risk_score với recall_threshold (0.35 -
#                          cố tình đặt thấp để ưu tiên recall, chấp nhận
#                          tăng false positive để giảm bỏ sót ca nguy hiểm
#                          thật) -> trả về True/False có nên tạo alert.
# =========================================================================
import logging
import joblib
import numpy as np 
from src.realtime_medical.config import ModelConfig

logger = logging.getLogger(__name__)

class MLTriageModel:
    def __init__(self,  config: ModelConfig):
        try:
            # Load the trained classification model
            self.model = joblib.load(config.path)
            self.input_features = config.input_features
            logger.info(f"Loaded model from {config.path}")
        except FileNotFoundError:
            logger.warning(f"Model file not found at: {config.path}")
            raise
            
        # Lower the decision threshold to prioritize recall (minimize false negatives)
        self.recall_threshold = 0.35
        
    def predict_risk(self, vitals:dict) -> float:
        """ Predicting risk probability based on realtime data"""
        
        # feature extraction - must match the training phase
        # Assume the model accepts 3 features: Hr spO2 sys BP
        features = np.array([[vitals.get(f) for f in self.input_features]])
        probabilities = self.model.predict_proba(features)
        risk_score = probabilities[0][1]
        return float(risk_score)
    
    def is_alert_required(self, risk_score: float) -> bool:
        """Check  score """
        return risk_score >= self.recall_threshold