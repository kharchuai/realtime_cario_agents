# =========================================================================
# Module: windowing.py (Feature Engineering cho streaming)
#
# THIẾT KẾ (sliding window, không phải tumbling):
#   - window_size_seconds   : độ dài dữ liệu dùng để tính mean/std/trend
#                              (bộ nhớ ngắn hạn), giữ ổn định thống kê.
#   - evaluate_every_seconds: tần suất TRẢ KẾT QUẢ dự đoán, tách rời hoàn
#                              toàn khỏi window_size_seconds. Đây là điểm
#                              khác biệt so với bản tumbling ban đầu —
#                              tumbling làm độ trễ phát hiện lên tới cả
#                              window_size_seconds (không chấp nhận được
#                              cho biến cố cấp tính trong ca mổ).
#   - min_samples_to_evaluate: tránh trả kết quả khi ca mổ vừa bắt đầu,
#                              dữ liệu còn quá ít để tin cậy.
#
# 3 thành phần:
#   1. WindowAggregator   : gom dữ liệu vitals theo cửa sổ trượt, lọc
#                            artifact, forward-fill, tính đặc trưng thống kê.
#   2. StaticFeatureStore : cache dữ liệu tĩnh (patient/surgeries) theo
#                            surgery_id, tránh query DB lặp lại mỗi giây.
#   3. FeatureBuilder      : ghép (1) + (2) thành 1 feature vector phẳng,
#                            dùng chung được cho cả lúc train model (offline,
#                            xem build_offline_windows) lẫn lúc serving
#                            real-time (online) — tránh train/serve skew.
# =========================================================================

import logging
import time

from collections import deque
from dataclasses import dataclass, field
from typing import  Dict,  List,  Optional
from sqlalchemy import text

import numpy as np 
import pandas as pd

from src.realtime_medical.config import StreamingConfig

logger = logging.getLogger(__name__)

CANONICAL_FIELDS =  ["hr", "spo2", "sbp", "dbp", "mbp", "rr", "bt"]

KNOWN_DEPARTMENTS = ["General surgery", "Thoracic surgery", "Urology", "Gynecology"]

@dataclass 
class _SurgeryBuffer:
    """Trạng thái đang tích luỹ cho 1 surgery_id — cửa sổ trượt (sliding)."""
    readings: deque =  field(default_factory= deque) #(ts, filled_vitals) tự động rơi rụng dữ liệu cũ hơn window_size
    last_known: Dict[str,  float] = field(default_factory= dict) #forward-fill xuyen suot ca mo
    last_known_ts: Dict[str, float]= field(default_factory= dict)
    last_evaluated_ts: Optional[float] = None # lần trả kết quả gần nhất
    
class WindowAggregator:
    """Gom dữ liệu vitals streaming theo cửa sổ TRƯỢT (sliding window).

    Khác với tumbling window (đóng-mở theo chu kỳ cố định, độ trễ phát
    hiện có thể lên tới gần window_size_seconds), sliding window luôn giữ
    window_size_seconds dữ liệu gần nhất và cho phép trả kết quả thường
    xuyên hơn nhiều (evaluate_every_seconds) — phù hợp với tính chất ca mổ
    cần phát hiện biến cố cấp tính trong vài giây, không phải vài chục giây.
    """
    
    def __init__(self, config: StreamingConfig):
        self.window_size = config.window_size_seconds
        self.evaluate_every = config.evaluate_every_seconds
        self.min_samples = config.min_samples_to_evaluate
        self.artifact_ranges = config.artifact_filtering
        self.max_gap_seconds = config.missing_data.max_gap_seconds
        self._buffers: Dict[int, _SurgeryBuffer] = {}
        
    def add_reading(self, surgery_id: int, vitals:dict, timestamp: Optional[float] = None) -> Optional[dict]:
        """ 
        Thêm 1 điểm dữ liệu vào buffer của surgery_id.
        Trả về dict feature nếu đã đến lúc đánh giá (đủ evaluate_every_seconds
        kể từ lần trước VÀ đủ min_samples), trả về None nếu chưa.
        """
        
        ts = timestamp if timestamp is not None else time.time()
        filtered = self._filter_artifacts(vitals)
        
        buf = self._buffers.setdefault(surgery_id, _SurgeryBuffer())
        filled = self._forward_fill(buf, filtered, ts)
        buf.readings.append((ts, filled))
        
        # Trượt cửa sổ: bỏ dữ liệu cũ hơn window_size_seconds so với điểm mới nhất
        while buf.readings and (ts - buf.readings[0][0]) > self.window_size:
            buf.readings.popleft()
            
        should_evaluate = (
            len(buf.readings) >= self.min_samples
            and (buf.last_evaluated_ts is None or (ts - buf.last_evaluated_ts) >= self.evaluate_every) 
        )
        if not should_evaluate:
            return None
        
        buf.last_evaluated_ts = ts
        return self._aggregate(surgery_id, buf, ts)
    
    def flush_all(self) -> Dict[int, dict]:
        """Ép tính feature cho toàn bộ surgery đang dang dở — gọi lúc consumer
        shutdown để không mất dữ liệu của cửa sổ chưa tới nhịp evaluate."""
        results = {}
        for surgery_id, buf in self._buffers.items():
            if buf.readings:
                results[surgery_id] = self._aggregate(surgery_id, buf, buf.readings[-1][0])
        self._buffers.clear()
        return results
    
    # -------------------- internal ---------------
    def _filter_artifacts(self, vitals: dict )-> dict:
        """Loại giá trị nằm ngoài range hợp lệ (nhiễu cảm biến), thay bằng None."""
        cleaned = dict(vitals)
        for field_name, rng in self.artifact_ranges.items():
            val = cleaned.get(field_name)
            if val is not None and not (rng.valid_min <= val <= rng.valid_max):
                logger.debug(f"Artifact filtered: {field_name} = {val} outside [{rng.valid_min}, {rng.valid_max}]")
                cleaned[field_name] = None
        return cleaned
    
    def _forward_fill(self, buf: _SurgeryBuffer, vitals: dict, ts:float) ->   dict:
        """Điền giá trị thiếu bằng giá trị hợp lệ gần nhất, nếu khoảng trống
        không vượt quá max_gap_seconds (config.missing_data)."""
        filled = {}
        for f in CANONICAL_FIELDS:
            val = vitals.get(f)
            if val is not None:
                buf.last_known[f] = val
                buf.last_known_ts[f] = ts
                filled[f] = val
            else:
                last_ts = buf.last_known_ts.get(f)
                if last_ts is not None and (ts - last_ts) <= self.max_gap_seconds:
                    filled[f] = buf.last_known[f]        # forward-fill
                else:
                    filled[f] = None     # mat tin hieu qua lau -> de missing that
        return filled
    
    def _aggregate(self, surgery_id:int, buf: _SurgeryBuffer, now_ts:float) -> dict:
        """Tính đặc trưng thống kê trên dữ liệu hiện có trong cửa sổ trượt."""
        actual_span = now_ts -buf.readings[0][0]
        out = {
            "surgery_id": surgery_id,
            "window_end": now_ts,
            "n_samples": len(buf.readings),
            # Lúc case vừa bắt đầu, actual_span < window_size -> cho model/agent
            # biết mức độ tin cậy của feature này thấp hơn bình thường.
            "window_actual_span_seconds":  round(actual_span, 1),
        }
        for f in CANONICAL_FIELDS:
            series = np.array([r[1].get(f) for r in buf.readings], dtype = float)
            valid = series[~np.isnan(series)]
            
            out[f"{f}_mean"] = float(np.mean(valid)) if len(valid) else None
            out[f"{f}_std"] = float(np.std(valid)) if len(valid) > 1 else 0.0
            out[f"{f}_min"] = float(np.min(valid)) if len(valid) else None
            out[f"{f}_max"] = float(np.max(valid)) if len(valid) else None
            out[f"{f}_last"] = float(valid[-1]) if len(valid) else None
            out[f"{f}_trend"] = float(valid[-1] - valid[0]) if len(valid) >= 2 else 0.0
            out[f"{f}_missing_ratio"] = round(1- len(valid) / len(series), 3) if len(series) else 1.0
            
        # Feature phai sinh co y nghia lam sang (giong ShockIndexRule trong rules.py)
        if out["hr_mean"] and out["sbp_mean"]:
            out["shock_index_mean"] = round(out["hr_mean"] / out["sbp_mean"], 3)
        else:
            out["shock_index_mean"] =None
        return out
    
class StaticFeatureStore:
    """Cache dữ liệu tĩnh (patient + surgeries) theo surgery_id, tránh query
    DB lặp lại mỗi giây trong lúc streaming."""
        
    def __init__(self, db_client):
        self.db_client = db_client
        self._cache: Dict[int, dict] = {}
            
    def get(self, surgery_id:int) -> dict:
        if surgery_id in self._cache:
            return self._cache[surgery_id]
        query = text("""
                    SELECT p.age, p.sex, p.bmi,  s.asa, s.emop, s.department, s.optype
                    FROM surgeries s
                    JOIN patient p ON p.patient_id = s.patient_id
                    WHERE s.surgery_id = :surgery_id
                    """)
        
        with self.db_client.Session() as session:
            row = session.execute(query, {"surgery_id": surgery_id}).mappings().first()
            
        if row is None:
            logger.warning("Static data not found for surgery_id = {surgery_id}")
            features = self._default_features()
        else:
            features ={
                "age": row["age"],
                "sex_M": 1 if row["sex"] =="M" else 0,
                "bmi":  row["bmi"],
                "asa":  row["asa"],
                "emop": row["emop"],
            }
            for dept in KNOWN_DEPARTMENTS:
                key = f"department_{dept.lower().replace(' ', '_')}"
                features[key] = 1 if row["department"] == dept else 0 
        self._cache[surgery_id] = features
        return features
    @staticmethod
    def _default_features() -> dict:
        base =  {"age": None, "sex_M": None, "bmi": None, "asa": None, "emop": None}
        for dept in KNOWN_DEPARTMENTS:
            base[f"department_{dept.lower().replace(' ', '_')}"] = 0
        return base
    
class FeatureBuilder:
    """Ghép window feature (streaming) + static feature (patient/surgeries)
    thành 1 dict phẳng — dùng chung schema này cho cả training lẫn serving."""
    def __init__(self, window_aggregator: WindowAggregator, static_store: StaticFeatureStore):
        self.window_aggregator = window_aggregator
        self.static_store = static_store
        
    def process_reading(self, surgery_id: int, vitals:  dict, timestamp: Optional[float] = None) -> Optional[dict]:
        """Gọi trong consumer.py mỗi khi nhận 1 message Kafka.
        Trả về feature vector đầy đủ mỗi evaluate_every_seconds, None nếu chưa tới nhịp."""
        window_features = self.window_aggregator.add_reading(surgery_id, vitals, timestamp)
        if window_features is None:
            return None
        static_features = self.static_store.get(surgery_id)
        return {**window_features, **static_features}
    
        
# Dùng offline lúc train model — TÁI SỬ DỤNG ĐÚNG logic _aggregate() ở
# trên để đặc trưng lúc train và lúc serving KHỚP NHAU 100%.
def build_offline_windows(df: pd.DataFrame, config: StreamingConfig, surgery_id: int) -> pd.DataFrame:
    """
    Chia 1 case (dữ liệu numerics.csv của 1 ca mổ, đã map canonical field)
    thành các mốc evaluate_every_seconds, tính feature giống hệt lúc
    real-time. Dùng khi build tập dữ liệu train cho model.
    """
    aggregator = WindowAggregator(config)
    rows = []
    for _, r in df.sort_values("time_sec").iterrows():
        vitals = {f: r.get(f) for f in CANONICAL_FIELDS}
        result = aggregator.add_reading(surgery_id,  vitals, timestamp=float(r["time_sec"]))
        if result:
            rows.append(result)
    return pd.DataFrame(rows)

            