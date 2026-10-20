"""
Định nghĩa các bước xử lý độc lập của agent.

Các bước kiểm tra độ mới của dữ liệu, phân loại điểm ML, tổng hợp bằng
chứng quan sát và tạo giải thích theo mẫu. Không bước nào thay đổi điểm
ML hoặc hạ mức cảnh báo hiện tại do rules cung cấp.
"""

from datetime import datetime
from math import isfinite

from typing import Any, Dict, Optional, Tuple
from .state import AgentPolicy, AgentResult, RiskEvent, SurgeryState

def assess_quality(
    event: RiskEvent,
    policy: AgentPolicy,
    now: datetime,
) -> Tuple[str, Tuple[str, ...]]:
    """Đánh giá khả năng sử dụng dự đoán tại thời điểm xử lý.

    Dữ liệu cũ hoặc timestamp đi trước đồng hồ quá mức cho phép sẽ được
    đánh dấu UNUSABLE. Sai khoảng dự đoán khiến dự đoán không được sử dụng.
    """
    limitations = []
    quality = event.data_quality
    age = (now - event.observed_at).total_seconds()

    if age > policy.max_data_age_seconds:
        quality = "UNUSABLE"
        limitations.append(f"Data is stale by {age:.1f} seconds.")

    if age < -policy.max_future_skew_seconds:
        quality =  "UNUSABLE"
        limitations.append("Data timestamp is ahead of the system clock.")

    if event.horizon_seconds != policy.horizon_seconds:
        quality = "UNUSABLE"
        limitations.append("Prediction horizon does not match the agent policy.")

    if quality == "PARTIAL":
        limitations.append("Data only partially meets the assessment requirements.")
    elif quality ==  "UNKNOWN":
        limitations.append("Data quality has not been determined.")
    elif quality == "UNUSABLE" and not limitations:
        limitations.append("Data does not meet the prediction requirements.")
    
    if event.model_score is None:
        limitations.append("ML prediction score is not available.")
    
    return quality, tuple(limitations)

def classify_forecast(
    score: Optional[float],
    quality: str,
    previous_level: str,
    policy: AgentPolicy
) -> str:
    """Phân loại nguy cơ tương lai với ngưỡng vào/ra khác nhau.

    NONE chỉ có nghĩa điểm dưới ngưỡng của chính sách. UNKNOWN nghĩa là
    chưa đủ điều kiện đánh giá; không được diễn giải thành nguy cơ thấp.
    """
    if score is None or quality in {"UNKNOWN", "UNUSABLE"}:
        return "UNKNOWN"
    
    if previous_level == "HIGH" and score >= policy.high_exit:
        return "HIGH"

    if score >= policy.high_enter:
        return "HIGH"

    if previous_level in {"WATCH", "HIGH"}:
        if score >= policy.watch_exit:
            return "WATCH"

    if score >=  policy.watch_enter:
        return "WATCH"

    return "NONE"

def collect_evidence(event: RiskEvent) -> Tuple[str, ...]:
    """Tạo bằng chứng quan sát từ đặc trưng đã có.

    Trend là chênh lệch cuối đầu theo code hiện tại, không phải tốc độ
    thay đổi mỗi giây và không phải đóng góp nhân quả vào điểm model.
    """

    evidence = []
    labels = {
        "hr": ("Heart rate", "bpm"),
        "spo2": ("SpO2", "%"),
        "sbp": ("Systolic blood pressure", "mmHg"),
        "mbp": ("Mean blood pressure", "mmHg"),
        "rr": ("Respiratory rate", "breaths/min"),
    }

    for field_name, (label, unit) in labels.items():
        latest = event.features.get(f"{field_name}_last")
        trend = event.features.get(f"{field_name}_trend")

        if isinstance(latest, (int, float)) and isfinite(latest):
            evidence.append(f"{label} latest: {latest:.1f} {unit}.")
        
        if isinstance(trend, (int, float)) and isfinite(trend):
            if trend != 0:
                evidence.append(
                    f"{label} change from window start to end: "
                    f"{trend:+.1f} {unit}."
                )
    
    span = event.features.get("window_actual_span_seconds")
    if isinstance(span, (int, float)) and isfinite(span):
        evidence.append(f"Observed data span: {span:.1f} seconds.")
    
    return tuple(evidence)

def build_explanation(
    event: RiskEvent,
    forecast_level: str,
    evidence: Tuple[str, ...],
    limitations: Tuple[str, ...],
    context: Dict[str, Any],
) -> str:
    """Viết giải thích theo mẫu từ dữ liệu có thật.

    Chỉ thêm bộ phận chuyên môn nếu có trong ngữ cảnh. Không suy đoán
    chẩn đoán, xác suất lâm sàng hoặc hướng dẫn điều trị.
    """
    current_labels = {
        "LOW": "no rule exceeds an alert threshold",
        "MEDIUM": "a rule indicates a MEDIUM alert",
        "CRITICAL": "a rule indicates a CRITICAL alert",
    }

    forecast_labels = {
        "NONE": "ML score is below the policy watch threshold",
        "WATCH": "ML score meets the watch threshold",
        "HIGH": "ML score meets the high-priority threshold",
        "UNKNOWN": "insufficient conditions for assessment",
    }

    sentences = [
        f"Surgery {event.surgery_id}.",
        f"Current status: {current_labels[event.current_rule_level]}.",
        f"Forecast for the next {event.horizon_seconds} seconds: "
        f"{forecast_labels[forecast_level]}.",
    ]

    department = context.get("department")
    if isinstance(department, str) and department:
        sentences.append(f"Department: {department}")

    if event.model_score is not None:
        sentences.append(
            f"ML score: {event.model_score:.3f}; "
            "not interpreted as a clinical probability."
        )
    
    if event.rule_reasons:
        sentences.append("Rule reasons: "+";".join(event.rule_reasons))

    if evidence:
        sentences.append("Observed evidence: "+" ".join(evidence))

    if limitations:
        sentences.append("Limitations: "+" ".join(limitations))

    return " ".join(sentences)

def should_public_update(
    event: RiskEvent,
    forecast_level: str,
    quality: str,
    state: SurgeryState,
    policy: AgentPolicy,
    now: datetime
) -> bool:
    """Quyết định xuất bản cập nhật agent.

    Xuất bản khi mức cảnh báo hoặc chất lượng dữ liệu thay đổi. Với nguy
    cơ đang hoạt động, cho phép nhắc lại theo khoảng thời gian cấu hình.
    Đây là chính sách cập nhật agent, không chặn cảnh báo khẩn của consumer.
    """

    previous:   Optional[AgentResult]=  state.last_result

    if previous is None:
        return True
    
    changed = (
        previous.current_rule_level != event.current_rule_level
        or previous.forecast_level != forecast_level
        or previous.data_quality != quality
        or previous.model_version != event.model_version
        or previous.rule_reasons != event.rule_reasons 
    )

    if changed:
        return True
    active = (
        event.current_rule_level in  {"MEDIUM", "CRITICAL"}
        or forecast_level in {"WATCH", "HIGH"}
    )


    if not active:
        return False
    
    if state.last_published_at is None:
        return True
    
    elapsed = (now -  state.last_published_at).total_seconds()
    return elapsed >= policy.repeat_after_seconds
