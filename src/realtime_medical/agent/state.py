"""
Định nghĩa dữ liệu đầu vào, kết quả và chính sách của agent.

Agent nhận một snapshot đã được consumer tính toán, gồm kết quả rules,
điểm ML, đặc trưng cửa sổ và chất lượng dữ liệu. Điểm ML được giữ nguyên,
không được diễn giải thành xác suất lâm sàng đã hiệu chỉnh.

Trạng thái hiện tại và nguy cơ tương lai được lưu riêng để tránh nhầm
một cảnh báo đang xảy ra với một dự đoán về sự kiện sắp xảy ra.
"""

from dataclasses import dataclass, field
from datetime import datetime
from  math import isfinite
from typing import Any, Dict, Optional, Tuple

@dataclass
class RiskEvent:
    """Snapshot của một ca mổ tại một lần đánh giá.

    Attributes:
        event_id: Mã duy nhất, phải giữ nguyên khi gửi lại cùng sự kiện.
        surgery_id: Mã ca mổ.
        observed_at: Thời điểm dữ liệu, có thông tin múi giờ.
        current_rule_level: Kết quả rules: LOW, MEDIUM hoặc CRITICAL.
        rule_reasons: Các lý do do rules cung cấp.
        model_score: Điểm ML; None khi chưa có dự đoán.
        horizon_seconds: Khoảng tương lai mà model được huấn luyện để dự đoán.
        model_version: Định danh phiên bản model.
        data_quality: GOOD, PARTIAL, UNKNOWN hoặc UNUSABLE.
        features: Đặc trưng được tính từ dữ liệu đến observed_at.
        context: Ngữ cảnh đã có sẵn, giúp hạn chế truy vấn bổ sung.
    """
    event_id: str
    surgery_id: int
    observed_at:  datetime
    current_rule_level: str
    rule_reasons: Tuple[str, ...]
    model_score: Optional[float]
    horizon_seconds: int
    model_version: str
    data_quality:  str
    features: Dict[str, Any] = field(default_factory = dict)
    context: Dict[str, Any] = field(default_factory = dict)

    def __post_init__(self):
        """Kiểm tra định dạng sự kiện trước khi đưa vào hàng đợi."""
        if not self.event_id or self.surgery_id <= 0:
            raise ValueError("Invalid event_id and surgery_id")

        if self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must include a timezone")

        if self.current_rule_level not in {"LOW", "MEDIUM", "CRITICAL"}:
            raise ValueError("Invalid current_rule_level")

        if  self.data_quality not in {"GOOD", "PARTIAL", "UNKNOWN", "UNUSABLE"}:
            raise ValueError("Invalid data_quality")
        
        if self.horizon_seconds <= 0:
            raise ValueError("horizon_seconds must be greater than 0")

        if self.model_score is not None:
            if not isfinite(self.model_score):
                raise ValueError("model_score must be finite")
            if not 0.0 <= self.model_score <= 1.0:
                raise ValueError("model_score must be in [0, 1]")

@dataclass(frozen = True)
class AgentPolicy:
    """Cấu hình đánh giá và chống dao động cảnh báo.
    Ngưỡng vào cao hơn ngưỡng ra để một điểm số dao động nhẹ không liên tục
    bật/tắt cảnh báo. Các ngưỡng phải được lựa chọn từ tập validation.
    """

    watch_enter: float
    watch_exit:float
    high_enter: float
    high_exit: float
    horizon_seconds: int = 60
    max_data_age_seconds: float = 10.0
    max_future_skew_seconds: float = 2.0
    repeat_after_seconds:  float = 60.0
    prosessing_budget_seconds: float = 0.2

    def __post_init__(self):
        """Kiểm tra thứ tự ngưỡng và các giới hạn thời gian."""
        valid_threshold = (
            0 <= self.watch_exit
            <  self.watch_enter
            <= self.high_exit
            < self.high_enter
            <= 1
        )

        if not valid_threshold:
            raise ValueError("Invalid threshold ordering")
        if self.horizon_seconds <= 0:
            raise ValueError("horizon_seconds must be greater than 0")

        positive_limits = (
            self.max_data_age_seconds,
            self.repeat_after_seconds,    
            self.prosessing_budget_seconds,
            )

        if any(value<= 0 for value in positive_limits):
            raise ValueError("Time limits must be greater than 0")

        if self.max_future_skew_seconds < 0:
            raise ValueError("max_future_skew_seconds must be non-negative")

@dataclass(frozen = True)
class AgentResult:
    """Kết quả có cấu trúc để hiển thị hoặc lưu vào hệ thống.

    forecast_level gồm NONE, WATCH, HIGH hoặc UNKNOWN. Đây là mức ưu tiên
    theo chính sách phần mềm, chưa phải phân loại lâm sàng đã xác thực.
    """

    event_id: str
    surgery_id: int
    observed_at: datetime
    generate_at: datetime
    current_rule_level: str
    forecast_level: str
    model_score: Optional[float]
    horizon_seconds: int
    model_version: str
    data_quality: str
    rule_reasons: Tuple[str, ...]
    evidence: Tuple[str, ...]
    limitations:Tuple[str, ...]
    explaination: str
    should_public: bool
    processing_ms: float = 0.0
    budget_exceeded: bool = False


    def to_dict(self) -> Dict[str, Any]:
        """Chuyển kết quả thành dictionary tương thích JSON."""
        from dataclasses import asdict

        payload = asdict(self)
        payload["observed_at"] = self.observed_at.isoformat()
        payload["generate_at"] = self.generate_at.isoformat()
        return payload

@dataclass
class SurgeryState:
    """Bộ nhớ ngắn hạn cho một ca mổ trong một worker.

    Trạng thái này giúp so sánh diễn biến và chống cảnh báo lặp.
    Nó không thay thế lịch sử được lưu bền vững trong database.
    """
    last_result: Optional[AgentResult] = None
    last_published_at: Optional[datetime] = None
    