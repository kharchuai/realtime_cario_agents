"""
Bao graph bằng kiểm tra đầu ra, đo latency và xử lý lỗi dự phòng.

Harness bảo đảm kết quả không thay đổi điểm model hoặc mức rules đầu vào.
Khi graph lỗi, trả một giải thích tối thiểu với dự đoán UNKNOWN.

Ngân sách thời gian là ngưỡng giám sát, không phải cơ chế cưỡng chế hủy
I/O. Mọi client được gọi phải có timeout riêng.
"""
import logging 
from dataclasses import replace
from datetime import datetime, timezone
from time import perf_counter

from .graph import RiskAgentGraph
from .state import AgentResult, RiskEvent, SurgeryState

logger = logging.getLogger(__name__)

class AgentHarness:
    """Thực thi graph và kiểm tra tính nhất quán của kết quả."""

    def __init__(self, graph: RiskAgentGraph):
        self.graph = graph
    
    def execute(self,  event: RiskAgentGraph, state: SurgeryState) ->  AgentResult:
        """Chạy graph, xác thực kết quả và ghi thời gian xử lý."""
        started = perf_counter()

        try:
            result = self.graph.run(event, state)
            self._validate(event, result)
        except Exception:
            logger.exception("Agent lỗi event_id=%s", event.event_id)
            result = self._fallback(event)

        elapsed_ms = (perf_counter() - started) * 1000
        exceeded = (elapsed_ms > self.graph.policy.prosessing_budget_seconds * 1000)

        if exceeded:
            logger.warning("Agent vượt ngân sách event_id=%s latency_ms=%.1f", event.event_id, elapsed_ms)
        return replace(
            result, 
            processing_ms = round(elapsed_ms, 3),
            budget_exceeded  = exceeded 
            )
    
    def _validate(self, event:RiskEvent, result: AgentResult):
        """Kiểm tra kết quả vẫn tham chiếu đúng snapshot đầu vào."""
        expected =  (
            event.event_id,
            event.surgery_id,
            event.observed_at,
            event.current_rule_level,
            event.model_score,
            event.horizon_seconds,
            event.model_version,
        )

        actual = (
            result.event_id,
            result.surgery_id,
            result.observed_at,
            result.current_rule_level,
            result.model_score,
            result.horizon_seconds,
            result.model_version,
        )

        if actual != expected:
            raise ValueError("Agent đã thay đổi dữ liệu nguồn")
        
        if result.forecast_level not in {"NONE", "WATCH", "HIGH", "UNKNOWN"}:
            raise ValueError("forecast_level không hợp lệ")

        if not result.explaination:
            raise ValueError("Thiếu giải thích")

    def _fallback(self, event: RiskEvent) -> AgentResult:
        """Tạo đầu ra tối thiểu, giữ nguyên cảnh báo rules và điểm ML."""
        return AgentResult(
            event_id =  event.event_id,
            surgery_id = event.surgery_id,
            observed_at=  event.observed_at,
            generate_at = datetime.now(timezone.utc),
            current_rule_level = event.current_rule_level,
            forecast_level ="UNKNOWN",
            model_score = event.model_score,
            horizon_seconds = event.horizon_seconds,
            model_version = event.model_version,
            data_quality = "UNKNOWN",
            rule_reasons = event.rule_reasons,
            evidence = (),
            limitations =("Agent gặp lỗi khi tổng hợp kết quả.",),
            explaination = (
                f"Ca mổ {event.surgery_id}: "
                f"rule = {event.current_rule_level}."
                "Chưa tạo được giải thích dự đoán;  "
                "giữ nguyên kết quả ngồn đẻ người phụ trách xem xét"
            ),
            should_public= True,
        )
