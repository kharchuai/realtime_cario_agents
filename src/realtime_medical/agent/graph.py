"""
Ghép các node thành luồng xử lý agent có giới hạn.

Luồng thực hiện:
    kiểm tra chất lượng → phân loại điểm → lấy ngữ cảnh
    → tổng hợp bằng chứng → giải thích → quyết định cập nhật.

Không sử dụng vòng suy luận LLM. Nếu lấy ngữ cảnh thất bại, agent vẫn
trả kết quả từ snapshot và ghi rõ phần thông tin còn thiếu.
"""

import logging 
from datetime import  datetime, timezone

from .nodes import assess_quality, build_explanation, classify_forecast, collect_evidence, should_public_update
from .state import AgentPolicy,  AgentResult, RiskEvent,  SurgeryState
from .tools import AgentTools

logger = logging.getLogger(__name__)

class RiskAgentGraph:
    """Điều phối các bước đánh giá cho một sự kiện."""
    def __init__(self, policy: AgentPolicy, tools: AgentTools):
        self.policy = policy
        self.tools = tools
    def run(self, event: RiskEvent, state: SurgeryState) -> AgentResult:
        """Xử lý snapshot và trả kết quả, chưa cập nhật state.

        State chỉ được worker cập nhật sau khi xuất bản thành công,
        giúp một lần xuất bản lỗi có thể được xử lý lại.
        """
        now = datetime.now(timezone.utc)
        quality, limitations = assess_quality(event, self.policy, now)

        previous_level = (
            state.last_result.forecast_level
            if state.last_result is not None
            else "UNKNOWN"
        )
        forecast_level = classify_forecast(
            event.model_score,
            quality,
            previous_level,
            self.policy,
        )

        context = {}
        if quality != "UNUSABLE":
            try: 
                context = self.tools.get_context(event)
            except Exception:
                logger.exception("Không lấy được ngữ cảnh surgery_id=%s", event.surgery_id)
                limitations +=  ("Không lấy được ngữ cảnh bổ sung",)

        evidence = collect_evidence(event)
        explaination = build_explanation(event, forecast_level, evidence, limitations, context)
        publish = should_public_update(event, forecast_level, quality, state, self.policy, now)

        return AgentResult(
            event_id = event.event_id, 
            surgery_id = event.surgery_id,
            observed_at =event.observed_at,
            generate_at = datetime.now(timezone.utc),
            current_rule_level = event.current_rule_level,
            forecast_level = forecast_level,
            model_score = event.model_score,
            horizon_seconds = event.horizon_seconds,
            model_version = event.model_version,
            data_quality = quality,
            rule_reasons = event.rule_reasons,
            evidence = evidence,
            limitations = limitations,
            explaination = explaination,
            should_public = publish,
        )
