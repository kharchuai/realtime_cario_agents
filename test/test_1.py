"""
Kiểm tra runtime nhận một sự kiện, xuất kết quả và dừng worker.
Không sử dụng Kafka hoặc database.
"""

import logging
from datetime import datetime, timezone

from src.realtime_medical.agent import AgentPolicy, RiskEvent
from src.realtime_medical.agent.runtime import AgentRuntime

logging.basicConfig(level=logging.INFO)

runtime = AgentRuntime(
    policy=AgentPolicy(
        watch_enter=0.35,
        watch_exit=0.25,
        high_enter=0.65,
        high_exit=0.55,
    ),
)

runtime.start()

try:
    runtime.submit(
        RiskEvent(
            event_id="runtime-test-1",
            surgery_id=1,
            observed_at=datetime.now(timezone.utc),
            current_rule_level="LOW",
            rule_reasons=(),
            model_score=0.52,
            horizon_seconds=60,
            model_version="test-v1",
            data_quality="PARTIAL",
            features={},
            context={},
        )
    )
finally:
    # close() chờ worker xử lý sự kiện đã nhận.
    runtime.close()