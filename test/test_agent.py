"""
Kiểm tra các hành vi nền tảng của agent, không cần Kafka hoặc database.

Bao gồm cấu trúc sự kiện, độ mới dữ liệu, chống dao động mức nguy cơ,
cache ngữ cảnh và khả năng xử lý một sự kiện qua worker.
"""

import unittest
from dataclasses import fields
from datetime import datetime, timezone

from src.realtime_medical.agent import (
    AgentHarness,
    AgentPolicy,
    AgentTools,
    AgentWorker,
    RiskAgentGraph,
    RiskEvent,
)
from src.realtime_medical.agent.nodes import (
    assess_quality,
    classify_forecast,
)


class AgentTests(unittest.TestCase):
    """Kiểm tra hành vi đầu vào, phân loại và luồng worker."""

    def setUp(self):
        """Tạo chính sách minh họa và timestamp cố định cho mỗi test."""
        self.policy = AgentPolicy(
            watch_enter=0.35,
            watch_exit=0.25,
            high_enter=0.65,
            high_exit=0.55,
        )
        self.now = datetime(2026, 10, 1, tzinfo=timezone.utc)

    def make_event(self, **overrides):
        """Tạo sự kiện hợp lệ và cho phép thay dữ liệu theo kịch bản."""
        values = {
            "event_id": "test-event-1",
            "surgery_id": 1,
            "observed_at": self.now,
            "current_rule_level": "LOW",
            "model_score": 0.52,
            "horizon_seconds": 60,
            "model_version": "test-v1",
            "data_quality": "GOOD",
        }

        # Giúp các test khác vẫn chạy khi field này đang khai báo sai.
        if "rule_reasons" in {item.name for item in fields(RiskEvent)}:
            values["rule_reasons"] = ()

        values.update(overrides)
        return RiskEvent(**values)

    def test_event_has_rule_reasons_field(self):
        """Lý do rules phải là một field của dataclass."""
        names = {item.name for item in fields(RiskEvent)}
        self.assertIn("rule_reasons", names)

    def test_fresh_data_remains_good(self):
        """Dữ liệu tại đúng thời điểm đánh giá không được coi là tương lai."""
        event = self.make_event()
        quality, limitations = assess_quality(
            event, self.policy, self.now
        )

        self.assertEqual(quality, "GOOD")
        self.assertEqual(limitations, ())

    def test_watch_is_kept_above_exit_threshold(self):
        """Giữ WATCH khi điểm chưa giảm xuống dưới ngưỡng ra."""
        level = classify_forecast(
            0.30, "GOOD", "WATCH", self.policy
        )
        self.assertEqual(level, "WATCH")

    def test_missing_score_is_unknown(self):
        """Không có điểm ML phải trả UNKNOWN, không phải NONE."""
        level = classify_forecast(
            None, "GOOD", "NONE", self.policy
        )
        self.assertEqual(level, "UNKNOWN")

    def test_context_is_loaded_once_and_copied(self):
        """Cache tránh đọc lại và không bị sửa bởi người sử dụng."""
        calls = []

        def loader(surgery_id):
            """Ghi nhận số lần đọc ngữ cảnh."""
            calls.append(surgery_id)
            return {"department": "General surgery"}

        tools = AgentTools(context_loader=loader)
        event = self.make_event()

        first = tools.get_context(event)
        first["department"] = "changed"
        second = tools.get_context(event)

        self.assertEqual(calls, [1])
        self.assertEqual(second["department"], "General surgery")

    def test_worker_publishes_one_valid_result(self):
        """Một sự kiện phải tạo kết quả đúng mà không đi vào nhánh lỗi."""
        published = []
        failures = []

        def record_failure(event, error):
            """Thu lỗi để test thất bại với thông tin cụ thể."""
            failures.append((event.event_id, repr(error)))

        harness = AgentHarness(
            RiskAgentGraph(self.policy, AgentTools())
        )
        worker = AgentWorker(
            harness,
            publisher=published.append,
            failure_handler=record_failure,
        )
        event = self.make_event(
            observed_at=datetime.now(timezone.utc)
        )

        self.assertTrue(worker.submit(event))
        self.assertTrue(worker.run_once(timeout_seconds=0))
        self.assertEqual(failures, [], msg=str(failures))
        self.assertEqual(len(published), 1)
        self.assertEqual(published[0].forecast_level, "WATCH")
        self.assertEqual(published[0].model_score, event.model_score)


if __name__ == "__main__":
    unittest.main()