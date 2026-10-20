"""
Quản lý vòng đời agent chạy nền trong cùng process với consumer.

Runtime tạo worker, chạy worker trong thread riêng và xuất kết quả ra
console. Consumer chỉ đưa snapshot vào queue, không chạy graph trực tiếp.

Queue đầy hoặc worker thất bại được báo về consumer để lượt thử nghiệm
không tiếp tục âm thầm. Queue và trạng thái vẫn nằm trong RAM; phiên bản
này chưa cung cấp lưu bền vững hoặc tự động xử lý lại sau restart.
"""
import json
import logging
from threading import Thread
from typing import Optional

from .graph import RiskAgentGraph
from .harness import AgentHarness
from .state import AgentPolicy, AgentResult, RiskEvent
from .tools import AgentTools
from .worker import AgentWorker

logger = logging.getLogger(__name__)

class AgentRuntimeError(RuntimeError):
    """Báo lỗi vòng đời worker hoặc lỗi tiếp nhận sự kiện."""

class AgentRuntime:
    """Chạy một agent worker nền và quản lý việc bắt đầu, dừng, báo lỗi."""

    def __init__(self, policy:AgentPolicy, queue_size: int = 256):
        self._error: Optional[Exception] = None
        self._started = False
        self._closed = False

        graph = RiskAgentGraph(policy = policy, tools =  AgentTools())
        harness = AgentHarness(graph)
        self.worker =  AgentWorker(
            harness = harness,
            publisher = self._publish,
            failure_handler = self._handle_failure,
            queue_size = queue_size,
        )

        # Thread daemon cho bản thử nghiệm. close() vẫn chủ động chờ
        # xử lý hết queue; nếu quá hạn, lượt chạy phải được coi là lỗi.
        self._thread = Thread(
            target = self._run,
            name = "risk-agent-worker",
            daemon = True,
        )

    def start(self): 
        """Khởi động worker đúng một lần trước khi nhận sự kiện."""
        if self._started or self._closed:
            raise AgentRuntimeError("Runtime has already started or is closed")

        self._thread.start()
        self._started = True
        logger.info("Agent worker started")

    def check_health(self):
        """Báo lỗi nếu worker thất bại hoặc dừng ngoài dự kiến."""
        if not self._started or self._closed:
            raise AgentRuntimeError("Agent runtime is not active")

        if self._error is not None:
            raise AgentRuntimeError(
                 "Agent worker failed during processing"
            ) from self._error

        if not self._thread.is_alive():
            raise AgentRuntimeError("Agent worker stopped unexpectedly")

    def submit(self, event: RiskEvent):
        """Đưa sự kiện vào queue và báo lỗi rõ ràng nếu không tiếp nhận."""
        self.check_health()

        if not self.worker.submit(event):
            raise AgentRuntimeError(
                f"Queue is full; event was not accepted event_id={event.event_id}"
            )

    def close(self, timeout_seconds: float = 5.0): 
        """Ngừng nhận sự kiện và chờ worker xử lý hết queue.

        Bên gọi phải ngừng submit trước khi đóng. Quá hạn hoặc worker
        thất bại làm lượt chạy không đạt điều kiện shutdown thành công.
        """

        if self._closed:
            return 
        
        self._closed = True 
         
        if not self._started:
            return

        self.worker.stop()
        self._thread.join(timeout = timeout_seconds)

        if self._thread.is_alive():
            raise AgentRuntimeError("Worker did not drain the queue within the shutdown timeout")

        if self._error is not None:
            raise AgentRuntimeError("Worker terminated with an error") from self._error
        
        logger.info("Agent worker stopped and drained the queue")

    def _run(self) -> None:
        """Chạy worker và lưu ngoại lệ để consumer phát hiện."""
        try: 
            self.worker.run_forever()
        except Exception as error:
            self._error = error
            logger.exception("Agent worker stopped due to an error")

    def _publish(self, result: AgentResult):
        """Xuất kết quả có cấu trúc ra console bằng logging."""
        logger.info(
            "AGENT_RESULT %s",
            json.dumps(
                result.to_dict(),
                ensure_ascii = False,
                allow_nan = False,
            ),
        )

    def _handle_failure(self, event: RiskEvent,  error: Exception):
        """Dừng worker khi một sự kiện thất bại trong bản thử nghiệm."""
        raise AgentRuntimeError(f"Failed to process event_id={event.event_id}") from error