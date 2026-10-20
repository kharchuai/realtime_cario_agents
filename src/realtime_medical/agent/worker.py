"""
Chạy agent bằng một worker với hàng đợi trong bộ nhớ có giới hạn.

submit() không chờ agent xử lý nên consumer không bị chặn bởi graph.
Khi hàng đợi đầy, submit() trả False; bên gọi phải lưu lại hoặc gửi sang
hàng đợi bền vững, không được coi sự kiện đã được tiếp nhận.

Worker bỏ qua sự kiện trùng đã xử lý và sự kiện cũ hơn trạng thái hiện có.
Bộ nhớ và chống trùng chỉ tồn tại trong vòng đời process. File này phù hợp
cho bản thử nghiệm; triển khai production cần adapter Kafka và lưu trạng
thái/checkpoint bền vững.
"""

import logging
from collections import OrderedDict
from copy import deepcopy
from queue import Empty, Full, Queue
from threading import Event
from typing import Callable, Optional

from .harness import AgentHarness
from .state import AgentResult,  RiskEvent, SurgeryState

logger = logging.getLogger(__name__)

class AgentWorker:
    """Một worker xử lý tuần tự để bảo toàn trạng thái từng ca mổ.

    Publisher được tiêm từ bên ngoài và phải có timeout. Publisher cần
    hỗ trợ idempotency theo event_id nếu có khả năng xử lý lại sự kiện.
    """

    def __init__(
        self,
        harness: AgentHarness,
        publisher: Callable[[AgentResult], None],
        failure_handler:  Callable[[RiskEvent, Exception], None],
        queue_size: int = 256,
        max_active_surgeries: int = 1024,
        dedup_capacity:  int = 10000,
    ):
        self.harness =  harness
        self.publisher = publisher
        self.failure_handler  = failure_handler
        self.max_active_surgeries = max_active_surgeries
        self.dedup_capacity= dedup_capacity

        self._queue: Queue[RiskEvent] = Queue(maxsize = queue_size)
        self._states: dict[int, SurgeryState] = {}
        self._processed:  OrderedDict[str,  None] = OrderedDict()
        self._stop =  Event()

    def submit(self, event: RiskEvent) -> bool:
        """Đưa bản sao snapshot vào hàng đợi mà không chờ.

        True nghĩa là đã nhận vào bộ nhớ, chưa có nghĩa đã lưu bền vững
        hoặc xuất bản thành công. False nghĩa là bên gọi phải xử lý lại.
        """
        try:
            self._queue.put_nowait(deepcopy(event))
            return True
        except Full:
            logger.error("Agent queue is full event_id=%s", event.event_id)
            return False
        
    def run_once(self, timeout_seconds:float = 0.1) -> bool:
        """Lấy và xử lý tối đa một sự kiện.

        Trả False khi không có sự kiện, True khi đã lấy một sự kiện.
        Nếu xử lý lỗi, chuyển sự kiện cho failure_handler. Nếu callback
        này cũng lỗi, ngoại lệ được đưa lên để supervisor phát hiện.
        """
        try:
            event = self._queue.get(timeout = timeout_seconds)
        except Empty:
            return False
        
        try:
            self._process(event)
        except Exception as error:
            logger.exception("Worker processing failed event_id=%s", event.event_id)
            self.failure_handler(event, error)
        finally:
            self._queue.task_done()
        
        return True

    def run_forever(self) -> None:
        """Chạy vòng xử lý đến khi có yêu cầu dừng.

        Sau yêu cầu dừng, tiếp tục xử lý các sự kiện đã nằm trong queue.
        Bên gọi cần ngừng submit trước khi gọi stop().
        """
        while not self._stop.is_set()  or not self._queue.empty():
            self.run_once(timeout_seconds = 0.1)

    def stop(self):
        """Yêu cầu dừng sau khi xử lý hết hàng đợi hiện tại."""
        self._stop.set()

    def release_surgery(self, surgery_id: int):
        """Giải phóng trạng thái và cache khi ca mổ kết thúc.

        Chỉ gọi từ thread của worker sau khi các sự kiện của ca mổ đã
        được xử lý; tránh thay đổi state đồng thời với _process().
        """
        self._states.pop(surgery_id, None)
        self.harness.graph.tools.invalidate_contex(surgery_id)

    def _process(self, event: RiskEvent) -> Optional[AgentResult]:
        """Xử lý sự kiện mới và cập nhật state sau khi publish thành công."""
        if event.event_id  in self._processed:
            return None
        
        state = self._states.get(event.surgery_id)

        if state is None:
            if len(self._states) >= self.max_active_surgeries:
                raise RuntimeError("Active surgery limit reached")
            state = SurgeryState()
            self._states[event.surgery_id] = state

        if state.last_result is not None:
            if event.observed_at < state.last_result.observed_at:
                logger.warning("Skipping late agent update event_id=%s", event.event_id)
                self._remember_processed(event.event_id)
                return None
        
        result = self.harness.execute(event, state)

        if result.should_public:
            self.publisher(result)
            state.last_published_at = result.generate_at
        
        state.last_result = result
        self._remember_processed(event.event_id)
        return result

    def _remember_processed(self, event_id: str):
        """Ghi nhận mã sự kiện đã xử lý trong bộ nhớ chống trùng giới hạn."""
        self._processed[event_id]=  None
        self._processed.move_to_end(event_id)

        while len(self._processed) > self.dedup_capacity:
            self._processed.popitem(last = False)