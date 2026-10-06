"""
Cung cấp công cụ chỉ đọc để lấy ngữ cảnh ca mổ.

Ưu tiên context trong sự kiện, sau đó dùng cache có TTL. Loader tùy chọn
có thể đọc database hoặc cache bên ngoài nhưng phải tự cấu hình timeout.

File này không gửi thông báo, không thay đổi dữ liệu bệnh nhân và không
gọi model hoặc LLM.
"""

from collections import OrderedDict
from copy import deepcopy
from time import monotonic
from typing import Any, Callable, Dict, Optional, Tuple

from  .state import RiskEvent

class AgentTools:
    """Lấy ngữ cảnh với cache giới hạn dung lượng.

    Một instance được sử dụng bởi một worker. Nếu chia sẻ giữa nhiều
    thread, cần bổ sung cơ chế đồng bộ hoặc dùng cache bên ngoài.
    """

    def __init__(
        self, 
        context_loader: Optional[
            Callable[[int], Dict[str, Any]]
        ] =  None,
        cache_ttl_seconds: float = 300.0,
        max_cache_entries: int  = 512,
    ) -> None:
        """Khởi tạo loader và cache TTL theo mã ca mổ."""
        if cache_ttl_seconds <= 0 or max_cache_entries <= 0:
            raise ValueError("Cấu hình cache phải lớn hơn 0")

        self.context_loader = context_loader
        self.cache_ttl_seconds = cache_ttl_seconds
        self.max_cache_entries = max_cache_entries
        self._cache: OrderedDict[
            int, Tuple[float,  Dict[str, Any]]
        ] = OrderedDict()

    def get_context(self, event:RiskEvent) -> Dict[str, Any]:
        """
        Trả ngữ cảnh từ snapshot, cache hoặc loader.

        Trả bản sao để bước xử lý sau không sửa dữ liệu trong cache.
        Ngoại lệ từ loader được chuyển lên graph để xử lý dự phòng.
        """

        if event.context:
            return deepcopy(event.context)

        now = monotonic()
        cached = self._cache.get(event.surgery_id)

        if cached is not None:
            cache_at, context= cached
            if now - cache_at < self.cache_ttl_seconds:
                self._cache.move_to_end(event.surgery_id)
                return deepcopy(context)
            self._cache.pop(event.surgery_id)

        if self.context_loader is None:
            return {}

        context = dict(self.context_loader(event.surgery_id) or {})
        self._cache[event.surgery_id] = (now, deepcopy(context))

        while len(self._cache) > self.max_cache_entries:
            self._cache.popitem(last = False)

        return deepcopy(context)
    
    def invalidate_contex(self, surgery_id: int) -> None:
        """Xóa cache khi ngữ cảnh thay đổi hoặc ca mổ kết thúc."""
        self._cache.pop(surgery_id,  None)