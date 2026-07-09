import time
from typing import Dict, List

from core.types import Features


class SharedState:
    """主循环和 Dashboard 服务之间的共享内存"""

    def __init__(self, symbols: list, mode: str):
        self.symbols = symbols
        self.system_mode = mode
        self.started_at = time.time()

        # 实时特征（Features 对象，主循环写入）
        self.features: Dict[str, Features] = {}
        # 实时评分（dict，主循环写入）
        self.scores: Dict[str, dict] = {}
        # 当前持仓（可序列化 dict，主循环写入）
        self.positions: Dict[str, dict] = {}
        # 最近 100 条信号（可序列化 dict）
        self.recent_signals: List[dict] = []
        # WS 状态
        self.ws_stats: dict = {}
        # 风控状态
        self.risk_status: dict = {}
        # 信号统计（费后拦截/通道分布）
        self.signal_stats: dict = {}

    def add_signal(self, sig_dict: dict) -> None:
        self.recent_signals.insert(0, sig_dict)
        self.recent_signals = self.recent_signals[:100]
