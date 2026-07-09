"""
风控引擎 —— 判断是否允许进场、单币暂停逻辑
"""
import logging
import time
from typing import Dict, Optional, Tuple

from core.config import RiskConfig
from core.types import Features

logger = logging.getLogger(__name__)


class RiskEngine:

    def __init__(self, cfg: RiskConfig):
        self._cfg = cfg

        # 今日累计亏损（paper 模式也记录）
        self._daily_loss_pct: float = 0.0
        self._daily_trade_count: int = 0
        self._daily_reset_ts: float = self._today_midnight()

        # 连续亏损计数
        self._consecutive_losses: int = 0

        # 系统级暂停
        self._system_paused: bool = False
        self._system_pause_until: float = 0.0

        # 单币暂停：symbol -> pause_until_ts
        self._symbol_paused: Dict[str, float] = {}

        # 单币当日亏损
        self._symbol_daily_loss: Dict[str, float] = {}

        # 单币连续失败
        self._symbol_consecutive_fail: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # 查询接口
    # ------------------------------------------------------------------

    def can_enter(self, symbol: str, f: Features) -> Tuple[bool, str]:
        """
        返回 (allowed, reason)。
        reason 为空字符串表示允许。
        """
        now = time.time()
        self._check_daily_reset(now)

        if self._system_paused or now < self._system_pause_until:
            return False, "SYSTEM_PAUSED"

        if now < self._symbol_paused.get(symbol, 0):
            remaining = int(self._symbol_paused[symbol] - now)
            return False, f"SYMBOL_PAUSED({remaining}s)"

        if self._daily_loss_pct >= self._cfg.max_daily_loss_equity_pct:
            return False, f"DAILY_LOSS_LIMIT({self._daily_loss_pct:.3%})"

        if self._symbol_daily_loss.get(symbol, 0) >= self._cfg.max_symbol_loss_equity_pct:
            return False, f"SYMBOL_DAILY_LOSS_LIMIT"

        # 行情质量检查
        if f.spread_rate > self._cfg.__dict__.get("spread_rate_max", 0.0008):
            return False, "SPREAD_TOO_WIDE"

        if f.spread_abnormal:
            return False, "SPREAD_ABNORMAL"

        if f.bid_depth_collapsed:
            return False, "BID_DEPTH_COLLAPSED"

        return True, ""

    def is_ws_healthy(self, lag_seconds: Optional[float]) -> Tuple[bool, str]:
        """检查 WebSocket 延迟"""
        if lag_seconds is None:
            return False, "WS_NO_DATA"
        if lag_seconds > 1.5:
            return False, f"WS_LAG({lag_seconds:.1f}s)"
        return True, ""

    # ------------------------------------------------------------------
    # 事件反馈
    # ------------------------------------------------------------------

    def on_trade_result(self, symbol: str, pnl_pct: float) -> None:
        """记录一笔交易结果"""
        self._daily_trade_count += 1
        self._daily_loss_pct += min(0.0, pnl_pct)  # 只累计亏损
        self._symbol_daily_loss[symbol] = (
            self._symbol_daily_loss.get(symbol, 0.0) + min(0.0, pnl_pct)
        )

        if pnl_pct < 0:
            self._consecutive_losses += 1
            self._symbol_consecutive_fail[symbol] = (
                self._symbol_consecutive_fail.get(symbol, 0) + 1
            )
            self._check_pause_rules(symbol)
        else:
            self._consecutive_losses = 0
            self._symbol_consecutive_fail[symbol] = 0

        logger.info(
            f"[{symbol}] trade result: pnl={pnl_pct:.3%}, "
            f"daily_loss={self._daily_loss_pct:.3%}, "
            f"consecutive={self._consecutive_losses}"
        )

    def emergency_stop(self) -> None:
        self._system_paused = True
        logger.critical("EMERGENCY STOP activated")

    def resume_system(self) -> None:
        self._system_paused = False
        self._system_pause_until = 0.0
        logger.info("System resumed")

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    def _check_pause_rules(self, symbol: str) -> None:
        now = time.time()
        sym_fails = self._symbol_consecutive_fail.get(symbol, 0)
        if sym_fails >= self._cfg.pause_symbol_after_losses:
            until = now + 1800  # 30 分钟
            self._symbol_paused[symbol] = until
            logger.warning(f"[{symbol}] paused for 30min (consecutive fails={sym_fails})")

        if self._consecutive_losses >= self._cfg.pause_system_after_losses:
            self._system_pause_until = now + 1800
            logger.warning(f"System paused 30min (consecutive losses={self._consecutive_losses})")

        if abs(self._daily_loss_pct) >= self._cfg.max_daily_loss_equity_pct:
            self.emergency_stop()
            logger.critical(f"Daily loss limit reached: {self._daily_loss_pct:.3%}")

    def _check_daily_reset(self, now: float) -> None:
        if now >= self._daily_reset_ts + 86400:
            self._daily_loss_pct = 0.0
            self._daily_trade_count = 0
            self._symbol_daily_loss.clear()
            self._daily_reset_ts = self._today_midnight()
            logger.info("Daily risk counters reset")

    @staticmethod
    def _today_midnight() -> float:
        import datetime
        today = datetime.date.today()
        return float(datetime.datetime(today.year, today.month, today.day).timestamp())

    def status(self) -> dict:
        return {
            "system_paused": self._system_paused,
            "system_pause_until": self._system_pause_until,
            "daily_loss_pct": self._daily_loss_pct,
            "daily_trade_count": self._daily_trade_count,
            "consecutive_losses": self._consecutive_losses,
            "paused_symbols": {
                k: v for k, v in self._symbol_paused.items()
                if v > time.time()
            },
        }
