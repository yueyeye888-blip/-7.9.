"""
信号引擎 —— 机会评分 + 做多进场条件判断
"""
import logging
import time
import uuid
from decimal import Decimal
from typing import Dict, Optional

from core.config import StrategyConfig
from core.types import Features, Signal

logger = logging.getLogger(__name__)


def calc_opportunity_score(f: Features) -> Dict[str, float]:
    """
    返回各子分和总分。
    文档公式：
      score = oi*0.25 + taker*0.20 + book*0.20 + breakout*0.15 + volume*0.10
              + funding*0.05 + spread*0.05 - risk_penalty
    """
    scores = {}

    # ---- OI 分 ----
    oi = 0.0
    if f.oi_change_5m > 0.02:
        oi += 20
    if f.oi_change_5m > 0.04:
        oi += 20   # 累加到 +40
    if f.oi_change_15m > 0.06:
        oi += 30
    if f.oi_change_15m > 0.10:
        oi += 10   # 累加到 +40
    if f.price_change_5m > 0.04 and f.oi_change_15m > 0.10:
        oi -= 30   # 已经大涨了，不是预热
    scores["oi_score"] = oi

    # ---- 主动买入分 ----
    taker = 0.0
    if f.taker_buy_ratio_10s > 0.60:
        taker += 25
    if f.taker_buy_ratio_30s > 0.58:
        taker += 25
    if f.taker_buy_ratio_60s > 0.55:
        taker += 20
    # 高买入比例但价格不动 = 诱多
    if f.taker_buy_ratio_10s > 0.70 and abs(f.price_change_10s) < 0.001:
        taker -= 30
    scores["taker_score"] = taker

    # ---- 盘口分 ----
    book = 0.0
    if f.book_imbalance_03 > 1.3:
        book += 20
    if f.book_imbalance_03 > 1.8:
        book += 10  # 累加到 +30
    if f.bid_depth_03_change > 0.40:
        book += 25
    if f.bid_depth_collapsed:
        book -= 50
    scores["book_score"] = book

    # ---- 突破分 ----
    breakout = 0.0
    if f.price_breaks_30s_high:
        breakout += 20
    if f.price_breaks_1m_high:
        breakout += 10  # 累加 +30
    if f.price_breaks_3m_high:
        breakout += 5   # 累加 +35
    scores["breakout_score"] = breakout

    # ---- 成交量分 ----
    volume = 0.0
    if f.volume_ratio_5m > 2.0:
        volume += 50
    elif f.volume_ratio_5m > 1.5:
        volume += 30
    elif f.volume_ratio_5m > 1.2:
        volume += 15
    scores["volume_score"] = volume

    # ---- 资金费率分 ----
    funding = 0.0
    if abs(f.funding_rate) < 0.0003:
        funding += 50   # 费率正常
    elif f.funding_rate < -0.0003:
        funding += 20   # 费率极负，潜在逼空
    scores["funding_score"] = funding

    # ---- 点差分 ----
    spread = 0.0
    if f.spread_rate < 0.0005:
        spread += 50    # <5bp  极优
    elif f.spread_rate < 0.0010:
        spread += 30    # 5-10bp 良好
    elif f.spread_rate < 0.0020:
        spread += 10    # 10-20bp 可接受
    scores["spread_score"] = spread

    # ---- 风险扣分 ----
    penalty = 0.0
    if f.funding_rate > 0.0005:
        penalty += 20
    if f.spread_rate > 0.0020:
        penalty += 30
    if f.spread_abnormal:
        penalty += 20
    if f.price_change_5m > 0.05:
        penalty += 30
    if f.price_change_15m > 0.10:
        penalty += 50
    if f.taker_buy_ratio_10s > 0.60 and f.price_change_10s < 0.001:
        penalty += 40
    if f.bid_depth_collapsed:
        penalty += 50
    scores["risk_penalty"] = penalty

    # ---- 加权总分 ----
    total = (
        scores["oi_score"] * 0.25
        + scores["taker_score"] * 0.20
        + scores["book_score"] * 0.20
        + scores["breakout_score"] * 0.15
        + scores["volume_score"] * 0.10
        + scores["funding_score"] * 0.05
        + scores["spread_score"] * 0.05
        - scores["risk_penalty"]
    )
    scores["total"] = total
    return scores


class SignalEngine:

    def __init__(self, cfg: StrategyConfig):
        self._cfg = cfg
        # 记录各 symbol 最近信号失败次数（用于风险扣分）
        self._fail_counts: Dict[str, int] = {}
        # 近失败日志节流：每个 symbol 每 60s 最多打一次
        self._last_log_ts: Dict[str, float] = {}

    def evaluate(self, f: Features) -> Optional[Signal]:
        """评估单个 symbol，返回有效信号或 None"""
        if f.price == 0:
            return None

        scores = calc_opportunity_score(f)

        # 连续失败 3 次额外扣 100 分
        fail_penalty = 0.0
        if self._fail_counts.get(f.symbol, 0) >= 3:
            fail_penalty = 100.0

        total_score = scores["total"] - fail_penalty

        reason = {**scores, "fail_penalty": fail_penalty, "final_score": total_score}

        # ---- 周期性状态日志（无论分数高低） ----
        now = time.time()
        log_interval = 60 if total_score >= 40 else 300
        if now - self._last_log_ts.get(f.symbol, 0) >= log_interval:
            self._last_log_ts[f.symbol] = now
            tag = "[NEAR-MISS]" if total_score >= self._cfg.min_score else "[STATUS]"
            logger.info(
                "%s %s  score=%.1f  "
                "oi5=%.2f%%  oi15=%.2f%%  tb10=%.0f%%  tb30=%.0f%%  "
                "bi=%.2f  bdc=%.0f%%  p10s=%.3f%%  p5=%.2f%%  vol=%.1fx  "
                "sp=%.1fbp  brk1m=%s",
                tag, f.symbol, total_score,
                f.oi_change_5m * 100,
                f.oi_change_15m * 100,
                f.taker_buy_ratio_10s * 100,
                f.taker_buy_ratio_30s * 100,
                f.book_imbalance_03,
                f.bid_depth_03_change * 100,
                f.price_change_10s * 100,
                f.price_change_5m * 100,
                f.volume_ratio_5m,
                f.spread_rate * 10000,
                f.price_breaks_1m_high,
            )

        if total_score < self._cfg.min_score:
            return None

        # 检查硬条件
        c = self._cfg
        conditions = {
            "book_imbalance": f.book_imbalance_03 >= c.book_imbalance_03_min,
            "bid_depth_change": f.bid_depth_03_change >= c.bid_depth_03_change_min,
            "spread_ok": f.spread_rate <= c.spread_rate_max,
            "funding_not_hot": f.funding_rate <= 0.0005,
            # 价格确认：10s内涨幅 > 0.05%，排除盘口噪音单
            "price_confirm": f.price_change_10s > 0.0005,
            # 突破确认：当前价格突破30s内高点
            "price_breakout": f.price_breaks_30s_high,
        }

        failed = [k for k, v in conditions.items() if not v]
        if failed:
            if total_score >= 40:
                logger.info(
                    "[NEAR-MISS-COND] %s  score=%.1f  cond=%d/12  miss=%s",
                    f.symbol, total_score, 12 - len(failed), ",".join(failed),
                )
            return None

        return Signal(
            symbol=f.symbol,
            timestamp=time.time(),
            is_valid=True,
            signal_type="LONG_PREHEAT_BREAKOUT",
            opportunity_score=total_score,
            entry_price=f.price,
            breakout_price=f.price_1m_high,
            reason={**reason, "conditions": conditions},
            features_snapshot=f,
        )

    def record_signal_result(self, symbol: str, success: bool) -> None:
        """影子/实盘结果反馈，用于调整失败计数"""
        if success:
            self._fail_counts[symbol] = 0
        else:
            self._fail_counts[symbol] = self._fail_counts.get(symbol, 0) + 1
