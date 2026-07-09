"""
信号引擎 —— 机会评分 + 做多进场条件判断
"""
import logging
import time
import uuid
from collections import deque
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
        # 费后准入与双通道提频参数
        self._round_trip_fee_floor = 0.0008   # Binance 合约实际双边 taker=0.08%
        self._fast_lane_score_relax = 5.0      # 快通道评分放宽量（原3.0→5.0）
        self._fast_lane_spread_max = min(cfg.spread_rate_max, 0.00060)  # 6bp
        self._fast_lane_book_min = 1.10        # 快通道盘口失衡门槛（低于标准的1.5）
        self._fast_lane_depth_min = 0.08       # 快通道深度变化门槛（低于标准的0.3）
        self._fast_lane_taker10_min = max(cfg.taker_buy_ratio_10s_min, 0.50)
        # 记录各 symbol 最近信号失败次数（用于风险扣分）
        self._fail_counts: Dict[str, int] = {}
        # 近失败日志节流：每个 symbol 每 60s 最多打一次
        self._last_log_ts: Dict[str, float] = {}
        self._stats: Dict[str, float] = {
            "accepted_total": 0,
            "accepted_standard": 0,
            "accepted_fast": 0,
            "blocked_conditions": 0,
            "blocked_fee": 0,
        }
        self._recent_fee_blocks = deque(maxlen=20)
        self._last_fast_lane_params: Dict[str, float] = {}

    @staticmethod
    def _clamp(value: float, low: float, high: float) -> float:
        return max(low, min(high, value))

    def _hour_route_profile(self, hour_utc: int) -> dict:
        # 基于历史回测表现做小时段路由：
        # 23 点段偏防守；0-2 点段适度提频；其余保持中性。
        if hour_utc == 23:
            return {
                "name": "DEFENSIVE_23UTC",
                "score_relax_delta": -0.8,
                "spread_max_delta": -0.00008,
                "book_min_delta": 0.10,
                "depth_min_delta": 0.03,
                "taker_min_delta": 0.01,
                "margin_bp_delta": 0.8,
            }
        if hour_utc in (0, 1, 2):
            return {
                "name": "ACTIVE_0_2UTC",
                "score_relax_delta": 0.7,
                "spread_max_delta": 0.00005,
                "book_min_delta": -0.05,
                "depth_min_delta": -0.02,
                "taker_min_delta": -0.01,
                "margin_bp_delta": -0.2,
            }
        return {
            "name": "NEUTRAL",
            "score_relax_delta": 0.0,
            "spread_max_delta": 0.0,
            "book_min_delta": 0.0,
            "depth_min_delta": 0.0,
            "taker_min_delta": 0.0,
            "margin_bp_delta": 0.0,
        }

    def _build_fast_lane_params(self, now_ts: float) -> dict:
        hour_utc = time.gmtime(now_ts).tm_hour
        route = self._hour_route_profile(hour_utc)

        score_relax = self._fast_lane_score_relax + route["score_relax_delta"]
        spread_max = self._fast_lane_spread_max + route["spread_max_delta"]
        book_min = self._fast_lane_book_min + route["book_min_delta"]
        depth_min = self._fast_lane_depth_min + route["depth_min_delta"]
        taker_min = self._fast_lane_taker10_min + route["taker_min_delta"]
        # 费后安全垫（bp），确保提频不牺牲费后质量。
        margin_bp = 0.4 + route["margin_bp_delta"]

        accepted = self._stats["accepted_total"]
        blocked_cond = self._stats["blocked_conditions"]
        blocked_fee = self._stats["blocked_fee"]

        # 条件拦截显著高于通过，且费后拦截占比不高时，适度放宽以提升频率。
        if blocked_cond > max(12, accepted * 4) and blocked_fee < blocked_cond * 0.55:
            score_relax += 0.8
            book_min -= 0.05
            depth_min -= 0.03

        # 无成交试跑档：长时间 0 通过时，进一步放宽 fast 通道，验证市场是否可交易。
        # 注意：费后 required_move 仍保留，防止放宽后变成低质量冲单。
        bootstrap_relax = False
        if accepted == 0 and blocked_cond > 150:
            bootstrap_relax = True
            score_relax += 2.0
            spread_max += 0.00010
            book_min -= 0.15
            depth_min -= 0.09
            taker_min -= 0.05

        # 若费后拦截过高，适度收紧但上限不超过 +0.2bp，避免正反馈死锁
        if blocked_fee > max(8, accepted * 1.5):
            score_relax -= 0.4
            taker_min += 0.005
            margin_bp += 0.2

        score_relax = self._clamp(score_relax, 2.0, 7.0)
        spread_max = self._clamp(spread_max, 0.00040, min(self._cfg.spread_rate_max, 0.00090))
        book_min = self._clamp(book_min, 0.90, 1.60)
        depth_min = self._clamp(depth_min, 0.05, 0.30)
        taker_min = self._clamp(taker_min, 0.50, 0.60)
        margin_bp = self._clamp(margin_bp, 0.3, 1.2)

        return {
            "route": route["name"],
            "hour_utc": hour_utc,
            "bootstrap_relax": bootstrap_relax,
            "score_relax": score_relax,
            "spread_max": spread_max,
            "book_min": book_min,
            "depth_min": depth_min,
            "taker_min": taker_min,
            "margin_bp": margin_bp,
        }

    def _estimate_expected_move(self, f: Features, total_score: float) -> float:
        """粗略估计未来可捕获波动（百分比），用于费后准入守门。"""
        score_edge   = max(0.0, total_score - self._cfg.min_score) * 0.00006  # 加权更高
        flow_edge    = max(0.0, f.taker_buy_ratio_10s - 0.50) * 0.0016
        book_edge    = max(0.0, f.book_imbalance_03 - 1.0) * 0.00065
        depth_edge   = max(0.0, f.bid_depth_03_change) * 0.00065
        breakout_edge = 0.00040 if f.price_breaks_1m_high else (0.00020 if f.price_breaks_30s_high else 0.0)
        p5_edge      = max(0.0, f.price_change_5m) * 0.030   # 5分钟涨幅本身是动量证据
        return score_edge + flow_edge + book_edge + depth_edge + breakout_edge + p5_edge

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

        # 检查硬条件（标准通道）
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

        # 低成本快通道：允许略低分，但要求更低点差与更强微观结构
        fast_params = self._build_fast_lane_params(now)
        self._last_fast_lane_params = fast_params
        fast_price_confirm_min = 0.00045 if fast_params.get("bootstrap_relax") else 0.0007
        fast_breakout_or_momentum = (
            f.price_breaks_30s_high or
            (fast_params.get("bootstrap_relax") and f.price_change_10s > 0.0009)
        )
        fast_conditions = {
            "spread_ultra_low": f.spread_rate <= fast_params["spread_max"],
            "book_strong": f.book_imbalance_03 >= fast_params["book_min"],
            "depth_strong": f.bid_depth_03_change >= fast_params["depth_min"],
            "taker_active": f.taker_buy_ratio_10s >= fast_params["taker_min"],
            "price_confirm": f.price_change_10s > fast_price_confirm_min,
            "price_breakout": fast_breakout_or_momentum,
        }

        passed_standard = total_score >= self._cfg.min_score and all(conditions.values())
        passed_fast = (
            total_score >= self._cfg.min_score - fast_params["score_relax"]
            and all(fast_conditions.values())
        )
        # 动量爆发通道：tb30 强势买盘 + 短期价格涨幅显著，即使盘口结构反向也允许进场
        # 适用场景：强势买方以市价单压制卖盘（bi<1 但 tb30>70%+p5>1.5%）
        passed_momentum_burst = (
            f.taker_buy_ratio_30s >= 0.70
            and f.price_change_5m > 0.015
            and total_score >= 12
            and f.spread_rate <= self._cfg.spread_rate_max
            and not f.spread_abnormal
            and not f.bid_depth_collapsed
        )

        if not passed_standard and not passed_fast and not passed_momentum_burst:
            self._stats["blocked_conditions"] += 1
            failed = [k for k, v in conditions.items() if not v]
            if total_score >= 40:
                logger.info(
                    "[NEAR-MISS-COND] %s  score=%.1f  cond=%d/12  miss=%s",
                    f.symbol, total_score, 12 - len(failed), ",".join(failed),
                )
            return None

        entry_lane = "STANDARD"
        lane_conditions = conditions
        if not passed_standard and passed_fast:
            entry_lane = "FAST_LOW_COST"
            lane_conditions = fast_conditions
        elif not passed_standard and not passed_fast and passed_momentum_burst:
            entry_lane = "MOMENTUM_BURST"
            lane_conditions = {"tb30_strong": True, "p5_burst": True, "spread_ok": True}

        # 费后准入：只有在预估可覆盖费用和点差时才允许入场
        expected_move = self._estimate_expected_move(f, total_score)
        estimated_cost = self._round_trip_fee_floor + max(0.0, f.spread_rate)
        required_move = estimated_cost + fast_params["margin_bp"] / 10000.0
        if expected_move < required_move:
            self._stats["blocked_fee"] += 1
            self._recent_fee_blocks.append({
                "symbol": f.symbol,
                "score": round(total_score, 2),
                "lane": entry_lane,
                "expected_bp": round(expected_move * 10000, 2),
                "cost_bp": round(estimated_cost * 10000, 2),
                "required_bp": round(required_move * 10000, 2),
                "route": fast_params["route"],
                "ts": now,
            })
            if total_score >= self._cfg.min_score - self._fast_lane_score_relax:
                logger.info(
                    "[NEAR-MISS-FEE] %s lane=%s route=%s score=%.1f expect=%.2fbp req=%.2fbp",
                    f.symbol,
                    entry_lane,
                    fast_params["route"],
                    total_score,
                    expected_move * 10000,
                    required_move * 10000,
                )
            return None

        self._stats["accepted_total"] += 1
        if entry_lane == "FAST_LOW_COST":
            self._stats["accepted_fast"] += 1
        else:
            self._stats["accepted_standard"] += 1

        return Signal(
            symbol=f.symbol,
            timestamp=time.time(),
            is_valid=True,
            signal_type="LONG_PREHEAT_BREAKOUT",
            opportunity_score=total_score,
            entry_price=f.price,
            breakout_price=f.price_1m_high,
            reason={
                **reason,
                "entry_lane": entry_lane,
                "hour_route": fast_params["route"],
                "expected_move": expected_move,
                "estimated_cost": estimated_cost,
                "required_move": required_move,
                "fast_lane_params": fast_params,
                "conditions": lane_conditions,
            },
            features_snapshot=f,
        )

    def runtime_stats(self) -> dict:
        stats = dict(self._stats)
        total_accepted = max(1.0, stats["accepted_total"])
        stats["fast_lane_share_pct"] = round(
            stats["accepted_fast"] / total_accepted * 100.0, 2
        )
        stats["fast_lane_live"] = self._last_fast_lane_params
        stats["recent_fee_blocks"] = list(self._recent_fee_blocks)
        return stats

    def record_signal_result(self, symbol: str, success: bool) -> None:
        """影子/实盘结果反馈，用于调整失败计数"""
        if success:
            self._fail_counts[symbol] = 0
        else:
            self._fail_counts[symbol] = self._fail_counts.get(symbol, 0) + 1
