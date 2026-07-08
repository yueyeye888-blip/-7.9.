"""
推送服务 —— 控制台打印 + Telegram HTML 格式推送
"""
import logging
import time
from decimal import Decimal
from typing import Optional

import aiohttp

from core.types import Position, Signal

logger = logging.getLogger(__name__)


def _pct(v: float, decimals: int = 2) -> str:
    sign = "+" if v >= 0 else ""
    return f"{sign}{v*100:.{decimals}f}%"


def _score_bar(score: float) -> str:
    """5 格进度条"""
    filled = min(5, max(0, int(score / 20)))
    return "█" * filled + "░" * (5 - filled)


class Alerter:

    def __init__(self, tg_enabled: bool, tg_token: str, tg_chat_id: str):
        self._tg_enabled = tg_enabled and bool(tg_token) and bool(tg_chat_id)
        self._tg_token = tg_token
        self._tg_chat_id = str(tg_chat_id)
        self._session: Optional[aiohttp.ClientSession] = None

    def set_session(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    # ------------------------------------------------------------------
    # 信号预警
    # ------------------------------------------------------------------

    async def on_signal(self, signal: Signal) -> None:
        f = signal.features_snapshot
        sc = signal.opportunity_score
        bar = _score_bar(sc)

        lines = [
            f"🚨 <b>高频机会预警</b>",
            f"",
            f"💹 <b>{signal.symbol}</b>  分数：<b>{sc:.1f}</b>  [{bar}]",
            f"价格：<code>{signal.entry_price}</code>",
            f"",
        ]

        if f:
            lines += [
                f"<b>—— OI ——</b>",
                f"  5m OI：  <code>{_pct(f.oi_change_5m)}</code>",
                f"  15m OI： <code>{_pct(f.oi_change_15m)}</code>",
                f"",
                f"<b>—— 主动买入 ——</b>",
                f"  10s： <code>{f.taker_buy_ratio_10s:.1%}</code>   "
                f"30s： <code>{f.taker_buy_ratio_30s:.1%}</code>",
                f"",
                f"<b>—— 盘口 ——</b>",
                f"  失衡： <code>{f.book_imbalance_03:.3f}</code>   "
                f"深度变化： <code>{_pct(f.bid_depth_03_change)}</code>",
                f"  点差： <code>{f.spread_rate*10000:.1f}bp</code>   "
                f"费率： <code>{f.funding_rate*10000:.2f}bp</code>",
                f"",
                f"<b>—— 价格 ——</b>",
                f"  1m： <code>{_pct(f.price_change_1m)}</code>   "
                f"5m： <code>{_pct(f.price_change_5m)}</code>",
                f"  突破1m高： {'<b>是 ✓</b>' if f.price_breaks_1m_high else '否'}",
            ]

        lines.append(f"")
        lines.append(f"🕑 {_ts()}")

        msg = "\n".join(lines)
        logger.info(f"[SIGNAL] {signal.symbol} score={sc:.1f}")
        await self._send_tg(msg)

    # ------------------------------------------------------------------
    # 进场通知
    # ------------------------------------------------------------------

    async def on_entry(self, pos: Position, signal: Signal) -> None:
        mode_tag = "🟡 PAPER" if True else "🔴 LIVE"
        msg = (
            f"🟢 <b>已进场</b> {mode_tag}\n"
            f"\n"
            f"💹 <b>{pos.symbol}</b>  LONG\n"
            f"入场价：<code>{pos.entry_price}</code>\n"
            f"数量：  <code>{float(pos.qty):.4f}</code>\n"
            f"信号分：<code>{signal.opportunity_score:.1f}</code>\n"
            f"\n"
            f"止损规则：信号失效 / 时间止损 / 硬止损\n"
            f"🕑 {_ts()}"
        )
        logger.info(f"[ENTRY] {pos.symbol} @ {pos.entry_price}")
        await self._send_tg(msg)

    # ------------------------------------------------------------------
    # 离场通知
    # ------------------------------------------------------------------

    async def on_exit(
        self,
        pos: Position,
        exit_price: Decimal,
        reason: str,
        pnl_pct: float,
        holding_sec: float,
    ) -> None:
        if pnl_pct > 0:
            emoji, label = "✅", "盈利"
        elif pnl_pct == 0:
            emoji, label = "⚪", "平"
        else:
            emoji, label = "❌", "亏损"

        msg = (
            f"{emoji} <b>已离场</b>  {label}\n"
            f"\n"
            f"💹 <b>{pos.symbol}</b>  LONG\n"
            f"入场价：<code>{pos.entry_price}</code>\n"
            f"离场价：<code>{exit_price}</code>\n"
            f"收益率：<b><code>{pnl_pct:+.3%}</code></b>\n"
            f"最大浮盈：<code>{pos.max_profit_pct:+.3%}</code>\n"
            f"持仓时间：{holding_sec:.0f}s\n"
            f"离场原因：<code>{reason}</code>\n"
            f"\n"
            f"🕑 {_ts()}"
        )
        logger.info(f"[EXIT] {pos.symbol} pnl={pnl_pct:+.3%} reason={reason}")
        await self._send_tg(msg)

    # ------------------------------------------------------------------
    # 强平推送
    # ------------------------------------------------------------------

    async def on_liquidation(self, symbol: str, order: dict) -> None:
        side = order.get("S", "?")
        qty = order.get("q", "?")
        price = order.get("ap", "?")
        filled = order.get("X", "?")
        msg = (
            f"⚡ <b>强平事件</b>\n"
            f"Symbol：<b>{symbol}</b>\n"
            f"方向：{side}  数量：{qty}\n"
            f"成交均价：<code>{price}</code>\n"
            f"状态：{filled}\n"
            f"🕑 {_ts()}"
        )
        logger.info(f"[LIQUIDATION] {symbol} {side} qty={qty} @ {price}")
        await self._send_tg(msg)

    # ------------------------------------------------------------------
    # 风控事件
    # ------------------------------------------------------------------

    async def on_risk_event(self, event_msg: str) -> None:
        full = f"⚠️ <b>风控事件</b>\n{event_msg}\n🕑 {_ts()}"
        logger.warning(f"[RISK] {event_msg}")
        await self._send_tg(full)

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    async def _send_tg(self, text: str) -> None:
        if not self._tg_enabled or not self._session:
            return
        url = f"https://api.telegram.org/bot{self._tg_token}/sendMessage"
        payload = {
            "chat_id": self._tg_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with self._session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"TG send failed: {resp.status} — {body[:120]}")
        except Exception as e:
            logger.debug(f"TG error: {e}")


def _ts() -> str:
    import datetime
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
