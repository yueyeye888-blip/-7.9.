"""
影子执行引擎 —— 支持 paper / live 双模式
  paper: 完全模拟，不发真实订单
  live : MARKET 单，全部使用 HMAC-SHA256 签名请求
"""
import asyncio
import hashlib
import hmac
import logging
import time
import uuid
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional

import aiohttp

from core.config import ExecutionConfig, StrategyConfig
from core.types import Features, Position, Signal

logger = logging.getLogger(__name__)


class ExecutionEngine:

    def __init__(
        self,
        exec_cfg: ExecutionConfig,
        strat_cfg: StrategyConfig,
        alerter=None,
        recorder=None,
        risk_engine=None,
        signal_engine=None,
    ):
        self._cfg = exec_cfg
        self._strat = strat_cfg
        self._alerter = alerter
        self._recorder = recorder
        self._risk = risk_engine
        self._signal_engine = signal_engine

        self._equity = Decimal(str(exec_cfg.paper_account_equity))
        self._positions: Dict[str, Position] = {}  # symbol -> Position
        self._closed_positions: List[Position] = []
        self._last_exit_ts: Dict[str, float] = {}   # symbol -> 平仓时间，用于冷却期
        self._last_hold_diag_ts: Dict[str, float] = {}  # symbol -> 最近一次持仓诊断日志时间
        self._exchange_positions: Dict[str, dict] = {}  # 从交易所同步的持仓（只读展示）
        self._last_exchange_sync_ts: float = 0.0
        self._exchange_sync_ok: bool = False
        self._last_exchange_sync_ok_ts: float = 0.0
        self._last_exchange_guard_log_ts: float = 0.0
        self._exchange_sync_grace_sec: float = 180.0
        # 连续失败计数与退避重试（指数退避，减少 asyncio 事件循环压容）
        self._exchange_sync_fail_count: int = 0
        self._exchange_sync_backoff_until: float = 0.0

        # 实盘上下文（由 main.py 通过 set_live_context 注入）
        self._api_key: str = ""
        self._api_secret: str = ""
        self._session: Optional[aiohttp.ClientSession] = None
        self._symbol_info: dict = {}  # symbol -> SymbolInfo
        self._base_url: str = "https://fapi.binance.com"
        self._live_leverage: int = 10

    # ------------------------------------------------------------------
    # 实盘初始化
    # ------------------------------------------------------------------

    def set_live_context(
        self,
        session: aiohttp.ClientSession,
        symbol_info: dict,
        api_key: str,
        api_secret: str,
        base_url: str,
        live_leverage: int = 10,
    ) -> None:
        self._session = session
        self._symbol_info = symbol_info
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = base_url
        self._live_leverage = live_leverage

    async def setup_live_account(self) -> None:
        """实盘模式启动时调用：获取实际权益、为每个 symbol 设置杠杆。"""
        if self._cfg.mode != "live":
            return

        # 获取实际权益
        equity = await self._fetch_live_equity()
        if equity > 0:
            self._equity = equity
            logger.info(f"[LIVE] account equity: {equity} USDT")
        else:
            logger.warning("[LIVE] account equity is 0 USDT —— check futures account balance")

        # 为所有 symbol 设置杠杆倍数
        for sym in list(self._symbol_info.keys()):
            try:
                await self._set_leverage(sym, self._live_leverage)
            except Exception as e:
                logger.warning(f"[LIVE] set leverage failed for {sym}: {e}")

    # ------------------------------------------------------------------
    # 实盘 API 工具
    # ------------------------------------------------------------------

    def _sign(self, params: dict) -> str:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        return hmac.new(self._api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()

    async def _api_call(
        self,
        method: str,
        path: str,
        params: dict,
        timeout: float = 5.0,
    ) -> dict:
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self._sign(params)
        url = f"{self._base_url}{path}"
        headers = {"X-MBX-APIKEY": self._api_key}
        timeout_obj = aiohttp.ClientTimeout(total=timeout)
        if method == "GET":
            async with self._session.get(url, params=params, headers=headers, timeout=timeout_obj) as r:
                return await r.json()
        else:
            async with self._session.post(url, params=params, headers=headers, timeout=timeout_obj) as r:
                return await r.json()

    async def _fetch_live_equity(self) -> Decimal:
        data = await self._api_call("GET", "/fapi/v2/account", {})
        if isinstance(data, dict) and "code" in data:
            logger.warning(f"[LIVE] fetch equity failed: {data}")
            return Decimal(0)
        bal = data.get("totalMarginBalance", "0")
        return Decimal(str(bal))

    async def refresh_exchange_positions(self, min_interval_sec: float = 10.0) -> None:
        """同步交易所当前非零持仓到内存（只用于 Dashboard 展示，不参与策略仓位管理）。"""
        if self._cfg.mode != "live":
            self._exchange_positions = {}
            self._exchange_sync_ok = True
            self._last_exchange_sync_ok_ts = time.time()
            return

        now = time.time()
        if now - self._last_exchange_sync_ts < min_interval_sec:
            return
        # 指数退避：-2015 等持续失败时稍后再试，避免占用事件循环
        if now < self._exchange_sync_backoff_until:
            return
        self._last_exchange_sync_ts = now

        try:
            data = await self._api_call("GET", "/fapi/v2/positionRisk", {})
            if isinstance(data, dict) and "code" in data:
                self._exchange_sync_fail_count += 1
                backoff_sec = min(30 * (2 ** (self._exchange_sync_fail_count - 1)), 300)
                self._exchange_sync_backoff_until = now + backoff_sec
                logger.warning(
                    f"[LIVE] fetch exchange positions failed: {data} "
                    f"(fail#{self._exchange_sync_fail_count}, backoff {backoff_sec:.0f}s)"
                )
                self._exchange_sync_ok = False
                return

            synced: Dict[str, dict] = {}
            for row in data:
                amt = Decimal(str(row.get("positionAmt", "0")))
                if amt == 0:
                    continue

                symbol = row.get("symbol", "")
                side = "LONG" if amt > 0 else "SHORT"
                position_side = row.get("positionSide", "BOTH")
                key = f"{symbol}:{position_side}"

                notional = Decimal(str(row.get("notional", "0")))
                unreal = Decimal(str(row.get("unRealizedProfit", "0")))
                pnl_pct = float(unreal / abs(notional)) if notional != 0 else 0.0

                synced[key] = {
                    "symbol": symbol,
                    "side": side,
                    "qty": str(abs(amt)),
                    "entry_price": row.get("entryPrice", "0"),
                    "mark_price": row.get("markPrice", "0"),
                    "unrealized_pnl_pct": pnl_pct,
                    "max_profit_pct": 0.0,
                    "max_loss_pct": 0.0,
                    "opened_at": 0,
                    "signal_id": f"EX:{position_side}",
                    "status": "EXCHANGE",
                    "source": "EXCHANGE",
                    "position_side": position_side,
                    "exit_diag": {},
                }

            self._exchange_positions = synced
            self._exchange_sync_ok = True
            self._last_exchange_sync_ok_ts = time.time()
            # 成功后重置退避计数器
            self._exchange_sync_fail_count = 0
            self._exchange_sync_backoff_until = 0.0
        except Exception as e:
            self._exchange_sync_fail_count += 1
            backoff_sec = min(30 * (2 ** (self._exchange_sync_fail_count - 1)), 300)
            self._exchange_sync_backoff_until = time.time() + backoff_sec
            logger.warning(f"[LIVE] refresh exchange positions error: {e} (backoff {backoff_sec:.0f}s)")
            self._exchange_sync_ok = False

    @property
    def exchange_positions(self) -> Dict[str, dict]:
        return self._exchange_positions

    async def _set_leverage(self, symbol: str, leverage: int) -> None:
        await self._api_call(
            "POST", "/fapi/v1/leverage",
            {"symbol": symbol, "leverage": leverage}
        )
        logger.debug(f"[LIVE] {symbol} leverage set to {leverage}x")

    def _round_qty(self, symbol: str, qty: Decimal) -> Decimal:
        info = self._symbol_info.get(symbol)
        if info is None:
            return qty
        step = Decimal(info.step_size)
        if step == 0:
            return qty
        return (qty / step).to_integral_value(rounding=ROUND_DOWN) * step

    async def _place_market_order(
        self,
        symbol: str,
        side: str,           # "BUY" | "SELL"
        qty: Decimal,
        reduce_only: bool = False,
    ) -> dict:
        params: dict = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": str(qty),
            "positionSide": "LONG",   # Hedge Mode 必须指定方向
        }
        # Hedge Mode 下用 positionSide 区分多空，不用 reduceOnly
        result = await self._api_call("POST", "/fapi/v1/order", params)
        if "orderId" not in result:
            raise RuntimeError(f"order failed: {result}")
        logger.info(
            f"[LIVE] {symbol} {side} qty={qty} → orderId={result.get('orderId')} "
            f"status={result.get('status')} avgPrice={result.get('avgPrice')}"
        )
        return result

    # ------------------------------------------------------------------
    # 主更新入口（每次特征更新时调用）
    # ------------------------------------------------------------------

    async def on_features(self, f: Features, signal: Optional[Signal]) -> None:
        if self._cfg.mode == "off":
            return

        # 更新已有持仓
        await self._update_positions(f)

        if self._cfg.mode == "live" and not self._exchange_sync_ok:
            now = time.time()
            sync_age = now - self._last_exchange_sync_ok_ts

            # 首次同步成功前保持 fail-closed；成功后允许短时容错窗口，避免网络抖动导致持续停摆。
            if self._last_exchange_sync_ok_ts <= 0 or sync_age > self._exchange_sync_grace_sec:
                if now - self._last_exchange_guard_log_ts > 30:
                    self._last_exchange_guard_log_ts = now
                    logger.warning("[LIVE] skip new entry because exchange position sync is not ready")
                return

            if now - self._last_exchange_guard_log_ts > 30:
                self._last_exchange_guard_log_ts = now
                logger.warning(
                    "[LIVE] exchange sync degraded, continue with cached positions (age=%.1fs)",
                    sync_age,
                )

        # 评估新进场
        if (
            signal and signal.is_valid and
            f.symbol not in self._positions and
            not self._has_exchange_position_for_symbol(f.symbol)
        ):
            await self._try_enter(signal, f)

    def _has_exchange_position_for_symbol(self, symbol: str) -> bool:
        for p in self._exchange_positions.values():
            if p.get("symbol") != symbol:
                continue
            try:
                if Decimal(str(p.get("qty", "0"))) > 0:
                    return True
            except Exception:
                continue
        return False

    # ------------------------------------------------------------------
    # 进场
    # ------------------------------------------------------------------

    async def _try_enter(self, signal: Signal, f: Features) -> None:
        qty_usdt = float(self._equity) * self._cfg.first_entry_pct
        if f.price == 0:
            return

        raw_qty = Decimal(str(qty_usdt)) / f.price

        if self._cfg.mode == "live":
            qty = self._round_qty(signal.symbol, raw_qty)
            if qty <= 0:
                logger.warning(f"[{signal.symbol}] LIVE qty rounded to 0, skip")
                return
            try:
                order = await self._place_market_order(signal.symbol, "BUY", qty)
                fill_price = Decimal(str(order.get("avgPrice") or f.price))
            except Exception as e:
                logger.error(f"[{signal.symbol}] LIVE entry failed: {e}")
                return
            mode_tag = "LIVE"
        else:
            qty = raw_qty
            fill_price = f.price
            mode_tag = "PAPER"

        # 止损锚点：进场前 5 分钟最低价；若数据不足则退化为进场价 × 0.998（-0.2%）
        sl_anchor = f.price_5m_low if f.price_5m_low > 0 else fill_price * Decimal("0.998")

        pos = Position(
            symbol=signal.symbol,
            side="LONG",
            qty=qty,
            entry_price=fill_price,
            entry_bid_depth_03=f.bid_depth_03,
            entry_bid_depth_07=f.bid_depth_07,
            breakout_price=signal.breakout_price,
            opened_at=time.time(),
            signal_id=str(uuid.uuid4()),
            sl_price=sl_anchor,
        )
        self._positions[signal.symbol] = pos

        logger.info(
            f"[{signal.symbol}] {mode_tag} ENTER LONG @ {fill_price:.6f} "
            f"qty={qty:.4f} score={signal.opportunity_score:.1f}"
        )

        if self._alerter:
            await self._alerter.on_entry(pos, signal)
        if self._recorder:
            await self._recorder.save_signal(signal)
            await self._recorder.save_position_open(pos)

        if self._cfg.mode != "live":
            asyncio.create_task(self._confirm_entry(signal.symbol))

    async def _confirm_entry(self, symbol: str) -> None:
        await asyncio.sleep(self._cfg.confirm_seconds)
        pos = self._positions.get(symbol)
        if not pos or pos.status != "HOLDING":
            return
        # 如果仍然成立（这里简化：paper 模式直接继续）
        logger.debug(f"[{symbol}] entry confirmed after {self._cfg.confirm_seconds}s")

    # ------------------------------------------------------------------
    # 持仓更新 + 离场判断
    # ------------------------------------------------------------------

    async def _update_positions(self, f: Features) -> None:
        pos = self._positions.get(f.symbol)
        if not pos or pos.status != "HOLDING":
            return
        if f.price == 0:
            return

        # 更新 PnL
        pos.mark_price = f.price
        pos.unrealized_pnl_pct = float(f.price / pos.entry_price - 1)
        if pos.unrealized_pnl_pct > pos.max_profit_pct:
            pos.max_profit_pct = pos.unrealized_pnl_pct
        if pos.unrealized_pnl_pct < pos.max_loss_pct:
            pos.max_loss_pct = pos.unrealized_pnl_pct

        holding_sec = time.time() - pos.opened_at
        pos.exit_diag = self._build_exit_diag(pos, f, holding_sec)

        self._log_hold_diagnostics(pos, f)

        should_exit, reason = self._should_exit(pos, f)
        if should_exit:
            await self._exit(pos, f, reason)

    def _build_exit_diag(self, pos: Position, f: Features, holding_sec: float) -> dict:
        depth_ratio = 0.0
        if pos.entry_bid_depth_03 > 0:
            depth_ratio = f.bid_depth_03 / pos.entry_bid_depth_03

        return {
            "holding_sec": round(holding_sec, 1),
            "ready": {
                "book_imbalance_reversed": holding_sec > 30 and f.book_imbalance_03 < 1.0,
                "taker_buy_weakened": holding_sec > 30 and f.taker_buy_ratio_10s < 0.40,
                "bid_depth_disappeared": (
                    pos.entry_bid_depth_03 > 0 and
                    f.bid_depth_03 < pos.entry_bid_depth_03 * 0.30
                ),
                "spread_abnormal": f.spread_abnormal,
            },
            "metrics": {
                "book_imbalance_03": round(f.book_imbalance_03, 3),
                "taker_buy_ratio_10s": round(f.taker_buy_ratio_10s, 4),
                "depth_ratio_vs_entry": round(depth_ratio, 4),
                "spread_abnormal": f.spread_abnormal,
            },
            "gap": {
                "book_imbalance_to_1": round(f.book_imbalance_03 - 1.0, 3),
                "taker_buy_to_40pct": round(f.taker_buy_ratio_10s - 0.40, 4),
                "depth_ratio_to_30pct": round(depth_ratio - 0.30, 4),
            },
        }

    def _log_hold_diagnostics(self, pos: Position, f: Features) -> None:
        now = time.time()
        holding_sec = now - pos.opened_at

        # 新开仓阶段更频繁输出，后续每 30s 输出一次，避免刷屏。
        interval = 15 if holding_sec < 120 else 30
        last_ts = self._last_hold_diag_ts.get(pos.symbol, 0)
        if now - last_ts < interval:
            return
        self._last_hold_diag_ts[pos.symbol] = now

        diag = self._build_exit_diag(pos, f, holding_sec)
        ready = diag["ready"]
        metrics = diag["metrics"]

        logger.info(
            "[HOLD] %s hold=%.0fs pnl=%+.3f%% bi=%.2f tb10=%.0f%% depth=%.0f%% "
            "spread_abn=%s ready={bi:%s,taker:%s,depth:%s,spread:%s}",
            pos.symbol,
            holding_sec,
            pos.unrealized_pnl_pct * 100,
            metrics["book_imbalance_03"],
            metrics["taker_buy_ratio_10s"] * 100,
            metrics["depth_ratio_vs_entry"] * 100,
            ready["spread_abnormal"],
            ready["book_imbalance_reversed"],
            ready["taker_buy_weakened"],
            ready["bid_depth_disappeared"],
            ready["spread_abnormal"],
        )

    def _should_exit(self, pos: Position, f: Features):
        pnl  = pos.unrealized_pnl_pct
        peak = pos.max_profit_pct

        # 1. 止损：价格跌破进场前 5 分钟最低价
        #    sl_price 在开仓时已锚定，0 表示数据不足（不触发）
        if pos.sl_price > 0 and f.price <= pos.sl_price:
            return True, f"SL_5M_LOW({float(pos.sl_price):.6f})"

        # 2. 追踪止盈
        #    激活线：峰值盈利 ≥ 0.20%（确保触发时净利仍为正）
        #    回撤量：动态分档，涨得越猛给越大的空间让行情充分发展
        #      峰值 < 0.40%   → 追踪 0.10%（小行情，快速锁利）
        #      峰值 0.40-1.0% → 追踪 0.18%（中等行情，允许正常震荡）
        #      峰值 ≥ 1.0%    → 追踪 0.30%（大行情/暴拉，给足空间）
        TRAIL_ON = 0.0020
        if peak < 0.0040:
            trail_dd = 0.0010
        elif peak < 0.0100:
            trail_dd = 0.0018
        else:
            trail_dd = 0.0030

        if peak >= TRAIL_ON and pnl <= peak - trail_dd:
            return True, f"TRAILING_STOP(peak={peak:.3%},dd={trail_dd:.3%})"

        # 3. 买盘深度崩塌：扫描皅0.7%价位深度，跌至进场时30%以下立即出场
        if (pos.entry_bid_depth_07 > 0 and
                f.bid_depth_07 < pos.entry_bid_depth_07 * 0.30):
            return True, "BID_DEPTH_DISAPPEARED"

        # 4. 点差异常：仅在账面浮亏时触发（暖拉时点差也会扩大，盈利时不应强制离场）
        if f.spread_abnormal and pnl < 0:
            return True, "SPREAD_ABNORMAL"

        return False, ""

    async def _exit(self, pos: Position, f: Features, reason: str) -> None:
        exit_price = f.price

        if self._cfg.mode == "live":
            try:
                order = await self._place_market_order(
                    pos.symbol, "SELL", pos.qty, reduce_only=True
                )
                fp = order.get("avgPrice")
                if fp:
                    exit_price = Decimal(str(fp))
            except Exception as e:
                logger.error(f"[{pos.symbol}] LIVE exit failed: {e}")
                # 不移除持仓，等下一次重试
                return
            mode_tag = "LIVE"
        else:
            mode_tag = "PAPER"

        pos.status = "CLOSED"
        pnl_pct = float(exit_price / pos.entry_price - 1)
        pos.unrealized_pnl_pct = pnl_pct
        holding_sec = time.time() - pos.opened_at

        logger.info(
            f"[{pos.symbol}] {mode_tag} EXIT @ {exit_price:.6f} "
            f"pnl={pnl_pct:+.3%} holding={holding_sec:.0f}s reason={reason}"
        )

        self._closed_positions.append(pos)
        del self._positions[pos.symbol]
        self._last_exit_ts[pos.symbol] = time.time()   # 记录平仓时间，开始冷却
        self._last_hold_diag_ts.pop(pos.symbol, None)

        # 实盘模式：买卖完后更新实际权益
        if self._cfg.mode == "live":
            try:
                self._equity = await self._fetch_live_equity()
            except Exception:
                pass

        # 风控反馈
        if self._risk:
            self._risk.on_trade_result(pos.symbol, pnl_pct)

        # 信号引擎反馈
        if self._signal_engine:
            self._signal_engine.record_signal_result(pos.symbol, pnl_pct > 0)

        if self._alerter:
            await self._alerter.on_exit(pos, exit_price, reason, pnl_pct, holding_sec)
        if self._recorder:
            await self._recorder.save_position_close(pos, exit_price, reason)

    # ------------------------------------------------------------------
    # 状态查询
    # ------------------------------------------------------------------

    @property
    def positions(self) -> Dict[str, Position]:
        return self._positions

    def summary(self) -> dict:
        closed = self._closed_positions
        wins = [p for p in closed if p.unrealized_pnl_pct > 0]
        losses = [p for p in closed if p.unrealized_pnl_pct <= 0]
        return {
            "mode": self._cfg.mode,
            "equity": float(self._equity),
            "open_positions": len(self._positions),
            "closed_trades": len(closed),
            "win_count": len(wins),
            "loss_count": len(losses),
            "win_rate": len(wins) / len(closed) if closed else 0.0,
            "avg_pnl": (
                sum(p.unrealized_pnl_pct for p in closed) / len(closed)
                if closed else 0.0
            ),
        }
