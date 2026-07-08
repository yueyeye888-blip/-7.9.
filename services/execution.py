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
        bal = data.get("totalMarginBalance", "0")
        return Decimal(str(bal))

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

        # 评估新进场
        if signal and signal.is_valid and f.symbol not in self._positions:
            await self._try_enter(signal, f)

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

        pos = Position(
            symbol=signal.symbol,
            side="LONG",
            qty=qty,
            entry_price=fill_price,
            entry_bid_depth_03=f.bid_depth_03,
            breakout_price=signal.breakout_price,
            opened_at=time.time(),
            signal_id=str(uuid.uuid4()),
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
        holding_sec = time.time() - pos.opened_at

        # 1. 盘口信号反转：买压消失（进场条件的对立面）
        if holding_sec > 30 and f.book_imbalance_03 < 1.0:
            return True, "BOOK_IMBALANCE_REVERSED"

        # 2. 主动买入大幅减弱（价格代理：跌幅 > 0.25%）
        if holding_sec > 30 and f.taker_buy_ratio_10s < 0.40:
            return True, "TAKER_BUY_WEAKENED"

        # 3. 买盘深度崩塌 70%（大单撤单/被吃光）
        if (pos.entry_bid_depth_03 > 0 and
                f.bid_depth_03 < pos.entry_bid_depth_03 * 0.30):
            return True, "BID_DEPTH_DISAPPEARED"

        # 4. 点差异常（流动性恶化）
        if f.spread_abnormal:
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
