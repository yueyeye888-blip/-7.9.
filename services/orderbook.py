"""
本地订单簿重建 —— 严格按照 Binance 官方文档的 U/u/pu 规则
https://binance-docs.github.io/apidocs/futures/en/#diff-book-depth-streams
"""
import asyncio
import logging
from collections import defaultdict
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import aiohttp

logger = logging.getLogger(__name__)


class LocalOrderBook:
    """单个 symbol 的本地订单簿"""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: Dict[Decimal, Decimal] = {}  # price -> qty
        self.asks: Dict[Decimal, Decimal] = {}  # price -> qty
        self.last_update_id: int = 0
        self.last_processed_u: Optional[int] = None
        self.initialized: bool = False
        self._buffer: List[dict] = []
        self._reinit_needed: bool = False
        # 首次初始化完成后进入 live 模式，live 模式下 gap 只警告不 reinit
        self._live_mode: bool = False

    # ------------------------------------------------------------------
    # 外部调用接口
    # ------------------------------------------------------------------

    def buffer_event(self, event: dict) -> None:
        """WS 刚连接时，快照未到达前先缓存 depth 事件"""
        if not self.initialized:
            self._buffer.append(event)

    def apply_snapshot(self, snapshot: dict) -> None:
        """应用 REST /fapi/v1/depth 快照，然后消费缓存事件"""
        self.bids = {}
        self.asks = {}
        self.last_update_id = snapshot["lastUpdateId"]
        self.last_processed_u = None

        for price_str, qty_str in snapshot["bids"]:
            qty = Decimal(qty_str)
            if qty > 0:
                self.bids[Decimal(price_str)] = qty

        for price_str, qty_str in snapshot["asks"]:
            qty = Decimal(qty_str)
            if qty > 0:
                self.asks[Decimal(price_str)] = qty

        self.initialized = True
        logger.info(
            f"[{self.symbol}] snapshot applied: lastUpdateId={self.last_update_id}, "
            f"bids={len(self.bids)}, asks={len(self.asks)}, buffered={len(self._buffer)}"
        )

        # 处理缓存的 WS 事件（首次初始化严格校验）
        for evt in self._buffer:
            self._process_event(evt, strict=True)
        self._buffer.clear()

        # 缓冲处理完后进入 live 模式
        self._live_mode = True

    def handle_depth_event(self, event: dict) -> bool:
        """
        处理 WS depth 事件。
        返回 True 表示正常处理，False 表示需要重新初始化（仅首次初始化期间）。
        """
        if not self.initialized:
            self._buffer.append(event)
            return True

        return self._process_event(event, strict=False)

    def needs_reinit(self) -> bool:
        result = self._reinit_needed
        self._reinit_needed = False
        return result

    def reset(self) -> None:
        """WS 断线重连时调用"""
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = 0
        self.last_processed_u = None
        self.initialized = False
        self._buffer.clear()
        self._reinit_needed = False
        self._live_mode = False

    # ------------------------------------------------------------------
    # 查询接口
    # ------------------------------------------------------------------

    def get_depth(self, pct: float) -> Tuple[float, float]:
        """
        计算 mid_price ± pct 范围内的买/卖盘 USDT 价值。
        pct: 0.001 = 0.1%, 0.003 = 0.3%, 0.005 = 0.5%
        """
        if not self.bids or not self.asks:
            return 0.0, 0.0
        best_bid = max(self.bids)
        best_ask = min(self.asks)
        mid = (best_bid + best_ask) / 2
        lower = mid * Decimal(str(1 - pct))
        upper = mid * Decimal(str(1 + pct))

        bid_val = sum(
            float(p * q) for p, q in self.bids.items() if p >= lower
        )
        ask_val = sum(
            float(p * q) for p, q in self.asks.items() if p <= upper
        )
        return bid_val, ask_val

    def best_bid_ask(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        if not self.bids or not self.asks:
            return None, None
        return max(self.bids), min(self.asks)

    # ------------------------------------------------------------------
    # 内部处理
    # ------------------------------------------------------------------

    def _process_event(self, event: dict, strict: bool = False) -> bool:
        """
        strict=True：首次初始化期间，严格按 Binance 规则校验，gap 触发 reinit。
        strict=False：live 模式，gap 只记录 debug 日志，接受事件继续处理。
        订单簿 delta 是幂等增量，短暂 gap 不影响深度计算的准确性。
        """
        u = event["u"]
        U = event["U"]
        pu = event["pu"]

        # 丢弃 u <= lastUpdateId 的过期事件（快照已包含这段范围）
        if u <= self.last_update_id:
            return True

        if self.last_processed_u is None:
            # 第一条有效事件
            if U > self.last_update_id + 1:
                if strict:
                    # 首次初始化：gap 触发 reinit（一次机会）
                    logger.warning(
                        f"[{self.symbol}] gap after snapshot: "
                        f"U={U} lastUpdateId={self.last_update_id} u={u}"
                    )
                    self._reinit_needed = True
                    return False
                else:
                    # Live 模式：接受并从此 event 继续
                    logger.debug(f"[{self.symbol}] live gap after snapshot (accepted), U={U}")
            elif not (U <= self.last_update_id + 1 <= u):
                # u < lastUpdateId+1：仍是过期事件，跳过
                return True
        else:
            if pu != self.last_processed_u:
                if strict:
                    logger.warning(
                        f"[{self.symbol}] pu gap: pu={pu} expected={self.last_processed_u}"
                    )
                    self._reinit_needed = True
                    return False
                else:
                    # Live 模式：重置 chain，继续处理（orderbook delta 自修复）
                    logger.debug(
                        f"[{self.symbol}] live pu gap (accepted): "
                        f"pu={pu} expected={self.last_processed_u}"
                    )

        self._apply_deltas(event["b"], self.bids)
        self._apply_deltas(event["a"], self.asks)
        self.last_processed_u = u
        return True

    @staticmethod
    def _apply_deltas(deltas: list, side: Dict[Decimal, Decimal]) -> None:
        for price_str, qty_str in deltas:
            price = Decimal(price_str)
            qty = Decimal(qty_str)
            if qty == 0:
                side.pop(price, None)
            else:
                side[price] = qty


class OrderBookManager:
    """管理所有 symbol 的订单簿，并负责快照初始化"""

    def __init__(self, symbols: list, base_url: str, snapshot_limit: int = 500):
        self._books: Dict[str, LocalOrderBook] = {
            sym: LocalOrderBook(sym) for sym in symbols
        }
        self._base_url = base_url
        self._snapshot_limit = snapshot_limit
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self, session: aiohttp.ClientSession) -> None:
        self._session = session
        # 并发拉取所有 symbol 的快照
        await asyncio.gather(
            *[self._init_symbol(sym) for sym in self._books]
        )

    async def _init_symbol(self, symbol: str) -> None:
        book = self._books[symbol]
        for attempt in range(5):
            try:
                snapshot = await self._fetch_snapshot(symbol)
                book.apply_snapshot(snapshot)
                return
            except Exception as e:
                logger.error(f"[{symbol}] snapshot fetch failed (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
        logger.error(f"[{symbol}] failed to initialize orderbook after 5 attempts")

    async def _fetch_snapshot(self, symbol: str) -> dict:
        url = f"{self._base_url}/fapi/v1/depth"
        params = {"symbol": symbol, "limit": self._snapshot_limit}
        async with self._session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def handle_depth_event(self, symbol: str, event: dict) -> None:
        book = self._books.get(symbol)
        if not book:
            return
        ok = book.handle_depth_event(event)
        if not ok or book.needs_reinit():
            logger.warning(f"[{symbol}] reinitializing orderbook")
            book.reset()
            await self._init_symbol(symbol)

    def get_book(self, symbol: str) -> Optional[LocalOrderBook]:
        return self._books.get(symbol)

    def reset_all(self) -> None:
        """WS 断线时重置所有订单簿"""
        for book in self._books.values():
            book.reset()
