"""
WebSocket 行情接入 —— 一个 combined stream 覆盖全部 10 个 symbol
"""
import asyncio
import json
import logging
import time
from typing import Optional

import aiohttp

from services.feature_engine import FeatureEngine
from services.orderbook import OrderBookManager

logger = logging.getLogger(__name__)

# 每个 symbol 订阅的 stream 类型
_STREAMS_PER_SYMBOL = [
    "{sym}@depth@100ms",
    "{sym}@bookTicker",
    "{sym}@aggTrade",
    "{sym}@markPrice@1s",
    "{sym}@forceOrder",
]

WS_RECONNECT_DELAY = 5   # 秒
WS_PING_INTERVAL = 20


class MarketDataService:

    def __init__(
        self,
        symbols: list,
        ws_base_url: str,
        book_mgr: OrderBookManager,
        feature_engine: FeatureEngine,
        alerter=None,
        session: Optional[aiohttp.ClientSession] = None,
        proxy: Optional[str] = None,
    ):
        self._symbols = [s.upper() for s in symbols]
        self._ws_base_url = ws_base_url
        self._book_mgr = book_mgr
        self._feature_engine = feature_engine
        self._alerter = alerter
        self._session = session
        self._proxy = proxy
        self._running = False
        self._connected_event = asyncio.Event()

        # 统计
        self._msg_count = 0
        self._last_msg_ts = 0.0
        self._connect_time: Optional[float] = None

    def _build_stream_url(self) -> str:
        streams = []
        for sym in self._symbols:
            sym_lower = sym.lower()
            for template in _STREAMS_PER_SYMBOL:
                streams.append(template.format(sym=sym_lower))
        joined = "/".join(streams)
        return f"{self._ws_base_url}/stream?streams={joined}"

    async def start(self) -> None:
        self._running = True
        url = self._build_stream_url()
        logger.info(f"WS streams: {len(self._symbols)} symbols × {len(_STREAMS_PER_SYMBOL)} = {len(self._symbols)*len(_STREAMS_PER_SYMBOL)} streams")

        while self._running:
            try:
                await self._connect(url)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"WS error: {e}")
                # 断线：重置所有订单簿，等待后重连
                self._book_mgr.reset_all()
                logger.info(f"WS reconnecting in {WS_RECONNECT_DELAY}s...")
                await asyncio.sleep(WS_RECONNECT_DELAY)

    async def stop(self) -> None:
        self._running = False

    async def wait_connected(self) -> None:
        """等待 WS 首次连接成功（供 main.py 在拉快照前调用）"""
        await self._connected_event.wait()

    async def _connect(self, url: str) -> None:
        ws_kwargs: dict = dict(
            heartbeat=WS_PING_INTERVAL,
            max_msg_size=10 * 1024 * 1024,
        )
        if self._proxy:
            ws_kwargs["proxy"] = self._proxy
            logger.info(f"WS connecting via proxy {self._proxy}")

        async with self._session.ws_connect(url, **ws_kwargs) as ws:
            self._connect_time = time.time()
            self._connected_event.set()  # 通知等待方：WS 已连接，开始缓冲
            logger.info("WS connected")
            async for msg in ws:
                if not self._running:
                    break
                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    logger.warning(f"WS closed/error: {msg.type} {msg.data}")
                    break
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                self._msg_count += 1
                self._last_msg_ts = time.time()
                try:
                    data = json.loads(msg.data)
                    await self._dispatch(data)
                except Exception as e:
                    logger.warning(f"dispatch error: {e}")

    async def _dispatch(self, msg: dict) -> None:
        # Combined stream 格式: {"stream": "...", "data": {...}}
        event = msg.get("data", msg)
        etype = event.get("e", "")

        if etype == "depthUpdate":
            sym = event["s"]
            await self._book_mgr.handle_depth_event(sym, event)

        elif etype == "bookTicker":
            sym = event["s"]
            await self._feature_engine.handle_book_ticker(sym, event)

        elif etype == "aggTrade":
            sym = event["s"]
            await self._feature_engine.handle_trade(sym, event)

        elif etype == "markPriceUpdate":
            sym = event["s"]
            await self._feature_engine.handle_mark_price(sym, event)

        elif etype == "forceOrder":
            order = event.get("o", {})
            sym = order.get("s", "")
            if sym and self._alerter:
                await self._alerter.on_liquidation(sym, order)

    def stats(self) -> dict:
        return {
            "msg_count": self._msg_count,
            "last_msg_ts": self._last_msg_ts,
            "connected_since": self._connect_time,
            "lag_seconds": time.time() - self._last_msg_ts if self._last_msg_ts else None,
        }
