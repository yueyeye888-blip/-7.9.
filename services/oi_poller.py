"""
OI + 资金费率轮询（REST）
"""
import asyncio
import logging
import time
from decimal import Decimal
from typing import Optional

import aiohttp

from core.types import OpenInterest
from services.feature_engine import FeatureEngine

logger = logging.getLogger(__name__)


class OIPoller:

    def __init__(
        self,
        symbols: list,
        base_url: str,
        feature_engine: FeatureEngine,
        poll_interval: int = 5,
    ):
        self._symbols = [s.upper() for s in symbols]
        self._base_url = base_url
        self._feature_engine = feature_engine
        self._poll_interval = poll_interval
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._premium_tick = 0  # 计数器，每 6 次 OI 轮询（30s）拉一次 premiumIndex

    async def start(self, session: aiohttp.ClientSession) -> None:
        self._session = session
        self._running = True
        logger.info(f"OI poller started: {len(self._symbols)} symbols, interval={self._poll_interval}s")
        while self._running:
            await self._poll_all()
            self._premium_tick += 1
            if self._premium_tick % 6 == 1:  # 首次即拉，之后每 30s
                await self._poll_premium_index()
            await asyncio.sleep(self._poll_interval)

    async def stop(self) -> None:
        self._running = False

    async def _poll_all(self) -> None:
        tasks = [self._poll_oi(sym) for sym in self._symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for sym, result in zip(self._symbols, results):
            if isinstance(result, Exception):
                logger.debug(f"[{sym}] OI poll failed: {result}")

    async def _poll_oi(self, symbol: str) -> None:
        url = f"{self._base_url}/fapi/v1/openInterest"
        params = {"symbol": symbol}
        async with self._session.get(
            url, params=params, timeout=aiohttp.ClientTimeout(total=5)
        ) as resp:
            if resp.status == 400:
                # 该 symbol 可能不存在于 futures
                text = await resp.text()
                logger.warning(f"[{symbol}] OI not available: {text[:100]}")
                return
            resp.raise_for_status()
            data = await resp.json()

        oi = OpenInterest(
            symbol=data["symbol"],
            open_interest=Decimal(str(data["openInterest"])),
            timestamp=data["time"] / 1000.0,
        )
        self._feature_engine.on_oi_update(oi)

    async def _poll_premium_index(self) -> None:
        """REST 轮询 premiumIndex，获取 markPrice + fundingRate（弥补 WS 流不可用的情况）"""
        url = f"{self._base_url}/fapi/v1/premiumIndex"
        tasks = [self._fetch_premium(sym) for sym in self._symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for sym, result in zip(self._symbols, results):
            if isinstance(result, Exception):
                logger.debug(f"[{sym}] premiumIndex poll failed: {result}")

    async def _fetch_premium(self, symbol: str) -> None:
        url = f"{self._base_url}/fapi/v1/premiumIndex"
        params = {"symbol": symbol}
        async with self._session.get(
            url, params=params, timeout=aiohttp.ClientTimeout(total=5)
        ) as resp:
            if resp.status != 200:
                return
            data = await resp.json()

        from core.types import MarkPrice
        mp = MarkPrice(
            symbol=data["symbol"],
            mark_price=Decimal(str(data["markPrice"])),
            index_price=Decimal(str(data["indexPrice"])),
            funding_rate=Decimal(str(data["lastFundingRate"])),
            next_funding_time=int(data["nextFundingTime"]),
            timestamp=time.time(),
        )
        self._feature_engine.on_mark_price_update(mp)
