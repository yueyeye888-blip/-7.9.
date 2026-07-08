"""
symbol 服务 —— 启动时验证标的是否在 Binance U本位合约上市并处于 TRADING 状态
"""
import logging
from typing import Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class SymbolInfo:
    def __init__(self, data: dict):
        self.symbol: str = data["symbol"]
        self.status: str = data["status"]
        self.price_precision: int = data["pricePrecision"]
        self.quantity_precision: int = data["quantityPrecision"]
        self.tick_size: str = self._filter(data["filters"], "PRICE_FILTER", "tickSize")
        self.step_size: str = self._filter(data["filters"], "LOT_SIZE", "stepSize")
        self.min_qty: str = self._filter(data["filters"], "LOT_SIZE", "minQty")

    @staticmethod
    def _filter(filters: list, type_: str, key: str) -> str:
        for f in filters:
            if f["filterType"] == type_:
                return f.get(key, "0")
        return "0"


async def fetch_symbol_info(
    base_url: str,
    wanted: List[str],
    session: aiohttp.ClientSession,
) -> Dict[str, SymbolInfo]:
    """
    拉取 exchangeInfo，返回 wanted 列表中实际上市的 symbol 信息。
    不在列表里或未 TRADING 的会打 warning。
    """
    url = f"{base_url}/fapi/v1/exchangeInfo"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        data = await resp.json()

    all_info: Dict[str, SymbolInfo] = {}
    for item in data["symbols"]:
        if (
            item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and item.get("contractType") == "PERPETUAL"
        ):
            all_info[item["symbol"]] = SymbolInfo(item)

    result: Dict[str, SymbolInfo] = {}
    for sym in wanted:
        if sym in all_info:
            result[sym] = all_info[sym]
            logger.info(f"[{sym}] OK confirmed on Binance Futures")
        else:
            logger.warning(f"[{sym}] ✗ NOT found or not TRADING on Binance Futures — will skip")

    return result
