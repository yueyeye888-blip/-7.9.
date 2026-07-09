"""
特征引擎 —— 维护每个 symbol 的滚动窗口，实时计算所有特征
"""
import logging
import time
from collections import deque
from decimal import Decimal
from typing import Deque, Dict, Optional, Tuple

from core.types import BookTick, Features, MarkPrice, OpenInterest, Trade
from services.orderbook import OrderBookManager

logger = logging.getLogger(__name__)


class _TimeWindow:
    """定长时间窗口，自动丢弃过期元素"""

    def __init__(self, max_seconds: float):
        self._max_sec = max_seconds
        self._q: Deque[Tuple[float, object]] = deque()

    def append(self, value: object, ts: float = None) -> None:
        t = ts if ts is not None else time.time()
        self._q.append((t, value))
        self._evict(t)

    def items(self, now: float = None) -> list:
        t = now if now is not None else time.time()
        self._evict(t)
        return [v for _, v in self._q]

    def _evict(self, now: float) -> None:
        cutoff = now - self._max_sec
        while self._q and self._q[0][0] < cutoff:
            self._q.popleft()

    def oldest_ts(self) -> Optional[float]:
        return self._q[0][0] if self._q else None

    def __len__(self) -> int:
        return len(self._q)


class SymbolFeatureState:
    """单个 symbol 的特征状态"""

    def __init__(self, symbol: str):
        self.symbol = symbol

        # 成交记录（最大保留 15 分钟）
        self._trades = _TimeWindow(900)

        # bookTicker 记录（最大保留 15 分钟）
        self._book_ticks = _TimeWindow(900)

        # 价格快照（用于计算价格变化）
        self._prices = _TimeWindow(900)

        # 订单簿深度历史（用于计算 bid_depth_03_change 和 bid_depth_collapsed）
        self._bid_depth_03_history = _TimeWindow(300)  # 5 分钟
        self._bid_depth_03_5s = _TimeWindow(5)          # 5 秒内深度

        # 点差历史（用于判断点差异常）
        self._spread_history = _TimeWindow(300)

        # 最新 bookTick
        self.latest_book_tick: Optional[BookTick] = None

        # 最新 mark price
        self.latest_mark_price: Optional[MarkPrice] = None

        # OI 历史
        self._oi_history = _TimeWindow(900)

        # 最新特征（缓存）
        self.latest_features: Optional[Features] = None

    def on_trade(self, trade: Trade) -> None:
        self._trades.append(trade, trade.timestamp)
        self._prices.append((trade.price, trade.timestamp), trade.timestamp)

    def on_book_tick(self, tick: BookTick) -> None:
        self.latest_book_tick = tick
        self._book_ticks.append(tick, tick.timestamp)
        spread = float(tick.best_ask - tick.best_bid)
        mid = (tick.best_bid + tick.best_ask) / 2
        mid_f = float(mid)
        if mid_f > 0:
            self._spread_history.append(spread / mid_f, tick.timestamp)
            # bookTicker mid price 作为价格历史（弥补 aggTrade 稀少的低流动性币）
            self._prices.append((mid, tick.timestamp), tick.timestamp)

    def on_mark_price(self, mp: MarkPrice) -> None:
        self.latest_mark_price = mp

    def on_oi(self, oi: OpenInterest) -> None:
        self._oi_history.append(oi, oi.timestamp)

    def on_depth_update(self, bid_depth_03: float) -> None:
        now = time.time()
        self._bid_depth_03_history.append(bid_depth_03, now)
        self._bid_depth_03_5s.append(bid_depth_03, now)

    def compute_features(self, book_mgr: OrderBookManager) -> Features:
        now = time.time()
        f = Features(symbol=self.symbol, timestamp=now)

        # 当前价格（优先用最新 trade，其次用 bookTick mid）
        current_price = self._get_current_price()
        f.price = current_price

        # 价格变化
        f.price_change_10s = self._price_change(current_price, 10, now)
        f.price_change_30s = self._price_change(current_price, 30, now)
        f.price_change_1m = self._price_change(current_price, 60, now)
        f.price_change_5m = self._price_change(current_price, 300, now)
        f.price_change_15m = self._price_change(current_price, 900, now)

        # 主动买入占比
        f.taker_buy_ratio_10s = self._taker_buy_ratio(10, now)
        f.taker_buy_ratio_30s = self._taker_buy_ratio(30, now)
        f.taker_buy_ratio_60s = self._taker_buy_ratio(60, now)
        f.taker_buy_ratio_300s = self._taker_buy_ratio(300, now)

        # 成交量放大倍数
        f.volume_ratio_1m, f.volume_ratio_5m = self._volume_ratios(now)

        # 订单簿深度
        book = book_mgr.get_book(self.symbol)
        if book and book.initialized:
            best_bid, best_ask = book.best_bid_ask()
            if best_bid and best_ask:
                f.best_bid = best_bid
                f.best_ask = best_ask
                f.spread = best_ask - best_bid
                mid = (best_bid + best_ask) / 2
                f.mid_price = mid
                if mid > 0:
                    f.spread_rate = float(f.spread / mid)

                bid_01, ask_01 = book.get_depth(0.001)
                bid_03, ask_03 = book.get_depth(0.003)
                bid_05, ask_05 = book.get_depth(0.005)
                bid_07, ask_07 = book.get_depth(0.007)
                f.bid_depth_01, f.ask_depth_01 = bid_01, ask_01
                f.bid_depth_03, f.ask_depth_03 = bid_03, ask_03
                f.bid_depth_05, f.ask_depth_05 = bid_05, ask_05
                f.bid_depth_07, f.ask_depth_07 = bid_07, ask_07

                if ask_03 > 0:
                    f.book_imbalance_03 = bid_03 / ask_03

                # 买盘深度变化（对比过去 5min 均值）
                self.on_depth_update(bid_03)
                avg_bid_03 = self._avg_bid_depth_03(300, now)
                if avg_bid_03 > 0:
                    f.bid_depth_03_change = (bid_03 - avg_bid_03) / avg_bid_03

                # 5s 内买盘深度是否突然消失
                f.bid_depth_collapsed = self._bid_depth_collapsed(bid_03, now)

        # 点差异常检测
        avg_spread = self._avg_spread(300, now)
        if avg_spread > 0 and f.spread_rate > 0:
            f.spread_abnormal = f.spread_rate > avg_spread * 2

        # OI
        f.open_interest, f.oi_change_5m, f.oi_change_15m = self._oi_features(now)

        # 资金费率
        if self.latest_mark_price:
            f.funding_rate = float(self.latest_mark_price.funding_rate)
            f.mark_price = self.latest_mark_price.mark_price

        # 突破信号（用滤后高点，避免当前价包含在高点计算中导致永远 False）
        f.price_30s_high = self._price_high(30, now)
        f.price_1m_high  = self._price_high(60, now)
        f.price_3m_high  = self._price_high(180, now)
        f.price_5m_low   = self._price_low(300, now)
        # 对比 2s 前的高点：当前价 > 过去 2-32s内最高价 = 真实突破
        high_30s_prev = self._price_high_before(30, now, lag=2.0)
        high_1m_prev  = self._price_high_before(60, now, lag=2.0)
        high_3m_prev  = self._price_high_before(180, now, lag=2.0)
        if current_price > 0:
            f.price_breaks_30s_high = high_30s_prev > 0 and current_price > high_30s_prev
            f.price_breaks_1m_high  = high_1m_prev  > 0 and current_price > high_1m_prev
            f.price_breaks_3m_high  = high_3m_prev  > 0 and current_price > high_3m_prev

        self.latest_features = f
        return f

    # ------------------------------------------------------------------
    # 内部计算辅助
    # ------------------------------------------------------------------

    def _get_current_price(self) -> Decimal:
        trades = self._trades.items()
        if trades:
            return trades[-1].price
        if self.latest_book_tick:
            bt = self.latest_book_tick
            return (bt.best_bid + bt.best_ask) / 2
        return Decimal(0)

    def _price_change(self, current: Decimal, window_sec: float, now: float) -> float:
        if current == 0:
            return 0.0
        cutoff = now - window_sec
        # 找最接近 cutoff 时刻的价格
        prices = list(self._prices._q)
        past_price = None
        for ts, (price, _) in reversed(prices):
            if ts <= cutoff:
                past_price = price
                break
        if past_price and past_price > 0:
            return float(current / past_price - 1)
        return 0.0

    def _price_high(self, window_sec: float, now: float) -> Decimal:
        """window_sec 秒内的最高价（包含当前价，用于显示）"""
        cutoff = now - window_sec
        highs = [
            price for ts, (price, _) in self._prices._q
            if ts >= cutoff
        ]
        return max(highs) if highs else Decimal(0)

    def _price_low(self, window_sec: float, now: float) -> Decimal:
        """window_sec 秒内的最低价（用于进场前止损锚点）"""
        cutoff = now - window_sec
        lows = [
            price for ts, (price, _) in self._prices._q
            if ts >= cutoff and price > 0
        ]
        return min(lows) if lows else Decimal(0)

    def _price_high_before(self, window_sec: float, now: float, lag: float = 2.0) -> Decimal:
        """window_sec 秒内、但排除最近 lag 秒的最高价（用于突破判断）"""
        start = now - window_sec
        end   = now - lag
        highs = [
            price for ts, (price, _) in self._prices._q
            if start <= ts < end
        ]
        return max(highs) if highs else Decimal(0)

    def _taker_buy_ratio(self, window_sec: float, now: float) -> float:
        """主买占比。窗口内有 >=2 笔真实成交时直接统计；
        否则低流动性币用 bookTicker 价格动量作代理，映射到 [0.2, 0.8]。"""
        cutoff = now - window_sec
        buy_vol = Decimal(0)
        sell_vol = Decimal(0)
        trade_count = 0
        for ts, t in self._trades._q:
            if ts < cutoff:
                continue
            trade_count += 1
            if not t.is_buyer_maker:   # buyer 是 taker = 主动买入
                buy_vol += t.qty
            else:
                sell_vol += t.qty

        # 有足够真实成交数据时直接用
        if trade_count >= 2:
            total = buy_vol + sell_vol
            return float(buy_vol / total) if total > 0 else 0.5

        # 低流动性币：用 bookTicker 价格动量作代理
        # _prices 中混合了 aggTrade + bookTicker mid，数据量充足
        window_prices = [
            (ts, p) for ts, (p, _) in self._prices._q if ts >= cutoff
        ]
        if len(window_prices) >= 2:
            oldest_p = window_prices[0][1]
            newest_p = window_prices[-1][1]
            if oldest_p > 0:
                change = float(newest_p / oldest_p - 1)
                # +0.5% → 0.70，-0.5% → 0.30；超出范围截断
                return max(0.20, min(0.80, 0.5 + change * 40.0))

        return 0.5  # 完全无数据，返回中性值

    def _volume_ratios(self, now: float) -> Tuple[float, float]:
        """计算 1m / 5m 成交量放大倍数"""
        cutoff_1m = now - 60
        cutoff_5m = now - 300
        cutoff_30m = now - 1800
        cutoff_1h = now - 3600

        vol_1m = sum(float(t.qty * t.price) for ts, t in self._trades._q if ts >= cutoff_1m)
        vol_5m = sum(float(t.qty * t.price) for ts, t in self._trades._q if ts >= cutoff_5m)

        # 过去 30min 的 1m 均值
        past_1m_windows = []
        for i in range(30):
            wstart = now - 60 * (i + 2)
            wend = now - 60 * (i + 1)
            wvol = sum(float(t.qty * t.price) for ts, t in self._trades._q if wstart <= ts < wend)
            past_1m_windows.append(wvol)
        avg_1m = sum(past_1m_windows) / len(past_1m_windows) if past_1m_windows else 0

        # 过去 1h 的 5m 均值
        past_5m_windows = []
        for i in range(12):
            wstart = now - 300 * (i + 2)
            wend = now - 300 * (i + 1)
            wvol = sum(float(t.qty * t.price) for ts, t in self._trades._q if wstart <= ts < wend)
            past_5m_windows.append(wvol)
        avg_5m = sum(past_5m_windows) / len(past_5m_windows) if past_5m_windows else 0

        ratio_1m = vol_1m / avg_1m if avg_1m > 0 else 1.0
        ratio_5m = vol_5m / avg_5m if avg_5m > 0 else 1.0
        return ratio_1m, ratio_5m

    def _oi_features(self, now: float):
        items = list(self._oi_history._q)
        if not items:
            return Decimal(0), 0.0, 0.0
        current_oi = items[-1][1].open_interest

        def oi_at(sec_ago: float):
            cutoff = now - sec_ago
            for ts, oi in reversed(items):
                if ts <= cutoff:
                    return oi.open_interest
            return None

        oi_5m = oi_at(300)
        oi_15m = oi_at(900)

        change_5m = float(current_oi / oi_5m - 1) if oi_5m and oi_5m > 0 else 0.0
        change_15m = float(current_oi / oi_15m - 1) if oi_15m and oi_15m > 0 else 0.0
        return current_oi, change_5m, change_15m

    def _avg_bid_depth_03(self, window_sec: float, now: float) -> float:
        cutoff = now - window_sec
        vals = [v for ts, v in self._bid_depth_03_history._q if ts >= cutoff]
        return sum(vals) / len(vals) if vals else 0.0

    def _bid_depth_collapsed(self, current: float, now: float) -> bool:
        """5s 内买盘深度下降超过 80%"""
        vals = [v for _, v in self._bid_depth_03_5s._q]
        if not vals or current == 0:
            return False
        peak = max(vals)
        if peak == 0:
            return False
        return (peak - current) / peak > 0.80

    def _avg_spread(self, window_sec: float, now: float) -> float:
        cutoff = now - window_sec
        vals = [v for ts, v in self._spread_history._q if ts >= cutoff]
        return sum(vals) / len(vals) if vals else 0.0


class FeatureEngine:
    """所有 symbol 的特征引擎"""

    def __init__(self, symbols: list, book_mgr: OrderBookManager):
        self._states: Dict[str, SymbolFeatureState] = {
            sym: SymbolFeatureState(sym) for sym in symbols
        }
        self._book_mgr = book_mgr

    async def handle_trade(self, symbol: str, event: dict) -> None:
        state = self._states.get(symbol)
        if not state:
            return
        trade = Trade(
            symbol=symbol,
            price=Decimal(event["p"]),
            qty=Decimal(event["q"]),
            is_buyer_maker=event["m"],
            trade_id=event["a"],
            timestamp=event["T"] / 1000.0,
        )
        state.on_trade(trade)

    async def handle_book_ticker(self, symbol: str, event: dict) -> None:
        state = self._states.get(symbol)
        if not state:
            return
        ts = event.get("T", event.get("E", 0)) / 1000.0
        tick = BookTick(
            symbol=symbol,
            best_bid=Decimal(event["b"]),
            best_bid_qty=Decimal(event["B"]),
            best_ask=Decimal(event["a"]),
            best_ask_qty=Decimal(event["A"]),
            timestamp=ts,
        )
        state.on_book_tick(tick)

    async def handle_mark_price(self, symbol: str, event: dict) -> None:
        state = self._states.get(symbol)
        if not state:
            return
        mp = MarkPrice(
            symbol=symbol,
            mark_price=Decimal(event["p"]),
            index_price=Decimal(event.get("i", event["p"])),
            funding_rate=Decimal(event.get("r", "0")),
            next_funding_time=event.get("T", 0),
            timestamp=event["E"] / 1000.0,
        )
        state.on_mark_price(mp)

    def on_oi_update(self, oi: OpenInterest) -> None:
        state = self._states.get(oi.symbol)
        if state:
            state.on_oi(oi)

    def on_mark_price_update(self, mp: MarkPrice) -> None:
        state = self._states.get(mp.symbol)
        if state:
            state.on_mark_price(mp)

    def get_features(self, symbol: str) -> Optional[Features]:
        state = self._states.get(symbol)
        if not state:
            return None
        return state.compute_features(self._book_mgr)

    def get_all_features(self) -> Dict[str, Features]:
        return {sym: self.get_features(sym) for sym in self._states}
