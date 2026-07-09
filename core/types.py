from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional


@dataclass
class Trade:
    symbol: str
    price: Decimal
    qty: Decimal
    is_buyer_maker: bool   # True = 主动卖出打买盘；False = 主动买入打卖盘
    trade_id: int
    timestamp: float       # unix seconds


@dataclass
class BookTick:
    symbol: str
    best_bid: Decimal
    best_bid_qty: Decimal
    best_ask: Decimal
    best_ask_qty: Decimal
    timestamp: float


@dataclass
class MarkPrice:
    symbol: str
    mark_price: Decimal
    index_price: Decimal
    funding_rate: Decimal
    next_funding_time: int
    timestamp: float


@dataclass
class OpenInterest:
    symbol: str
    open_interest: Decimal
    timestamp: float


@dataclass
class Liquidation:
    symbol: str
    side: str
    qty: Decimal
    price: Decimal
    timestamp: float


@dataclass
class Features:
    symbol: str
    timestamp: float

    # 当前价格
    price: Decimal = Decimal(0)

    # 价格变化
    price_change_10s: float = 0.0
    price_change_30s: float = 0.0
    price_change_1m: float = 0.0
    price_change_5m: float = 0.0
    price_change_15m: float = 0.0

    # 成交量放大倍数
    volume_ratio_1m: float = 1.0
    volume_ratio_5m: float = 1.0

    # 主动买入占比（不同窗口）
    taker_buy_ratio_10s: float = 0.5
    taker_buy_ratio_30s: float = 0.5
    taker_buy_ratio_60s: float = 0.5
    taker_buy_ratio_300s: float = 0.5

    # 盘口
    best_bid: Decimal = Decimal(0)
    best_ask: Decimal = Decimal(0)
    spread: Decimal = Decimal(0)
    spread_rate: float = 0.0
    mid_price: Decimal = Decimal(0)

    # 订单簿深度（USDT 价值）
    bid_depth_01: float = 0.0
    ask_depth_01: float = 0.0
    bid_depth_03: float = 0.0
    ask_depth_03: float = 0.0
    bid_depth_05: float = 0.0
    ask_depth_05: float = 0.0
    bid_depth_07: float = 0.0
    ask_depth_07: float = 0.0

    # 盘口失衡
    book_imbalance_03: float = 1.0

    # 买盘深度变化（对比过去 5min 均值）
    bid_depth_03_change: float = 0.0

    # OI
    open_interest: Decimal = Decimal(0)
    oi_change_5m: float = 0.0
    oi_change_15m: float = 0.0

    # 资金费率
    funding_rate: float = 0.0
    mark_price: Decimal = Decimal(0)

    # 突破信号
    price_breaks_30s_high: bool = False
    price_breaks_1m_high: bool = False
    price_breaks_3m_high: bool = False
    price_30s_high: Decimal = Decimal(0)
    price_1m_high: Decimal = Decimal(0)
    price_3m_high: Decimal = Decimal(0)
    price_5m_low: Decimal = Decimal(0)    # 进场前 5 分钟最低价（止损锚点）

    # 点差异常（突然扩大超过过去 5min 均值 2 倍）
    spread_abnormal: bool = False

    # 盘口深度突然消失（5s 内下降 > 80%）
    bid_depth_collapsed: bool = False


@dataclass
class Signal:
    symbol: str
    timestamp: float
    is_valid: bool
    signal_type: str = ""
    opportunity_score: float = 0.0
    entry_price: Decimal = Decimal(0)
    breakout_price: Decimal = Decimal(0)
    reason: dict = field(default_factory=dict)
    features_snapshot: Optional["Features"] = None


@dataclass
class Position:
    symbol: str
    side: str                        # LONG
    qty: Decimal
    entry_price: Decimal
    entry_bid_depth_03: float
    entry_bid_depth_07: float          # 进场时0.7%深度，用于深度崩塌检测
    breakout_price: Decimal
    opened_at: float                 # unix seconds
    signal_id: str

    mark_price: Decimal = Decimal(0)
    unrealized_pnl_pct: float = 0.0
    max_profit_pct: float = 0.0
    max_loss_pct: float = 0.0

    # 止损价（开仓时从进场前 5 分钟最低价锚定）
    sl_price: Decimal = Decimal(0)

    # 已减仓比例
    reduced_pct: float = 0.0

    # 平仓诊断数据（用于 Dashboard 可解释性展示）
    exit_diag: dict = field(default_factory=dict)

    status: str = "HOLDING"          # HOLDING | EXITING | CLOSED
