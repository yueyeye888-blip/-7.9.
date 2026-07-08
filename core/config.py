import yaml
from dataclasses import dataclass
from typing import List


@dataclass
class StrategyConfig:
    min_score: float
    oi_change_5m_min: float
    oi_change_15m_min: float
    price_change_5m_max: float
    volume_ratio_5m_min: float
    taker_buy_ratio_10s_min: float
    taker_buy_ratio_30s_min: float
    bid_depth_03_change_min: float
    book_imbalance_03_min: float
    spread_rate_max: float
    max_holding_seconds: int


@dataclass
class RiskConfig:
    max_single_trade_loss_equity_pct: float
    max_symbol_loss_equity_pct: float
    max_daily_loss_equity_pct: float
    max_position_equity_pct: float
    pause_symbol_after_losses: int
    pause_system_after_losses: int


@dataclass
class ExecutionConfig:
    mode: str
    paper_account_equity: float
    first_entry_pct: float
    second_entry_pct: float
    confirm_seconds: int
    no_profit_exit_seconds: int
    reduce_after_no_profit_seconds: int


@dataclass
class BinanceConfig:
    base_url: str
    ws_url: str
    oi_poll_interval: int
    depth_snapshot_limit: int
    api_key: str = ""
    api_secret: str = ""
    live_leverage: int = 10


@dataclass
class TelegramConfig:
    enabled: bool
    token: str
    chat_id: str


@dataclass
class AppConfig:
    symbols: List[str]
    strategy: StrategyConfig
    risk: RiskConfig
    execution: ExecutionConfig
    binance: BinanceConfig
    telegram: TelegramConfig
    db_path: str
    log_level: str
    log_file: str


def load_config(path: str = "config.yaml") -> AppConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    s = raw["strategy"]
    r = raw["risk"]
    e = raw["execution"]
    b = raw["binance"]
    t = raw.get("telegram", {})

    return AppConfig(
        symbols=[sym.upper() for sym in raw["symbols"]],
        strategy=StrategyConfig(
            min_score=s["min_score"],
            oi_change_5m_min=s["oi_change_5m_min"],
            oi_change_15m_min=s["oi_change_15m_min"],
            price_change_5m_max=s["price_change_5m_max"],
            volume_ratio_5m_min=s["volume_ratio_5m_min"],
            taker_buy_ratio_10s_min=s["taker_buy_ratio_10s_min"],
            taker_buy_ratio_30s_min=s["taker_buy_ratio_30s_min"],
            bid_depth_03_change_min=s["bid_depth_03_change_min"],
            book_imbalance_03_min=s["book_imbalance_03_min"],
            spread_rate_max=s["spread_rate_max"],
            max_holding_seconds=s["max_holding_seconds"],
        ),
        risk=RiskConfig(
            max_single_trade_loss_equity_pct=r["max_single_trade_loss_equity_pct"],
            max_symbol_loss_equity_pct=r["max_symbol_loss_equity_pct"],
            max_daily_loss_equity_pct=r["max_daily_loss_equity_pct"],
            max_position_equity_pct=r["max_position_equity_pct"],
            pause_symbol_after_losses=r["pause_symbol_after_losses"],
            pause_system_after_losses=r["pause_system_after_losses"],
        ),
        execution=ExecutionConfig(
            mode=e["mode"],
            paper_account_equity=e["paper_account_equity"],
            first_entry_pct=e["first_entry_pct"],
            second_entry_pct=e["second_entry_pct"],
            confirm_seconds=e["confirm_seconds"],
            no_profit_exit_seconds=e["no_profit_exit_seconds"],
            reduce_after_no_profit_seconds=e["reduce_after_no_profit_seconds"],
        ),
        binance=BinanceConfig(
            base_url=b["base_url"],
            ws_url=b["ws_url"],
            oi_poll_interval=b["oi_poll_interval"],
            depth_snapshot_limit=b["depth_snapshot_limit"],
            api_key=b.get("api_key", ""),
            api_secret=b.get("api_secret", ""),
            live_leverage=b.get("live_leverage", 10),
        ),
        telegram=TelegramConfig(
            enabled=t.get("enabled", False),
            token=t.get("token", ""),
            chat_id=t.get("chat_id", ""),
        ),
        db_path=raw["database"]["path"],
        log_level=raw["logging"]["level"],
        log_file=raw["logging"]["file"],
    )
