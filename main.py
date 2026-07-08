"""
主入口 —— 组装所有服务，启动异步主循环
"""
import asyncio
import logging
import os
import sys
import time

import aiohttp
import uvicorn

# 确保从 binance-ambush/ 目录运行
sys.path.insert(0, os.path.dirname(__file__))

from core.config import load_config
from dashboard.app import app as dashboard_app, init_dashboard
from dashboard.state import SharedState
from services.alerter import Alerter
from services.execution import ExecutionEngine
from services.feature_engine import FeatureEngine
from services.market_data import MarketDataService
from services.oi_poller import OIPoller
from services.orderbook import OrderBookManager
from services.recorder import Recorder
from services.risk_engine import RiskEngine
from services.signal_engine import SignalEngine, calc_opportunity_score
from services.symbol_service import fetch_symbol_info

# ---- 日志配置 ----
def setup_logging(level: str, log_file: str) -> None:
    os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )


# ---- 主循环 ----
EVAL_INTERVAL = 0.5   # 每 0.5 秒评估一次所有 symbol 的信号
DASHBOARD_PORT = 8080


def _pos_to_dict(pos) -> dict:
    return {
        "symbol": pos.symbol,
        "side": pos.side,
        "qty": str(pos.qty),
        "entry_price": str(pos.entry_price),
        "mark_price": str(pos.mark_price),
        "unrealized_pnl_pct": pos.unrealized_pnl_pct,
        "max_profit_pct": pos.max_profit_pct,
        "max_loss_pct": pos.max_loss_pct,
        "opened_at": pos.opened_at,
        "signal_id": pos.signal_id,
        "status": pos.status,
    }


async def main_loop(
    symbols: list,
    feature_engine: FeatureEngine,
    signal_engine: SignalEngine,
    risk_engine: RiskEngine,
    execution_engine: ExecutionEngine,
    alerter: Alerter,
    recorder: Recorder,
    ws_service: MarketDataService,
    shared_state: SharedState,
) -> None:
    logger = logging.getLogger("main_loop")
    logger.info(f"Main loop started, evaluating {len(symbols)} symbols every {EVAL_INTERVAL}s")

    features_write_counter = {}
    _last_signal_alert = {}   # sym -> 上次TG推送时间戳，60s内不重复推送
    FEATURES_WRITE_EVERY = 20

    while True:
        await asyncio.sleep(EVAL_INTERVAL)

        ws_stats = ws_service.stats()
        ws_ok, ws_reason = risk_engine.is_ws_healthy(ws_stats["lag_seconds"])
        if not ws_ok:
            logger.warning(f"WS unhealthy: {ws_reason} — skipping signal eval")
            shared_state.ws_stats = ws_stats
            continue

        positions_snap = {}

        for sym in symbols:
            try:
                f = feature_engine.get_features(sym)
                if f is None or f.price == 0:
                    continue

                # 计算评分
                scores = calc_opportunity_score(f)

                # 更新 SharedState
                shared_state.features[sym] = f
                shared_state.scores[sym] = scores

                # 定期写特征快照到 DB
                cnt = features_write_counter.get(sym, 0) + 1
                features_write_counter[sym] = cnt
                if cnt % FEATURES_WRITE_EVERY == 0:
                    asyncio.create_task(recorder.save_features(f))

                # 风控检查
                allowed, reason = risk_engine.can_enter(sym, f)

                # 信号评估
                signal = signal_engine.evaluate(f) if allowed else None

                # 发出预警（节流：同标的未持仓且 60s 内不重复推送）
                if signal and signal.is_valid:
                    not_in_pos = sym not in execution_engine.positions
                    not_recently = time.time() - _last_signal_alert.get(sym, 0) > 60
                    if not_in_pos and not_recently:
                        _last_signal_alert[sym] = time.time()
                        asyncio.create_task(alerter.on_signal(signal))
                    shared_state.add_signal({
                        "symbol": signal.symbol,
                        "timestamp": signal.timestamp,
                        "score": signal.opportunity_score,
                        "type": signal.signal_type,
                        "price": str(signal.entry_price),
                    })

                # 执行（paper/live）
                await execution_engine.on_features(f, signal)

            except Exception as e:
                logger.error(f"[{sym}] eval error: {e}", exc_info=True)

        # 更新 SharedState 持仓和风控状态
        for sym, pos in execution_engine.positions.items():
            positions_snap[sym] = _pos_to_dict(pos)

        risk_status = risk_engine.status()
        shared_state.positions = positions_snap
        shared_state.ws_stats = ws_stats
        shared_state.risk_status = risk_status


async def run() -> None:
    cfg = load_config(os.path.join(os.path.dirname(__file__), "config.yaml"))
    setup_logging(cfg.log_level, cfg.log_file)
    logger = logging.getLogger("startup")

    logger.info("=" * 60)
    logger.info("Binance Ambush System starting")
    logger.info(f"Mode: {cfg.execution.mode}")
    logger.info(f"Symbols: {cfg.symbols}")
    logger.info("=" * 60)

    # ---- HTTP session ----
    # 检测代理（US 服务器需要通过 Clash/Mihomo 绕过地理限制）
    _proxy = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY") or os.environ.get("ALL_PROXY")
    if _proxy:
        logger.info(f"Using HTTP proxy: {_proxy}")
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector, trust_env=True) as session:

        # ---- 验证 symbol ----
        valid_symbols_info = await fetch_symbol_info(
            cfg.binance.base_url, cfg.symbols, session
        )
        active_symbols = list(valid_symbols_info.keys())
        if not active_symbols:
            logger.error("No valid symbols found on Binance Futures. Check config.yaml.")
            return
        logger.info(f"Active symbols ({len(active_symbols)}): {active_symbols}")

        # ---- SharedState ----
        shared_state = SharedState(active_symbols, cfg.execution.mode)

        # ---- 组装服务 ----
        book_mgr = OrderBookManager(
            active_symbols,
            cfg.binance.base_url,
            cfg.binance.depth_snapshot_limit,
        )
        feature_engine = FeatureEngine(active_symbols, book_mgr)
        alerter = Alerter(
            cfg.telegram.enabled,
            cfg.telegram.token,
            cfg.telegram.chat_id,
        )
        alerter.set_session(session)

        recorder = Recorder(cfg.db_path)
        await recorder.start()

        risk_engine = RiskEngine(cfg.risk)
        signal_engine = SignalEngine(cfg.strategy)
        execution_engine = ExecutionEngine(
            cfg.execution,
            cfg.strategy,
            alerter=alerter,
            recorder=recorder,
            risk_engine=risk_engine,
            signal_engine=signal_engine,
        )

        # 实盘上下文注入（paper 模式下 api_key 为空，set_live_context 仍然执行但不使用）
        execution_engine.set_live_context(
            session=session,
            symbol_info=valid_symbols_info,
            api_key=cfg.binance.api_key,
            api_secret=cfg.binance.api_secret,
            base_url=cfg.binance.base_url,
            live_leverage=cfg.binance.live_leverage,
        )

        ws_service = MarketDataService(
            active_symbols,
            cfg.binance.ws_url,
            book_mgr,
            feature_engine,
            alerter,
            session=session,
            proxy=_proxy,
        )

        oi_poller = OIPoller(
            active_symbols,
            cfg.binance.base_url,
            feature_engine,
            cfg.binance.oi_poll_interval,
        )

        # ---- Dashboard ----
        init_dashboard(shared_state, cfg.db_path)
        uvi_config = uvicorn.Config(
            dashboard_app,
            host="0.0.0.0",
            port=DASHBOARD_PORT,
            log_level="warning",
        )
        uvi_server = uvicorn.Server(uvi_config)

        # ---- 实盘账户初始化（live 模式下设置杠杆、获取实际权益）----
        await execution_engine.setup_live_account()

        # ---- 正确启动顺序：先启 WS 开始缓冲事件，再拉 REST 快照 ----
        # 1. 启动 WS（订单簿进入缓冲状态）
        ws_task = asyncio.create_task(ws_service.start(), name="ws")
        logger.info("Waiting for WS to connect...")
        await ws_service.wait_connected()

        # 2. WS 已连接并缓冲事件，现在拉快照（lastUpdateId 落在缓冲区内）
        logger.info("Fetching orderbook snapshots (WS is buffering)...")
        await book_mgr.start(session)
        logger.info("Orderbook snapshots ready")

        # ---- 并发启动其余任务（ws_task 已经在跑） ----
        tasks = [
            ws_task,
            asyncio.create_task(oi_poller.start(session), name="oi_poller"),
            asyncio.create_task(
                main_loop(
                    active_symbols, feature_engine, signal_engine,
                    risk_engine, execution_engine, alerter, recorder,
                    ws_service, shared_state,
                ),
                name="main_loop",
            ),
            asyncio.create_task(uvi_server.serve(), name="dashboard"),
        ]

        logger.info(f"All services started.")
        logger.info(f"📊 Dashboard: http://localhost:{DASHBOARD_PORT}")
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            for t in tasks:
                t.cancel()
            await recorder.stop()
            logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(run())
