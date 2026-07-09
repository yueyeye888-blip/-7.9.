"""
Dashboard HTTP + WebSocket 服务（FastAPI）
每秒向前端推送全量快照，前端实时渲染
"""
import asyncio
import json
import time
from pathlib import Path
from typing import Optional

import aiosqlite
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from dashboard.state import SharedState

app = FastAPI(title="Ambush Dashboard", docs_url=None, redoc_url=None)

_state: Optional[SharedState] = None
_db_path: str = "data/ambush.db"


def init_dashboard(state: SharedState, db_path: str) -> None:
    global _state, _db_path
    _state = state
    _db_path = db_path


# ── 页面 ──────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    html = (Path(__file__).parent / "static" / "index.html").read_text("utf-8")
    return HTMLResponse(html)


# ── REST API ──────────────────────────────────────────────────────────

@app.get("/api/status")
async def get_status():
    if not _state:
        return {"error": "not ready"}
    return {
        "mode": _state.system_mode,
        "symbols": _state.symbols,
        "started_at": _state.started_at,
        "ws": _state.ws_stats,
        "risk": _state.risk_status,
        "signal_stats": _state.signal_stats,
    }


@app.get("/api/opportunities")
async def get_opportunities():
    if not _state:
        return []
    return _build_opp_list()


@app.get("/api/symbols/{symbol}")
async def get_symbol(symbol: str):
    if not _state:
        return {}
    sym = symbol.upper()
    f = _state.features.get(sym)
    if not f:
        return {}
    return {
        "features": {
            "price": str(f.price),
            "price_change_1m": f.price_change_1m,
            "price_change_5m": f.price_change_5m,
            "oi_change_5m": f.oi_change_5m,
            "oi_change_15m": f.oi_change_15m,
            "taker_buy_ratio_10s": f.taker_buy_ratio_10s,
            "taker_buy_ratio_30s": f.taker_buy_ratio_30s,
            "book_imbalance_03": f.book_imbalance_03,
            "bid_depth_03_change": f.bid_depth_03_change,
            "spread_rate": f.spread_rate,
            "funding_rate": f.funding_rate,
            "price_breaks_1m_high": f.price_breaks_1m_high,
            "mark_price": str(f.mark_price),
            "open_interest": str(f.open_interest),
        },
        "score": _state.scores.get(sym, {}),
    }


@app.get("/api/positions")
async def get_positions():
    if not _state:
        return []
    return list(_state.positions.values())


@app.get("/api/signals")
async def get_signals(limit: int = 50):
    if not _state:
        return []
    return _state.recent_signals[:limit]


@app.get("/api/trades")
async def get_trades(limit: int = 50):
    """从 SQLite 读取历史已平仓记录"""
    try:
        async with aiosqlite.connect(_db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM positions WHERE status='CLOSED' ORDER BY closed_at DESC LIMIT ?",
                (limit,)
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


# ── WebSocket 实时推送 ────────────────────────────────────────────────

@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            snap = _build_snapshot()
            await websocket.send_json(snap)
            await asyncio.sleep(1.0)
    except (WebSocketDisconnect, Exception):
        pass


# ── 内部构建函数 ──────────────────────────────────────────────────────

def _build_opp_list() -> list:
    result = []
    for sym in _state.symbols:
        f = _state.features.get(sym)
        s = _state.scores.get(sym, {})
        if not f:
            continue
        result.append({
            "sym": sym,
            "price": str(f.price),
            "p1m": round(f.price_change_1m * 100, 2),
            "p5m": round(f.price_change_5m * 100, 2),
            "oi5": round(f.oi_change_5m * 100, 2),
            "oi15": round(f.oi_change_15m * 100, 2),
            "tb10": round(f.taker_buy_ratio_10s * 100, 1),
            "tb30": round(f.taker_buy_ratio_30s * 100, 1),
            "bi": round(f.book_imbalance_03, 3),
            "sp": round(f.spread_rate * 10000, 2),    # bps
            "fr": round(f.funding_rate * 10000, 3),   # bps
            "sc": round(s.get("total", 0), 1),
            "pos": sym in _state.positions,
            "sig": round(s.get("total", 0), 1) >= 75,
        })
    result.sort(key=lambda x: x["sc"], reverse=True)
    return result


def _build_snapshot() -> dict:
    if not _state:
        return {"type": "snapshot", "opportunities": [], "positions": [],
                "risk": {}, "ws": {}, "recent_signals": [], "ts": time.time()}
    risk = dict(_state.risk_status)
    risk["mode"] = _state.system_mode
    return {
        "type": "snapshot",
        "ts": time.time(),
        "started_at": _state.started_at,
        "mode": _state.system_mode,
        "opportunities": _build_opp_list(),
        "positions": list(_state.positions.values()),
        "risk": risk,
        "ws": _state.ws_stats,
        "signal_stats": _state.signal_stats,
        "recent_signals": _state.recent_signals[:30],
    }
