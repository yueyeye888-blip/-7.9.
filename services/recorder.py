"""
数据记录 —— SQLite（aiosqlite），schema 与文档设计一致
"""
import asyncio
import json
import logging
import time
import uuid
from decimal import Decimal
from typing import Optional

import aiosqlite

from core.types import Position, Signal

logger = logging.getLogger(__name__)


class Recorder:

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def start(self) -> None:
        import os
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._create_tables()
        logger.info(f"DB ready: {self._db_path}")

    async def stop(self) -> None:
        if self._db:
            await self._db.close()

    async def save_signal(self, signal: Signal) -> None:
        sql = """
        INSERT OR IGNORE INTO signals
          (id, time, symbol, signal_type, opportunity_score,
           entry_price, breakout_price, reason, status)
        VALUES (?,?,?,?,?,?,?,?,?)
        """
        await self._db.execute(sql, (
            str(uuid.uuid4()),
            signal.timestamp,
            signal.symbol,
            signal.signal_type,
            signal.opportunity_score,
            str(signal.entry_price),
            str(signal.breakout_price),
            json.dumps(signal.reason, default=str),
            "OPEN",
        ))
        await self._db.commit()

    async def save_position_open(self, pos: Position) -> None:
        sql = """
        INSERT OR IGNORE INTO positions
          (id, symbol, side, qty, entry_price, breakout_price,
           entry_bid_depth_03, signal_id, status, opened_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """
        await self._db.execute(sql, (
            pos.signal_id,
            pos.symbol,
            pos.side,
            str(pos.qty),
            str(pos.entry_price),
            str(pos.breakout_price),
            pos.entry_bid_depth_03,
            pos.signal_id,
            "HOLDING",
            pos.opened_at,
        ))
        await self._db.commit()

    async def save_position_close(
        self, pos: Position, exit_price: Decimal, reason: str
    ) -> None:
        sql = """
        UPDATE positions
        SET exit_price=?, pnl_pct=?, max_profit_pct=?, max_loss_pct=?,
            holding_seconds=?, exit_reason=?, status=?, closed_at=?
        WHERE id=?
        """
        holding = time.time() - pos.opened_at
        await self._db.execute(sql, (
            str(exit_price),
            pos.unrealized_pnl_pct,
            pos.max_profit_pct,
            pos.max_loss_pct,
            holding,
            reason,
            "CLOSED",
            time.time(),
            pos.signal_id,
        ))
        await self._db.commit()

    async def save_features(self, f) -> None:
        """可选：定期写入特征快照用于复盘"""
        sql = """
        INSERT OR IGNORE INTO features_snapshot
          (time, symbol, price, oi_change_5m, oi_change_15m,
           taker_buy_ratio_10s, taker_buy_ratio_30s,
           book_imbalance_03, bid_depth_03_change,
           spread_rate, funding_rate, opportunity_score)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """
        score = 0.0
        try:
            from services.signal_engine import calc_opportunity_score
            scores = calc_opportunity_score(f)
            score = scores.get("total", 0.0)
        except Exception:
            pass

        await self._db.execute(sql, (
            f.timestamp, f.symbol, str(f.price),
            f.oi_change_5m, f.oi_change_15m,
            f.taker_buy_ratio_10s, f.taker_buy_ratio_30s,
            f.book_imbalance_03, f.bid_depth_03_change,
            f.spread_rate, f.funding_rate, score,
        ))
        await self._db.commit()

    # ------------------------------------------------------------------

    async def _create_tables(self) -> None:
        await self._db.executescript("""
        CREATE TABLE IF NOT EXISTS signals (
            id TEXT PRIMARY KEY,
            time REAL,
            symbol TEXT,
            signal_type TEXT,
            opportunity_score REAL,
            entry_price TEXT,
            breakout_price TEXT,
            reason TEXT,
            status TEXT,
            future_return_30s REAL,
            future_return_1m REAL,
            future_return_5m REAL,
            created_at REAL DEFAULT (unixepoch())
        );

        CREATE TABLE IF NOT EXISTS positions (
            id TEXT PRIMARY KEY,
            symbol TEXT,
            side TEXT,
            qty TEXT,
            entry_price TEXT,
            exit_price TEXT,
            breakout_price TEXT,
            entry_bid_depth_03 REAL,
            signal_id TEXT,
            pnl_pct REAL,
            max_profit_pct REAL,
            max_loss_pct REAL,
            holding_seconds REAL,
            exit_reason TEXT,
            status TEXT,
            opened_at REAL,
            closed_at REAL
        );

        CREATE TABLE IF NOT EXISTS features_snapshot (
            time REAL,
            symbol TEXT,
            price TEXT,
            oi_change_5m REAL,
            oi_change_15m REAL,
            taker_buy_ratio_10s REAL,
            taker_buy_ratio_30s REAL,
            book_imbalance_03 REAL,
            bid_depth_03_change REAL,
            spread_rate REAL,
            funding_rate REAL,
            opportunity_score REAL,
            PRIMARY KEY (time, symbol)
        );

        CREATE TABLE IF NOT EXISTS risk_events (
            id TEXT PRIMARY KEY DEFAULT (hex(randomblob(8))),
            time REAL,
            symbol TEXT,
            risk_type TEXT,
            message TEXT,
            created_at REAL DEFAULT (unixepoch())
        );
        """)
        await self._db.commit()
