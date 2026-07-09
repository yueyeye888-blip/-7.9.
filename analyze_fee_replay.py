import argparse
import sqlite3
from collections import defaultdict


def _pct(value: float) -> float:
    return value * 100.0


def _summary(name: str, values: list[float]) -> None:
    if not values:
        print(f"{name}: n=0")
        return
    win_rate = sum(1 for item in values if item > 0) / len(values)
    avg = sum(values) / len(values)
    print(
        f"{name}: n={len(values)} avg_net={_pct(avg):+.4f}% "
        f"win_rate={_pct(win_rate):.2f}%"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fee-adjusted replay report")
    parser.add_argument("--db", default="data/ambush.db", help="sqlite database path")
    parser.add_argument(
        "--fee-rt",
        type=float,
        default=0.0010,
        help="round-trip fee ratio, e.g. 0.001 = 0.10%",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT symbol, pnl_pct, holding_seconds, exit_reason, opened_at, closed_at
        FROM positions
        WHERE status='CLOSED' AND pnl_pct IS NOT NULL
        ORDER BY closed_at
        """
    ).fetchall()

    if not rows:
        print("No closed positions found.")
        conn.close()
        return

    fee = args.fee_rt
    gross = [float(row["pnl_pct"]) for row in rows]
    net = [item - fee for item in gross]

    start_ts = float(rows[0]["closed_at"] or 0)
    end_ts = float(rows[-1]["closed_at"] or 0)
    span_days = max((end_ts - start_ts) / 86400.0, 1e-9)

    print("=== Fee Replay Report ===")
    print(f"trades={len(rows)} span_days={span_days:.2f} trades_per_day={len(rows)/span_days:.2f}")
    print(f"fee_round_trip={_pct(fee):.2f}%")
    print(f"gross_avg={_pct(sum(gross)/len(gross)):+.4f}%")
    _summary("all(net)", net)

    print("\n=== Recent Windows ===")
    _summary("last20", [float(row["pnl_pct"]) - fee for row in rows[-20:]])
    _summary("last40", [float(row["pnl_pct"]) - fee for row in rows[-40:]])

    print("\n=== Exit Reason ===")
    by_reason: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        reason = row["exit_reason"] or "UNKNOWN"
        by_reason[reason].append(float(row["pnl_pct"]) - fee)
    for reason, values in sorted(by_reason.items(), key=lambda item: len(item[1]), reverse=True):
        _summary(reason, values)

    print("\n=== Holding Bucket ===")
    buckets = [(0, 20), (20, 40), (40, 80), (80, 180), (180, 1_000_000_000)]
    for low, high in buckets:
        values = [
            float(row["pnl_pct"]) - fee
            for row in rows
            if low <= float(row["holding_seconds"] or 0.0) < high
        ]
        _summary(f"{low}-{high}s", values)

    print("\n=== Symbol (min_n=8) ===")
    by_symbol: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        by_symbol[row["symbol"]].append(float(row["pnl_pct"]) - fee)

    ranked = sorted(by_symbol.items(), key=lambda item: sum(item[1]), reverse=True)
    for symbol, values in ranked:
        if len(values) < 8:
            continue
        win_rate = sum(1 for item in values if item > 0) / len(values)
        avg = sum(values) / len(values)
        net_sum = sum(values)
        print(
            f"{symbol}: n={len(values)} avg_net={_pct(avg):+.4f}% "
            f"win_rate={_pct(win_rate):.2f}% sum_net={_pct(net_sum):+.3f}%"
        )

    conn.close()


if __name__ == "__main__":
    main()
