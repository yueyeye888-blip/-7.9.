import argparse
import sqlite3


def pct(value: float) -> float:
    return value * 100.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Fee-adjusted replay for a time window")
    parser.add_argument("--db", default="data/ambush.db", help="sqlite database path")
    parser.add_argument("--since", type=float, required=True, help="unix timestamp lower bound")
    parser.add_argument("--until", type=float, default=None, help="unix timestamp upper bound")
    parser.add_argument("--fee-rt", type=float, default=0.0010, help="round-trip fee ratio")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    until = args.until if args.until is not None else 10**18
    rows = conn.execute(
        """
        SELECT symbol, pnl_pct, holding_seconds, exit_reason, closed_at
        FROM positions
        WHERE status='CLOSED' AND pnl_pct IS NOT NULL
          AND closed_at >= ? AND closed_at <= ?
        ORDER BY closed_at
        """,
        (args.since, until),
    ).fetchall()

    print("=== Fee Window Report ===")
    print(f"since={args.since} until={until}")
    print(f"fee_round_trip={pct(args.fee_rt):.2f}%")
    print(f"trades={len(rows)}")

    if not rows:
        conn.close()
        return

    net = [float(row["pnl_pct"]) - args.fee_rt for row in rows]
    gross = [float(row["pnl_pct"]) for row in rows]
    hold = [float(row["holding_seconds"] or 0.0) for row in rows]

    win_rate = sum(1 for value in net if value > 0) / len(net)
    gross_avg = sum(gross) / len(gross)
    net_avg = sum(net) / len(net)
    avg_hold = sum(hold) / len(hold)

    print(f"gross_avg={pct(gross_avg):+.4f}%")
    print(f"net_avg={pct(net_avg):+.4f}%")
    print(f"net_win_rate={pct(win_rate):.2f}%")
    print(f"avg_hold_sec={avg_hold:.1f}")

    reason_count: dict[str, int] = {}
    for row in rows:
        reason = row["exit_reason"] or "UNKNOWN"
        reason_count[reason] = reason_count.get(reason, 0) + 1

    print("top_reasons=")
    for reason, count in sorted(reason_count.items(), key=lambda item: item[1], reverse=True)[:6]:
        print(f"  {reason}: {count}")

    conn.close()


if __name__ == "__main__":
    main()
