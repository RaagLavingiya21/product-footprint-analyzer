"""Print a summary table of LLM call logs from db/footprints.db.

Usage:
    python -m observability.viewer
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "db" / "footprints.db"

# claude-sonnet-4-6 pricing per 1M tokens
_PRICE_IN_PER_M = 3.0
_PRICE_OUT_PER_M = 15.0


def print_summary() -> None:
    if not DB_PATH.exists():
        print("No database found at", DB_PATH)
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row

        # Check table exists
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='llm_logs'"
        ).fetchone()
        if not exists:
            print("llm_logs table not found — no LLM calls have been logged yet.")
            return

        rows = conn.execute("SELECT * FROM llm_logs ORDER BY timestamp DESC").fetchall()

    if not rows:
        print("llm_logs is empty — no LLM calls have been logged yet.")
        return

    total = len(rows)
    errors = sum(1 for r in rows if r["error"])
    rag_calls = sum(1 for r in rows if r["rag_used"])

    latencies = [r["latency_seconds"] for r in rows if r["latency_seconds"] is not None]
    tokens_in = [r["tokens_in"] for r in rows if r["tokens_in"] is not None]
    tokens_out = [r["tokens_out"] for r in rows if r["tokens_out"] is not None]

    avg_latency = sum(latencies) / len(latencies) if latencies else 0
    avg_in = sum(tokens_in) / len(tokens_in) if tokens_in else 0
    avg_out = sum(tokens_out) / len(tokens_out) if tokens_out else 0

    total_cost = (
        sum(tokens_in) / 1_000_000 * _PRICE_IN_PER_M
        + sum(tokens_out) / 1_000_000 * _PRICE_OUT_PER_M
    )

    print("\n" + "=" * 60)
    print("  LLM CALL OBSERVABILITY SUMMARY")
    print("=" * 60)
    print(f"  Total calls         : {total}")
    print(f"  Error rate          : {errors}/{total} ({errors/total*100:.1f}%)")
    print(f"  RAG usage rate      : {rag_calls}/{total} ({rag_calls/total*100:.1f}%)")
    print(f"  Avg latency         : {avg_latency:.2f}s")
    print(f"  Avg tokens in       : {avg_in:.0f}")
    print(f"  Avg tokens out      : {avg_out:.0f}")
    print(f"  Est. total cost     : ${total_cost:.4f} (sonnet-4-6 pricing)")
    print("=" * 60)

    # Per-app breakdown
    apps: dict[str, list] = {}
    for r in rows:
        apps.setdefault(r["app_name"], []).append(r)

    print("\n  PER-APP BREAKDOWN")
    print(f"  {'App':<16} {'Calls':>6} {'Errors':>7} {'Avg lat':>9} {'Cost':>10}")
    print("  " + "-" * 52)
    for app, app_rows in sorted(apps.items()):
        n = len(app_rows)
        err = sum(1 for r in app_rows if r["error"])
        lats = [r["latency_seconds"] for r in app_rows if r["latency_seconds"] is not None]
        t_in = [r["tokens_in"] for r in app_rows if r["tokens_in"] is not None]
        t_out = [r["tokens_out"] for r in app_rows if r["tokens_out"] is not None]
        avg_lat = sum(lats) / len(lats) if lats else 0
        cost = (
            sum(t_in) / 1_000_000 * _PRICE_IN_PER_M
            + sum(t_out) / 1_000_000 * _PRICE_OUT_PER_M
        )
        print(f"  {app:<16} {n:>6} {err:>7} {avg_lat:>8.2f}s {cost:>9.4f}")

    # Per-tool breakdown
    tools: dict[str, list] = {}
    for r in rows:
        tools.setdefault(r["tool_name"], []).append(r)

    print("\n  PER-TOOL BREAKDOWN")
    print(f"  {'Tool':<35} {'Calls':>6} {'Avg lat':>9} {'Cost':>10}")
    print("  " + "-" * 64)
    for tool, tool_rows in sorted(tools.items()):
        n = len(tool_rows)
        lats = [r["latency_seconds"] for r in tool_rows if r["latency_seconds"] is not None]
        t_in = [r["tokens_in"] for r in tool_rows if r["tokens_in"] is not None]
        t_out = [r["tokens_out"] for r in tool_rows if r["tokens_out"] is not None]
        avg_lat = sum(lats) / len(lats) if lats else 0
        cost = (
            sum(t_in) / 1_000_000 * _PRICE_IN_PER_M
            + sum(t_out) / 1_000_000 * _PRICE_OUT_PER_M
        )
        print(f"  {tool:<35} {n:>6} {avg_lat:>8.2f}s {cost:>9.4f}")

    # Recent 10 calls
    print("\n  RECENT 10 CALLS")
    print(f"  {'Timestamp':<20} {'App':<14} {'Tool':<30} {'In':>6} {'Out':>6} {'Lat':>7} {'Err'}")
    print("  " + "-" * 95)
    for r in rows[:10]:
        ts = r["timestamp"][:19]
        err_flag = "✗" if r["error"] else "✓"
        lat = f"{r['latency_seconds']:.2f}s" if r["latency_seconds"] is not None else "—"
        t_in = str(r["tokens_in"]) if r["tokens_in"] is not None else "—"
        t_out = str(r["tokens_out"]) if r["tokens_out"] is not None else "—"
        print(f"  {ts:<20} {r['app_name']:<14} {r['tool_name']:<30} {t_in:>6} {t_out:>6} {lat:>7} {err_flag}")

    print()


if __name__ == "__main__":
    print_summary()
