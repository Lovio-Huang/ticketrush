#!/usr/bin/env python3
"""Medians and [min - max] of perf/results/<label>/runs.jsonl, side by side.

Usage: python3 perf/compare.py baseline batched
"""

import json
import pathlib
import statistics
import sys

METRICS = [
    ("req_per_s", "HTTP requests/s", "{:,.0f}"),
    ("ok_p50", "202 latency p50 (ms)", "{:.1f}"),
    ("ok_p95", "202 latency p95 (ms)", "{:.1f}"),
    ("ok_p99", "202 latency p99 (ms)", "{:.1f}"),
    ("consumer_per_s", "consumer msgs/s while backlogged", "{:,.0f}"),
    ("orders_per_s_end_to_end", "orders in MySQL/s, start to last", "{:,.0f}"),
    ("drain_s", "drained after last response (s)", "{:.2f}"),
    ("visible_p50", "202 -> visible p50 (ms)", "{:,.0f}"),
    ("visible_p95", "202 -> visible p95 (ms)", "{:,.0f}"),
    ("visible_p99", "202 -> visible p99 (ms)", "{:,.0f}"),
]


def load(label):
    path = pathlib.Path(__file__).parent / "results" / label / "runs.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()]


def per_order(run):
    """Consumer-side figures per stored order: MySQL statements by kind, transaction time."""
    orders = run["load"]["outcomes"].get("accepted", 0)
    kinds = {}
    for digest in run["mysql"]:
        sql = digest["sql"]
        kind = ("COMMIT" if sql.startswith("COMMIT") else "INSERT orders" if sql.startswith("INSERT INTO `orders`")
                else "UPDATE events" if sql.startswith("UPDATE EVENTS") else "SET autocommit"
                if sql.startswith("SET `autocommit`") else None)
        if kind:
            count, ms = kinds.get(kind, (0, 0))
            kinds[kind] = (count + digest["count"], ms + digest["ms"])
    consumer = run["consumer"]
    tx_ms = 1000 * consumer.get("ticketrush_orders_write_seconds_sum", 0)
    claim_ms = 1000 * consumer.get("ticketrush_orders_claim_seconds_sum", 0)
    return orders, kinds, tx_ms, claim_ms, consumer.get("ticketrush_orders_write_seconds_count", 0)


def fmt(values, pattern):
    values = [v for v in values if v is not None]
    if not values:
        return "n/a"
    return f"{pattern.format(statistics.median(values))} [{pattern.format(min(values))} - {pattern.format(max(values))}]"


def main():
    labels = sys.argv[1:]
    data = {label: load(label) for label in labels}
    for scenario in ("sell-out", "enough"):
        print(f"\n== {scenario} ({len(data[labels[0]]) // 2} runs per side)")
        for key, name, pattern in METRICS:
            row = [fmt([r["load"].get(key) for r in data[l] if r["scenario"] == scenario], pattern) for l in labels]
            print(f"{name:36} " + " | ".join(f"{cell:32}" for cell in row))
        for label in labels:
            runs = [r for r in data[label] if r["scenario"] == scenario]
            stats = [per_order(r) for r in runs]
            orders = sum(s[0] for s in stats)
            kinds = {}
            for s in stats:
                for kind, (count, ms) in s[1].items():
                    c, m = kinds.get(kind, (0, 0))
                    kinds[kind] = (c + count, m + ms)
            txs = sum(s[4] for s in stats)
            print(f"  {label}: per stored order "
                  + ", ".join(f"{kind} {c / orders:.2f}x {m * 1000 / orders:.0f}us" for kind, (c, m) in sorted(kinds.items()))
                  + f"; consumer transactions {txs / orders:.3f} per order, {sum(s[2] for s in stats) / orders * 1000:.0f}us"
                  f" tx time and {sum(s[3] for s in stats) / orders * 1000:.0f}us Redis claim per order;"
                  f" every run passed: {all(r['load']['pass'] for r in runs)}")


if __name__ == "__main__":
    main()
