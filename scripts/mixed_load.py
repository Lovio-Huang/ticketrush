#!/usr/bin/env python3
"""Mixed load: a reservation burst while other users pay, cancel and let orders
expire, all on the same event. Standard library only.

It checks the one thing the batch consumer could make worse: everything else
that writes the event's stock row (payment does not, cancel and expiry do)
waits while a batch transaction holds it. Every accepted order is watched until
GET shows it, then paid (1/3), cancelled (1/3) or left to expire (1/3; start the
app with a short payment window, e.g. TICKETRUSH_ORDERS_PAYMENTWINDOW=PT15S and
TICKETRUSH_ORDERS_EXPIRYSCANINTERVAL=PT1S). Reports latency and answers per
action, timeouts, and whether every order ended paid, cancelled or expired with
Redis and MySQL in agreement.

Usage:
  python3 scripts/mixed_load.py --base-url http://127.0.0.1:18080 --users 20000
"""

import argparse
import json
import os
import socket
import sys
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from load_test import DEV_JWT_SECRET, create_event, make_token, percentile, request, reserve  # noqa: E402

TERMINAL = {"PAID", "CANCELLED", "EXPIRED"}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--users", type=int, default=20000)
    parser.add_argument("--concurrency", type=int, default=200)
    parser.add_argument("--actors", type=int, default=20, help="threads paying and cancelling")
    parser.add_argument("--jwt-secret", default=os.environ.get("JWT_SECRET", DEV_JWT_SECRET))
    args = parser.parse_args()
    base = urlparse(args.base_url)

    admin = make_token(args.jwt_secret, "1", "user admin")
    event_id = create_event(base, args.users, admin)
    users = [make_token(args.jwt_secret, str(20_000_000 + i), "user") for i in range(args.users)]
    results = defaultdict(list)  # action -> [(status or 'timeout', code, ms)]
    lock = threading.Lock()

    def call(method, path, user):
        started = time.perf_counter()
        try:
            status, body = request(base, method, path, headers={"Authorization": user})
        except (socket.timeout, TimeoutError, OSError):
            return "timeout", None, (time.perf_counter() - started) * 1000
        return status, (body or {}).get("code"), (time.perf_counter() - started) * 1000

    def act(order_id, user):
        deadline = time.time() + 120
        while time.time() < deadline:
            status, body = request(base, "GET", f"/api/orders/{order_id}", headers={"Authorization": user})
            if status == 200 and body["status"] != "PROCESSING":
                break
            time.sleep(0.05)
        choice = hash(order_id) % 3
        if choice == 2:
            return  # left to expire
        action, path = (("pay", "payment") if choice == 0 else ("cancel", "cancellation"))
        outcome = call("POST", f"/api/orders/{order_id}/{path}", user)
        with lock:
            results[action].append(outcome)

    with ThreadPoolExecutor(max_workers=args.actors) as actors:
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            burst = list(pool.map(lambda u: reserve(base, event_id, u, lambda oid, usr, _: actors.submit(act, oid, usr)),
                                  users))
        burst_s = time.perf_counter() - started
    acted_s = time.perf_counter() - started

    # Wait for the unpaid third to expire.
    deadline = time.time() + 180
    while True:
        _, stats = request(base, "GET", f"/api/events/{event_id}/stats", headers={"Authorization": admin})
        by_status = {k: v["orders"] for k, v in stats["byStatus"].items() if v["orders"]}
        if not (set(by_status) - TERMINAL) or time.time() > deadline:
            break
        time.sleep(0.5)
    settled_s = time.perf_counter() - started

    reserve_ms = [ms for outcome, ms, *_ in burst if outcome == "accepted"]
    summary = {
        "users": args.users, "accepted": len(reserve_ms), "burst_s": round(burst_s, 2),
        "reserve_p50": round(percentile(reserve_ms, 50), 1), "reserve_p99": round(percentile(reserve_ms, 99), 1),
        "reserve_outcomes": Counter(outcome for outcome, *_ in burst),
        "all_paid_or_cancelled_s": round(acted_s, 2), "all_settled_s": round(settled_s, 2),
        "final_statuses": by_status, "stores_in_sync": stats["storesInSync"],
        "available_mysql": stats["availableInDatabase"], "available_redis": stats["availableInRedis"],
    }
    for action, outcomes in sorted(results.items()):
        ms = [m for _, _, m in outcomes]
        summary[action] = {
            "count": len(outcomes), "answers": Counter(f"{s} {c or ''}".strip() for s, c, _ in outcomes),
            "p50": round(percentile(ms, 50), 1), "p95": round(percentile(ms, 95), 1),
            "p99": round(percentile(ms, 99), 1), "max": round(max(ms), 1),
        }
    paid = by_status.get("PAID", 0)
    errors = sum(1 for outcomes in results.values() for s, _, _ in outcomes if s == "timeout" or s >= 500)
    ok = (not (set(by_status) - TERMINAL) and sum(by_status.values()) == len(reserve_ms) == args.users
          and stats["storesInSync"] and stats["availableInDatabase"] == args.users - paid and errors == 0)
    summary["pass"] = ok
    print(json.dumps(summary))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
