#!/usr/bin/env python3
"""Order-pipeline benchmark: runs scripts/load_test.py against a fresh compose
stack and records, per run, what the consumer did. Standard library only.

For each run it keeps:
  * the load test's JSON summary (202 latency, 202 -> visible, backlog, drain,
    the consumer's rate while backlogged)
  * MySQL's statement digests for the run (performance_schema): how many of
    each statement ran and the time MySQL spent in them, so SQL per order is
    counted, not guessed
  * the consumer's timers from /actuator/prometheus: the Redis claim
    (ticketrush_orders_claim) and the whole write transaction including the
    commit (ticketrush_orders_write)

Results go to perf/results/<label>/: env.txt (commit, uncommitted diff hash,
machine, Docker), code.diff, source.tar.gz + source.sha256 (every source file
as built, hashed before the build) and source-check.txt (whether the hashes
still matched after the run), and runs.jsonl (one line per run). The stack runs
under its own compose project on its own ports and is removed afterwards
(with its volumes) unless --keep is given.

Usage:
  python3 scripts/consumer_bench.py baseline --runs 5
"""

import argparse
import hashlib
import json
import os
import pathlib
import platform
import re
import subprocess
import sys
import time
import urllib.request

SOURCE_PATHS = ["src", "scripts", "pom.xml", "compose.yaml", "Dockerfile"]

SCENARIOS = [("sell-out", 2000, 20000), ("enough", 20000, 20000)]


def manifest(paths):
    return "".join(f"{hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()}  {p}\n" for p in paths)


def sh(*args, check=True, **kwargs):
    return subprocess.run(args, check=check, capture_output=True, text=True, **kwargs).stdout


def compose(*args, check=True):
    return sh("docker", "compose", *args, check=check)


def mysql_root(sql, password):
    return compose("exec", "-T", "mysql", "mysql", "-uroot", f"-p{password}", "-N", "-B", "-e", sql)


def prometheus(base):
    with urllib.request.urlopen(f"{base}/actuator/prometheus", timeout=10) as response:
        lines = response.read().decode().splitlines()
    return {line.split(" ")[0]: float(line.rsplit(" ", 1)[1]) for line in lines
            if line.startswith(("ticketrush_orders_write_seconds", "ticketrush_orders_claim_seconds",
                                "ticketrush_orders_consumed_total"))}


def digests(password):
    rows = mysql_root("SELECT COUNT_STAR, SUM_TIMER_WAIT, DIGEST_TEXT FROM performance_schema."
                      "events_statements_summary_by_digest WHERE SCHEMA_NAME = 'ticketrush' "
                      "ORDER BY COUNT_STAR DESC LIMIT 30", password)
    out = []
    for row in rows.splitlines():
        count, picos, text = row.split("\t", 2)
        out.append({"count": int(count), "ms": int(picos) / 1e9, "sql": text[:160]})
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("label")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--keep", action="store_true", help="leave the stack running")
    args = parser.parse_args()

    repo = pathlib.Path(__file__).resolve().parent.parent
    os.chdir(repo)
    os.environ.setdefault("COMPOSE_PROJECT_NAME", "trbench")
    for name, port in [("APP_PORT", "18080"), ("MYSQL_PORT", "13306"), ("REDIS_PORT", "16379"),
                       ("KAFKA_PORT", "19092")]:
        os.environ.setdefault(name, port)
    base = f"http://127.0.0.1:{os.environ['APP_PORT']}"
    out = repo / "perf" / "results" / args.label
    out.mkdir(parents=True, exist_ok=True)

    diff = sh("git", "diff", "HEAD")
    (out / "code.diff").write_text(diff)
    # The whole source tree as built (tracked and untracked files), saved and hashed
    # before the build, and hashed again after the run to show it didn't change.
    sources = sorted(sh("git", "ls-files", "--cached", "--others", "--exclude-standard", *SOURCE_PATHS).split())
    sh("tar", "-czf", str(out / "source.tar.gz"), *sources)
    (out / "source.sha256").write_text(manifest(sources))
    env = {
        "commit": sh("git", "rev-parse", "HEAD").strip(),
        "uncommitted_diff_sha256": hashlib.sha256(diff.encode()).hexdigest(),
        "untracked": sh("git", "ls-files", "--others", "--exclude-standard").split(),
        "machine": platform.platform(),
        "cpu": sh("sysctl", "-n", "machdep.cpu.brand_string", check=False).strip() or platform.processor(),
        "memory_bytes": sh("sysctl", "-n", "hw.memsize", check=False).strip(),
        "docker": sh("docker", "info", "--format", "{{.ServerVersion}} {{.NCPU}} CPUs {{.MemTotal}} bytes").strip(),
        "scenarios": SCENARIOS,
        "concurrency": 200,
        "started": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    (out / "env.txt").write_text(json.dumps(env, indent=2) + "\n")

    compose("down", "-v", check=False)
    compose("up", "--build", "-d", "--wait")
    password = re.search(r"GENERATED ROOT PASSWORD: (\S+)", compose("logs", "mysql")).group(1)
    while True:
        try:
            urllib.request.urlopen(f"{base}/actuator/health", timeout=5)
            break
        except OSError:
            time.sleep(1)

    def load_test(tickets, users):
        result = subprocess.run([sys.executable, "scripts/load_test.py", "--base-url", base, "--tickets",
                                 str(tickets), "--users", str(users), "--concurrency", "200", "--json"],
                                capture_output=True, text=True)
        summary = next((json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")), None)
        return summary, result.stdout

    load_test(2000, 2000)  # warm-up: JIT, connection pools, topic metadata
    try:
        with (out / "runs.jsonl").open("a") as runs:
            for run in range(1, args.runs + 1):
                for name, tickets, users in SCENARIOS:
                    mysql_root("TRUNCATE TABLE performance_schema.events_statements_summary_by_digest", password)
                    before = prometheus(base)
                    summary, text = load_test(tickets, users)
                    after = prometheus(base)
                    record = {"run": run, "scenario": name, "load": summary,
                              "consumer": {key: after[key] - before.get(key, 0) for key in after
                                           if key.endswith(("_count", "_sum", "_total"))},
                              "consumer_quantiles_after": {k: v for k, v in after.items() if "quantile" in k},
                              "mysql": digests(password)}
                    runs.write(json.dumps(record) + "\n")
                    runs.flush()
                    (out / f"run{run}-{name}.txt").write_text(text)
                    print(f"run {run} {name}: {json.dumps(summary)}")
    finally:
        unchanged = manifest(sources) == (out / "source.sha256").read_text()
        (out / "source-check.txt").write_text(f"source unchanged during the run: {unchanged}\n")
        if not args.keep:
            compose("down", "-v", check=False)


if __name__ == "__main__":
    main()
