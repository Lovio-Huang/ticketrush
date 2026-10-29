#!/usr/bin/env bash
# Mixed-load check on an isolated compose project. Usage: run.sh <out-dir> [runs]
set -uo pipefail
out=$1; runs=${2:-3}
cd "$(git rev-parse --show-toplevel)"
export COMPOSE_PROJECT_NAME=trmix APP_PORT=18080 MYSQL_PORT=13306 REDIS_PORT=16379 KAFKA_PORT=19092
dc() { docker compose -f compose.yaml -f "$(dirname "$0")/compose-override.yml" "$@"; }
dc down -v >/dev/null 2>&1
dc up --build -d --wait >/dev/null 2>&1 || { echo "compose up failed"; exit 1; }
until curl -fs http://127.0.0.1:18080/actuator/health >/dev/null; do sleep 1; done
pw=$(dc logs mysql | sed -n 's/.*GENERATED ROOT PASSWORD: \([^ ]*\).*/\1/p' | head -1)
locks() { dc exec -T mysql mysql -uroot -p"$pw" -N -B -e "SELECT NAME, COUNT FROM information_schema.INNODB_METRICS WHERE NAME IN ('lock_deadlocks','lock_timeouts','lock_row_lock_waits','lock_row_lock_time','lock_row_lock_time_max'); SHOW GLOBAL STATUS LIKE 'Innodb_row_lock%';" 2>/dev/null | tr '\t' '=' | paste -sd' ' -; }
python3 scripts/load_test.py --base-url http://127.0.0.1:18080 --tickets 2000 --users 2000 >/dev/null  # warm-up
for i in $(seq 1 "$runs"); do
  echo "run $i before: $(locks)" >> "$out/locks.txt"
  python3 scripts/mixed_load.py --base-url http://127.0.0.1:18080 --users 20000 >> "$out/runs.jsonl"
  echo "run $i after:  $(locks)" >> "$out/locks.txt"
done
dc logs app > "$out/app.log" 2>&1
dc down -v >/dev/null 2>&1
