#!/usr/bin/env bash
# Backfill a range of hours through the ingest DAG.
#
#   scripts/backfill.sh 2026-09-14 2026-09-15 [parallelism]
#
# Runs `airflow dags test` per hour, which executes the same code path as a scheduled
# run - there is no separate backfill logic in this project, by design (ADR 0002).
#
# Parallelism defaults to 2 to match max_active_runs on the DAG. That bound exists to
# be a good citizen toward dumps.wikimedia.org, which is a free public service; raising
# it here would route around the limit rather than respect it.
set -uo pipefail

cd "$(dirname "$0")/.."

START_DATE="${1:?usage: backfill.sh START_DATE END_DATE [parallelism]}"
END_DATE="${2:?usage: backfill.sh START_DATE END_DATE [parallelism]}"
PARALLEL="${3:-2}"

LOG_DIR="${TMPDIR:-/tmp}/wikiflow-backfill"
mkdir -p "$LOG_DIR"

# Build the list of hours to process.
hours=()
current="$START_DATE"
while [[ "$current" < "$END_DATE" || "$current" == "$END_DATE" ]]; do
  for h in $(seq -w 0 23); do
    hours+=("${current}T${h}:00:00+00:00")
  done
  current=$(date -u -d "$current + 1 day" +%Y-%m-%d 2>/dev/null) || break
done

total=${#hours[@]}
echo "backfilling $total hours with parallelism $PARALLEL"
echo "logs: $LOG_DIR"
echo

run_one() {
  local ts="$1"
  local slug="${ts//[:+]/-}"
  local log="$LOG_DIR/${slug}.log"

  # </dev/null matters: without it a backgrounded `compose exec` can block waiting on
  # stdin that will never arrive.
  if MSYS_NO_PATHCONV=1 docker compose exec -T airflow-scheduler \
       airflow dags test pageviews_ingest "$ts" </dev/null >"$log" 2>&1; then
    if grep -q "state=success" "$log"; then
      echo "OK   $ts"
      return 0
    fi
  fi
  echo "FAIL $ts   (see $log)"
  return 1
}

# Simple worker pool: each worker takes every Nth hour.
for ((w = 0; w < PARALLEL; w++)); do
  (
    for ((i = w; i < total; i += PARALLEL)); do
      run_one "${hours[$i]}"
    done
  ) &
done
wait

echo
echo "BACKFILL COMPLETE"
