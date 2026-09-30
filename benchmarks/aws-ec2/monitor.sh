#!/usr/bin/env bash
# Status line every INTERVAL seconds for one run; exits when the run ends. Usage: RUN_ID=<id> ./monitor.sh [interval-sec]
# Prints: stage, ok/fail counts, running instances, elapsed. Also prints (and exits) on: run finished, run process gone,
# a failed measurement, or instances still alive after the run reported completion.
set -uo pipefail; source "$(dirname "$0")/lib.sh"
INTERVAL="${1:-300}"; LOG="$RESULTS_DIR/orchestrator.log"; PROG="$RESULTS_DIR/progress.log"; T0=$(date +%s); LASTFAIL=0
state_load
while true; do
  last=$(grep -v '^$' "$LOG" 2>/dev/null | tail -1 | cut -c1-110)
  ok=$(grep -c '^OK' "$PROG" 2>/dev/null || true); fail=$(grep -c '^FAIL' "$PROG" 2>/dev/null || true)
  total=$(grep -o '[0-9]* runs' "$LOG" 2>/dev/null | head -1 | cut -d' ' -f1)
  inst=$(aws_ ec2 describe-instances --filters "Name=tag:Project,Values=$PROJECT_TAG" "Name=tag:RunId,Values=$RUN_ID" \
        "Name=instance-state-name,Values=pending,running,stopping,shutting-down" --query 'length(Reservations[].Instances[])' --output text 2>/dev/null || echo '?')
  echo "[$(date -u +%H:%M)] run $RUN_ID | ok=${ok:-0} fail=${fail:-0} of ${total:-?} | instances up=$inst | $(( ($(date +%s)-T0)/60 )) min watched | $last"
  [ "${fail:-0}" -gt "$LASTFAIL" ] && { echo "ALERT: a measurement run FAILED: $(grep '^FAIL' "$PROG" | tail -1)"; LASTFAIL=${fail:-0}; }
  if grep -qE 'E2E COMPLETE|E2E ended early' "$LOG" 2>/dev/null; then
    echo "RUN FINISHED: $(grep -E 'E2E COMPLETE|E2E ended early' "$LOG" | tail -1 | cut -c1-140)"
    [ "$inst" = 0 ] && echo "OK: no instances left running" || echo "ALERT: $inst instance(s) still up after finish, run ./07-destroy.sh"
    exit 0
  fi
  pgrep -f "[r]un-e2e.sh --yes" >/dev/null || { echo "ALERT: run-e2e.sh process is gone but the run did not report completion (instances up=$inst)"; exit 1; }
  sleep "$INTERVAL"
done
