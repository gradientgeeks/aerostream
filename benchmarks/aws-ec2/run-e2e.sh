#!/usr/bin/env bash
# Full end-to-end run:  preflight -> provision -> bootstrap -> measure -> scp results -> destroy (always, even on failure).
#   ./run-e2e.sh            print the plan, time and cost estimate, and exit (nothing is created)
#   ./run-e2e.sh --yes      actually run it (this spends money on AWS)
#   PROFILE=quick|fair|full  MODE=parity|fullnode  SYSTEMS="aerostream kafka redpanda"  KEEP=1 (skip destroy)
set -uo pipefail; source "$(dirname "$0")/lib.sh"
NS=$(wc -w <<< "$SYSTEMS"); NW=$(wc -w <<< "$WORKLOADS"); RUNS=$((ROUNDS*NW*NS))
PER_RUN=$((WARM+TEST+4)); SETUP=25; EST=$((SETUP + RUNS*PER_RUN + RUNS*COOLDOWN_SEC/60 + 6))
: "${MAX_RUNTIME_MIN:=$((EST + 75))}"; export MAX_RUNTIME_MIN
if [ "$TOPOLOGY" = single ]; then HOURLY=0.4032; TYPES="$SINGLE_TYPE (one machine: broker + load generator, cores split)"; else HOURLY=0.3716; TYPES="broker $BROKER_TYPE + client $CLIENT_TYPE"; fi
COST=$(awk -v m="$EST" -v h="$HOURLY" 'BEGIN{printf "%.2f", h*m/60 + 0.02}')
cat <<PLAN
== AeroStream EC2 benchmark: run $RUN_ID ==
  profile=$PROFILE mode=$MODE   $RUNS runs = $ROUNDS round(s) x $NW workload(s) x $NS system(s), warm-up ${WARM}m + test ${TEST}m each
  $TYPES in $AZ
  estimated wall time: ~${EST} min (setup ~${SETUP} + measurement ~$((RUNS*(PER_RUN)+RUNS*COOLDOWN_SEC/60)) + collect/destroy ~6)
  estimated cost:      ~\$$COST on-demand   (instances self-terminate after ${MAX_RUNTIME_MIN} min regardless)
PLAN
[ "${1:-}" = "--yes" ] || { echo "dry plan only. Re-run with --yes to launch."; exit 0; }

FINISHED=0
finish() {
  trap - EXIT INT TERM HUP
  if [ -f "$STATE" ] && grep -q '^CLIENT_PUBLIC_IP=' "$STATE"; then
    log "collecting results"; bash "$HERE/06-collect.sh" || log "collect had problems"
  fi
  if [ "$KEEP" = 1 ]; then log "KEEP=1: leaving instances running (run ./07-destroy.sh with RUN_ID=$RUN_ID). They still self-terminate after ${MAX_RUNTIME_MIN} min."
  elif [ -f "$STATE" ]; then bash "$HERE/07-destroy.sh"; fi
  [ "$FINISHED" = 1 ] && log "E2E COMPLETE: results in $RESULTS_DIR" || log "E2E ended early: partial results (if any) in $RESULTS_DIR"
}
trap finish EXIT INT TERM HUP
START=$(date +%s)
if [ "${RESUME:-0}" = 1 ]; then
  # RESUME=1 RUN_ID=<id> ./run-e2e.sh --yes : reuse the instances of an interrupted run (skips preflight + provisioning)
  grep -q '^CLIENT_PUBLIC_IP=' "$STATE" 2>/dev/null || die "nothing to resume: no provisioned instances in $STATE"
  state_load; log "RESUME: reusing broker $BROKER_ID and client $CLIENT_ID"
  retry 12 ssh_broker true && retry 12 ssh_client true || die "cannot ssh to the existing nodes"
  bash "$HERE/04-bootstrap.sh" && bash "$HERE/05-run-benchmarks.sh" && FINISHED=1
else
  bash "$HERE/02-preflight.sh" && bash "$HERE/03-provision.sh" && bash "$HERE/04-bootstrap.sh" && bash "$HERE/05-run-benchmarks.sh" && FINISHED=1
fi
log "total wall time so far: $(( ($(date +%s)-START)/60 )) min"
