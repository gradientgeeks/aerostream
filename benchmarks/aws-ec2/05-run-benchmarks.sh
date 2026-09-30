#!/usr/bin/env bash
# The measurement loop. For every round x workload it runs all systems one after another on the SAME two nodes,
# with the system order rotated so no system is always first/last (thermal, EBS/page-cache and noisy-neighbour effects
# average out). One broker container exists at a time; data dir is wiped and the page cache dropped before each start.
set -uo pipefail; source "$(dirname "$0")/lib.sh"; state_load
read -r -a SYS <<< "$SYSTEMS"; read -r -a WLS <<< "$WORKLOADS"; N=${#SYS[@]}
PROG="$RESULTS_DIR/progress.log"; touch "$PROG"
TOTAL=$(( ROUNDS * ${#WLS[@]} * N )); DONE=0; T0=$(date +%s)
log "measurement: $TOTAL runs (rounds=$ROUNDS workloads=${#WLS[@]} systems=$N, warmup ${WARM}m + test ${TEST}m each)"
ENVV="MODE=$MODE KAFKA_IMAGE=$KAFKA_IMAGE REDPANDA_IMAGE=$REDPANDA_IMAGE"
PINV=""; [ "$TOPOLOGY" = single ] && { ENVV="$ENVV PIN=1 BROKER_MEM=${BROKER_MEM:-8g}"; PINV="PIN=1 "; }
for r in $(seq 1 "$ROUNDS"); do
  wi=0
  for w in "${WLS[@]}"; do
    for k in $(seq 0 $((N-1))); do
      s="${SYS[$(( (k + r - 1 + wi) % N ))]}"            # rotated order
      label="${w}__${s}__r${r}"; DONE=$((DONE+1))
      if grep -q "^OK $label\$" "$PROG"; then log "[$DONE/$TOTAL] $label already done"; continue; fi
      log "[$DONE/$TOTAL] $label"
      if ssh_broker "$ENVV ~/bench/broker-node.sh start $s $label" >> "$RESULTS_DIR/orchestrator.log" 2>&1 && \
         ssh_client "${PINV}~/bench/client-node.sh $s $w $label $BROKER_PRIVATE_IP $TEST $WARM" >> "$RESULTS_DIR/orchestrator.log" 2>&1; then
        echo "OK $label" >> "$PROG"
      else
        echo "FAIL $label" >> "$PROG"; log "run $label FAILED (continuing)"
      fi
      ssh_broker "$ENVV ~/bench/broker-node.sh stop $label" >> "$RESULTS_DIR/orchestrator.log" 2>&1 || true
      # pull this run's files right away, so a later hang or the self-destruct timer can never lose finished runs
      mkdir -p "$RESULTS_DIR/client" "$RESULTS_DIR/broker"
      scp -q -r "${SSH_OPTS[@]}" "$SSH_USER@$CLIENT_PUBLIC_IP:/home/ec2-user/bench/results/$label" "$RESULTS_DIR/client/" 2>/dev/null || true
      scp -q -r "${SSH_OPTS[@]}" "$SSH_USER@$BROKER_PUBLIC_IP:/home/ec2-user/bench/broker/$label" "$RESULTS_DIR/broker/" 2>/dev/null || true
      el=$(( $(date +%s) - T0 )); eta=$(( el * (TOTAL - DONE) / DONE / 60 ))
      log "   elapsed $((el/60)) min, ~${eta} min left"; sleep "$COOLDOWN_SEC"
    done
    wi=$((wi+1))
  done
done
log "measurement done: $(grep -c '^OK' "$PROG") ok, $(grep -c '^FAIL' "$PROG") failed"
