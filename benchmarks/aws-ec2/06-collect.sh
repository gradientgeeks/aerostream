#!/usr/bin/env bash
# Copies everything back to this machine (scp) and records exactly what was tested.
set -uo pipefail; source "$(dirname "$0")/lib.sh"; state_load
[ -n "${CLIENT_PUBLIC_IP:-}" ] || die "no client in state"
mkdir -p "$RESULTS_DIR/client" "$RESULTS_DIR/broker" "$RESULTS_DIR/host"
log "collecting results from the client node"; scp -q -r "${SSH_OPTS[@]}" "$SSH_USER@$CLIENT_PUBLIC_IP:/home/ec2-user/bench/results/." "$RESULTS_DIR/client/" || log "client scp reported errors"
log "collecting broker-node metrics";          scp -q -r "${SSH_OPTS[@]}" "$SSH_USER@$BROKER_PUBLIC_IP:/home/ec2-user/bench/broker/." "$RESULTS_DIR/broker/" || log "broker scp reported errors"
for n in broker client; do
  ip_var="${n^^}_PUBLIC_IP"; ip="${!ip_var}"
  ssh "${SSH_OPTS[@]}" "$SSH_USER@$ip" 'lscpu | head -20; echo; uname -srm; echo; free -h; echo; lsblk -o NAME,SIZE,MODEL,MOUNTPOINT; echo; sudo docker version --format "docker {{.Server.Version}}" 2>/dev/null; java -version 2>&1 | head -1' > "$RESULTS_DIR/host/$n.txt" 2>&1 || true
done
IMAGES=""
case " $SYSTEMS " in *" aerostream "*) IMAGES="$IMAGES\"aerostream\": \"${AERO_IMAGE:-}\", " ;; esac
case " $SYSTEMS " in *" kafka "*) IMAGES="$IMAGES\"kafka\": \"$KAFKA_IMAGE\", " ;; esac
case " $SYSTEMS " in *" redpanda "*) IMAGES="$IMAGES\"redpanda\": \"$REDPANDA_IMAGE\", " ;; esac
IMAGES="${IMAGES%, }"
cat > "$RESULTS_DIR/run-info.json" <<JSON
{ "run_id": "$RUN_ID", "region": "$AWS_REGION", "az": "$AZ", "topology": "$TOPOLOGY", "broker_type": "$( [ "$TOPOLOGY" = single ] && echo $SINGLE_TYPE || echo $BROKER_TYPE )", "client_type": "$( [ "$TOPOLOGY" = single ] && echo $SINGLE_TYPE || echo $CLIENT_TYPE )",
  "profile": "$PROFILE", "mode": "$MODE", "workloads": "$WORKLOADS", "rounds": $ROUNDS, "warmup_min": $WARM, "test_min": $TEST,
  "systems": "$SYSTEMS", "images": { $IMAGES },
  "aerostream_git": "${GIT_SHA:-}", "omb_commit": "$OMB_COMMIT", "launched_at": "${LAUNCHED_AT:-}", "collected_at": "$(date -u +%FT%TZ)" }
JSON
N=$(find "$RESULTS_DIR/client" -name result.json | wc -l); log "collected $N result files into $RESULTS_DIR"
python3 "$HERE/summarize.py" "$RESULTS_DIR" || log "summary generation failed"
