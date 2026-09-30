#!/usr/bin/env bash
# Shared helpers. Source after config.env:  source "$(dirname "$0")/lib.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/config.env"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d-%H%M%S)}"; export RUN_ID
STATE_DIR="$HERE/state"; RESULTS_DIR="$HERE/results/$RUN_ID"; mkdir -p "$STATE_DIR" "$RESULTS_DIR"
STATE="$STATE_DIR/$RUN_ID.env"
KEY_NAME="aerostream-bench-$RUN_ID"; KEY_FILE="$STATE_DIR/$KEY_NAME.pem"

log()  { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$RESULTS_DIR/orchestrator.log" >&2; }
die()  { log "ERROR: $*"; exit 1; }
aws_() { aws --profile "$AWS_PROFILE" --region "$AWS_REGION" "$@"; }
state_set() { grep -v "^$1=" "$STATE" 2>/dev/null > "$STATE.tmp" || true; echo "$1=$2" >> "$STATE.tmp"; mv "$STATE.tmp" "$STATE"; }
state_load() { [ -f "$STATE" ] && source "$STATE" || true; }
SSH_OPTS=(-i "$KEY_FILE" -o IdentitiesOnly=yes -o IdentityAgent=none -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15 -o ServerAliveInterval=20 -o ServerAliveCountMax=6)
ssh_broker() { ssh "${SSH_OPTS[@]}" "$SSH_USER@$BROKER_PUBLIC_IP" "$@"; }
ssh_client() { ssh "${SSH_OPTS[@]}" "$SSH_USER@$CLIENT_PUBLIC_IP" "$@"; }
# scp_to <host> <src...> <dest-dir-on-host>
scp_to()   { local h="$1"; shift; local dest="${*: -1}"; scp -q "${SSH_OPTS[@]}" -r "${@:1:$#-1}" "$SSH_USER@$h:$dest"; }
retry() { local n=$1; shift; local i; for i in $(seq 1 "$n"); do "$@" && return 0; sleep 5; done; return 1; }
