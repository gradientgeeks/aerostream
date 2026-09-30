#!/usr/bin/env bash
# Terminates instances, deletes the security group and key pair, then VERIFIES nothing tagged with this run remains.
set -uo pipefail; source "$(dirname "$0")/lib.sh"; state_load
log "destroying run $RUN_ID"
IDS=$(aws_ ec2 describe-instances --filters "Name=tag:Project,Values=$PROJECT_TAG" "Name=tag:RunId,Values=$RUN_ID" \
      "Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down" --query 'Reservations[].Instances[].InstanceId' --output text)
if [ -n "$IDS" ]; then
  log "terminating: $IDS"; aws_ ec2 terminate-instances --instance-ids $IDS >/dev/null; aws_ ec2 wait instance-terminated --instance-ids $IDS
fi
SG="${SG:-$(aws_ ec2 describe-security-groups --filters "Name=tag:RunId,Values=$RUN_ID" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)}"
if [ -n "$SG" ] && [ "$SG" != None ]; then retry 12 aws_ ec2 delete-security-group --group-id "$SG" && log "deleted security group $SG" || log "WARNING: could not delete $SG"; fi
aws_ ec2 delete-key-pair --key-name "$KEY_NAME" >/dev/null 2>&1 && log "deleted key pair $KEY_NAME"
[ -f "$KEY_FILE" ] && shred -u "$KEY_FILE" 2>/dev/null || rm -f "$KEY_FILE"
LEFT=$(aws_ ec2 describe-instances --filters "Name=tag:Project,Values=$PROJECT_TAG" "Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down" --query 'length(Reservations[].Instances[])' --output text)
VOLS=$(aws_ ec2 describe-volumes --filters "Name=tag:Project,Values=$PROJECT_TAG" --query 'length(Volumes[])' --output text)
log "VERIFY: $LEFT tagged instance(s) and $VOLS tagged volume(s) remain in $AWS_REGION"
[ "$LEFT" = 0 ] && [ "$VOLS" = 0 ] && { log "destroyed: nothing left running or billing"; mv "$STATE" "$STATE.destroyed" 2>/dev/null; exit 0; }
die "resources may remain; check the console"
