#!/usr/bin/env bash
# Validates everything WITHOUT creating anything: identity, permissions (EC2 dry-runs), AMI, subnet, local tools.
set -euo pipefail; source "$(dirname "$0")/lib.sh"
log "preflight (run id $RUN_ID)"
for t in aws ssh scp jq docker python3 curl; do command -v "$t" >/dev/null || die "missing local tool: $t"; done
ARN=$(aws_ sts get-caller-identity --query Arn --output text); log "identity: $ARN"
case "$ARN" in *:root) die "refusing to run as the root user; use the aerostream-bench service account";; esac
AMI=$(aws_ ssm get-parameter --name "$AMI_PARAM" --query Parameter.Value --output text); log "AMI: $AMI"
SUBNET=$(aws_ ec2 describe-subnets --filters Name=default-for-az,Values=true Name=availability-zone,Values="$AZ" --query 'Subnets[0].SubnetId' --output text)
[ "$SUBNET" != None ] || die "no default subnet in $AZ"; log "subnet: $SUBNET ($AZ)"
TAGS="ResourceType=instance,Tags=[{Key=Project,Value=$PROJECT_TAG}] ResourceType=volume,Tags=[{Key=Project,Value=$PROJECT_TAG}]"
if [ "$TOPOLOGY" = single ]; then TYPES_TO_CHECK="$SINGLE_TYPE"; else TYPES_TO_CHECK="$BROKER_TYPE $CLIENT_TYPE"; fi
if [ -n "${AERO_IMAGE:-}" ]; then docker image inspect "$AERO_IMAGE" >/dev/null 2>&1 && log "AeroStream image: $AERO_IMAGE ($(docker image inspect "$AERO_IMAGE" --format '{{.Id}}' | cut -c8-19))" || die "AERO_IMAGE=$AERO_IMAGE not found locally"; fi
for T in $TYPES_TO_CHECK; do
  out=$(aws_ ec2 run-instances --dry-run --image-id "$AMI" --instance-type "$T" --subnet-id "$SUBNET" --tag-specifications $TAGS 2>&1 || true)
  echo "$out" | grep -q DryRunOperation && log "launch permission OK for $T" || die "cannot launch $T: $out"
done
N=$(aws_ ec2 describe-instances --filters Name=instance-state-name,Values=pending,running,stopping,stopped --query 'length(Reservations[].Instances[])' --output text)
[ "$N" = 0 ] || log "WARNING: $N non-terminated instance(s) already exist in $AWS_REGION (vCPU quota is 8: 2 x 4 vCPU needed)"
MYIP=$(curl -s https://checkip.amazonaws.com | tr -d '[:space:]'); log "your public IP (SSH will be opened to this /32 only): $MYIP"
log "profile=$PROFILE workloads=[$WORKLOADS] rounds=$ROUNDS warmup=${WARM}m test=${TEST}m systems=[$SYSTEMS] mode=$MODE"
log "preflight OK"
