#!/usr/bin/env bash
# Creates: key pair, security group (SSH from your IP only + all traffic inside the group), 2 tagged instances.
# Both instances terminate themselves after MAX_RUNTIME_MIN (dead-man switch) even if this machine dies.
set -euo pipefail; source "$(dirname "$0")/lib.sh"
: "${MAX_RUNTIME_MIN:?set MAX_RUNTIME_MIN (run-e2e.sh derives it from the profile)}"
[ -f "$STATE" ] && die "state for run $RUN_ID already exists ($STATE); destroy it first"
touch "$STATE"; state_set RUN_ID "$RUN_ID"
AMI=$(aws_ ssm get-parameter --name "$AMI_PARAM" --query Parameter.Value --output text)
SUBNET=$(aws_ ec2 describe-subnets --filters Name=default-for-az,Values=true Name=availability-zone,Values="$AZ" --query 'Subnets[0].SubnetId' --output text)
VPC=$(aws_ ec2 describe-subnets --subnet-ids "$SUBNET" --query 'Subnets[0].VpcId' --output text)
MYIP=$(curl -s https://checkip.amazonaws.com | tr -d '[:space:]')

log "creating key pair $KEY_NAME"
aws_ ec2 create-key-pair --key-name "$KEY_NAME" --query KeyMaterial --output text > "$KEY_FILE"; chmod 600 "$KEY_FILE"
state_set KEY_NAME "$KEY_NAME"

log "creating security group"
SG=$(aws_ ec2 create-security-group --group-name "$KEY_NAME" --description "aerostream benchmark $RUN_ID" --vpc-id "$VPC" \
     --tag-specifications "ResourceType=security-group,Tags=[{Key=Project,Value=$PROJECT_TAG},{Key=RunId,Value=$RUN_ID}]" --query GroupId --output text)
state_set SG "$SG"
aws_ ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 --cidr "$MYIP/32" >/dev/null
aws_ ec2 authorize-security-group-ingress --group-id "$SG" --ip-permissions "IpProtocol=-1,UserIdGroupPairs=[{GroupId=$SG}]" >/dev/null

UD=$(printf '#!/bin/bash\nshutdown -h +%s "aerostream-bench dead-man switch"\n' "$MAX_RUNTIME_MIN")
launch() { # role type
  aws_ ec2 run-instances --image-id "$AMI" --instance-type "$2" --subnet-id "$SUBNET" --security-group-ids "$SG" --key-name "$KEY_NAME" \
    --instance-initiated-shutdown-behavior terminate --user-data "$UD" --metadata-options HttpTokens=required,HttpEndpoint=enabled \
    --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=$ROOT_GB,VolumeType=gp3,DeleteOnTermination=true}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Project,Value=$PROJECT_TAG},{Key=RunId,Value=$RUN_ID},{Key=Role,Value=$1},{Key=Name,Value=aerostream-bench-$1-$RUN_ID}]" \
                         "ResourceType=volume,Tags=[{Key=Project,Value=$PROJECT_TAG},{Key=RunId,Value=$RUN_ID}]" \
    --query 'Instances[0].InstanceId' --output text
}
if [ "$TOPOLOGY" = single ]; then
  log "launching ONE machine ($SINGLE_TYPE) for broker + load generator; self-destruct in ${MAX_RUNTIME_MIN} min"
  BID=$(launch combined "$SINGLE_TYPE"); CID=$BID; state_set BROKER_ID "$BID"; state_set CLIENT_ID "$CID"
  log "waiting for the instance to pass status checks"
  aws_ ec2 wait instance-status-ok --instance-ids "$BID"
  read -r BPUB BPRIV < <(aws_ ec2 describe-instances --instance-ids "$BID" --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' --output text)
  CPUB=$BPUB; CPRIV=$BPRIV
else
  log "launching broker ($BROKER_TYPE) and client ($CLIENT_TYPE); self-destruct in ${MAX_RUNTIME_MIN} min"
  BID=$(launch broker "$BROKER_TYPE"); state_set BROKER_ID "$BID"
  CID=$(launch client "$CLIENT_TYPE"); state_set CLIENT_ID "$CID"
  log "waiting for instances to pass status checks"
  aws_ ec2 wait instance-status-ok --instance-ids "$BID" "$CID"
  read -r BPUB BPRIV < <(aws_ ec2 describe-instances --instance-ids "$BID" --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' --output text)
  read -r CPUB CPRIV < <(aws_ ec2 describe-instances --instance-ids "$CID" --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' --output text)
fi
state_set BROKER_PUBLIC_IP "$BPUB"; state_set BROKER_PRIVATE_IP "$BPRIV"; state_set CLIENT_PUBLIC_IP "$CPUB"; state_set CLIENT_PRIVATE_IP "$CPRIV"
state_set LAUNCHED_AT "$(date -u +%FT%TZ)"
state_load
log "broker $BID  $BPUB / $BPRIV     client $CID  $CPUB / $CPRIV"
retry 24 ssh_broker true && retry 24 ssh_client true || die "ssh to the nodes failed"
log "provisioned"
