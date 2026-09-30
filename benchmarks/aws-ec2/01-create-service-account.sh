#!/usr/bin/env bash
# One-time, idempotent: make sure a least-privilege IAM service account exists for the EC2 benchmark.
#   user    aerostream-bench            existing; kept as is
#   policy  aerostream-bench-ec2        existing: tag-scoped EC2 handling, but only c7i-flex.large may be launched
#   policy  aerostream-bench-ec2-perf   added here (additive): also c6i.xlarge / c6id.xlarge, only if tagged Project=aerostream-bench
# The original policy is not edited. Run with ADMIN credentials:   ADMIN_PROFILE=default ./01-create-service-account.sh
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ADMIN_PROFILE="${ADMIN_PROFILE:-default}"; USER_NAME="${SERVICE_USER:-aerostream-bench}"; POLICY_NAME="aerostream-bench-ec2-perf"
aws_() { aws --profile "$ADMIN_PROFILE" "$@"; }
ACCOUNT=$(aws_ sts get-caller-identity --query Account --output text)
echo "admin identity: $(aws_ sts get-caller-identity --query Arn --output text)"
aws_ iam get-user --user-name "$USER_NAME" >/dev/null 2>&1 || { echo "creating IAM user $USER_NAME"; aws_ iam create-user --user-name "$USER_NAME" --tags Key=Project,Value=aerostream-bench >/dev/null; }
POLICY_ARN="arn:aws:iam::${ACCOUNT}:policy/${POLICY_NAME}"
if aws_ iam get-policy --policy-arn "$POLICY_ARN" >/dev/null 2>&1; then
  echo "policy exists; publishing current JSON as the new default version"
  if [ "$(aws_ iam list-policy-versions --policy-arn "$POLICY_ARN" --query 'length(Versions)' --output text)" -ge 5 ]; then
    OLD=$(aws_ iam list-policy-versions --policy-arn "$POLICY_ARN" --query 'Versions[?!IsDefaultVersion]|[-1].VersionId' --output text)
    aws_ iam delete-policy-version --policy-arn "$POLICY_ARN" --version-id "$OLD"
  fi
  aws_ iam create-policy-version --policy-arn "$POLICY_ARN" --policy-document "file://$HERE/iam/aerostream-bench-ec2-perf.policy.json" --set-as-default >/dev/null
else
  echo "creating policy $POLICY_NAME"
  aws_ iam create-policy --policy-name "$POLICY_NAME" --policy-document "file://$HERE/iam/aerostream-bench-ec2-perf.policy.json" >/dev/null
fi
aws_ iam attach-user-policy --user-name "$USER_NAME" --policy-arn "$POLICY_ARN"
echo "attached: $(aws_ iam list-attached-user-policies --user-name "$USER_NAME" --query 'AttachedPolicies[].PolicyName' --output text)"
grep -q '^\[aerostream-bench\]' ~/.aws/credentials 2>/dev/null && echo "local profile [aerostream-bench] present: keys untouched" || \
  echo "NOTE: no local [aerostream-bench] profile; create a key (aws iam create-access-key --user-name $USER_NAME) and store it with 'aws configure --profile aerostream-bench'. Never commit keys."
