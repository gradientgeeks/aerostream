#!/usr/bin/env bash
# Installs software on both nodes (in parallel) and ships the AeroStream image built from the local checkout.
set -euo pipefail; source "$(dirname "$0")/lib.sh"; state_load
ROOT="$(cd "$HERE/../.." && pwd)"
SHA=$(git -C "$ROOT" rev-parse --short HEAD); DIRTY=$(git -C "$ROOT" status --porcelain -- rust-broker go-controller entrypoint.sh Dockerfile | wc -l)
IMG="aerostream:aws-bench-$SHA${DIRTY:+-dirty$DIRTY}"; [ "$DIRTY" = 0 ] && IMG="aerostream:aws-bench-$SHA"
IMG="${AERO_IMAGE:-$IMG}"   # AERO_IMAGE=<local tag> tests exactly that image and skips the build
if ! docker image inspect "$IMG" >/dev/null 2>&1; then log "building $IMG from the local checkout"; docker build -q -t "$IMG" -f "$ROOT/Dockerfile" "$ROOT" >/dev/null; fi
state_set AERO_IMAGE "$IMG"; state_set GIT_SHA "$SHA-dirty$DIRTY"

broker_setup() {
  ssh_broker 'set -e; sudo dnf install -y -q docker sysstat >/dev/null; sudo systemctl enable --now docker; sudo usermod -aG docker ec2-user
    mkdir -p ~/bench/broker
    DEV=$(lsblk -dpno NAME,MODEL | awk "/Instance Storage/{print \$1; exit}")
    if [ -n "$DEV" ]; then sudo mkfs.xfs -f "$DEV" >/dev/null; sudo mkdir -p /mnt/nvme; sudo mount -o noatime "$DEV" /mnt/nvme; sudo mkdir -p /mnt/nvme/data; echo "local NVMe $DEV mounted at /mnt/nvme"
    else echo "WARNING: no instance-store NVMe found; using the root volume"; fi'
  scp_to "$BROKER_PUBLIC_IP" "$HERE/remote/broker-node.sh" "/home/ec2-user/bench/"
  log "shipping AeroStream image ($IMG) to the broker node"
  docker save "$IMG" | gzip -1 | ssh "${SSH_OPTS[@]}" "$SSH_USER@$BROKER_PUBLIC_IP" 'gunzip | sudo docker load >/dev/null && echo '"$IMG"' > ~/bench/aerostream.image'
  ssh_broker "chmod +x ~/bench/broker-node.sh"
  : > "$RESULTS_DIR/image-digests.txt"
  case " $SYSTEMS " in *" kafka "*) ssh_broker "sudo docker pull -q '$KAFKA_IMAGE' >/dev/null && sudo docker image inspect '$KAFKA_IMAGE' --format '{{index .RepoDigests 0}}'" >> "$RESULTS_DIR/image-digests.txt" 2>&1 ;; esac
  case " $SYSTEMS " in *" redpanda "*) ssh_broker "sudo docker pull -q '$REDPANDA_IMAGE' >/dev/null && sudo docker image inspect '$REDPANDA_IMAGE' --format '{{index .RepoDigests 0}}'" >> "$RESULTS_DIR/image-digests.txt" 2>&1 ;; esac
  echo "$IMG $(docker image inspect "$IMG" --format '{{.Id}}')" >> "$RESULTS_DIR/image-digests.txt"
  log "broker node ready"
}
client_setup() {
  ssh_client 'set -e; sudo dnf install -y -q docker git java-17-amazon-corretto-devel sysstat python3 >/dev/null; mkdir -p ~/bench/results ~/bench/omb
    # OMB enforces Maven >= 3.8.6 but Amazon Linux ships 3.8.4, so always use the pinned upstream binary
    [ -x /opt/apache-maven-3.9.9/bin/mvn ] || curl -fsSL https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz | sudo tar xz -C /opt
    /opt/apache-maven-3.9.9/bin/mvn -version | head -1'
  scp_to "$CLIENT_PUBLIC_IP" "$HERE/remote/client-node.sh" "$HERE/drivers" "$HERE/workloads" "/home/ec2-user/bench/"
  log "client: building OpenMessaging Benchmark @ ${OMB_COMMIT:0:9} (a few minutes)"
  ssh_client "set -e; cd ~/bench/omb; [ -d src ] || { git init -q src && git -C src remote add origin '$OMB_REPO' && git -C src fetch -q --depth 1 origin '$OMB_COMMIT' && git -C src checkout -q FETCH_HEAD; }
    cd src; /opt/apache-maven-3.9.9/bin/mvn -q -B clean install -DskipTests -Dlicense.skip=true -Dspotless.check.skip=true -Dspotbugs.skip=true -pl benchmark-framework,driver-kafka,package -am
    mkdir -p ../dist; tar -xzf package/target/openmessaging-benchmark-*-bin.tar.gz -C ../dist --strip-components=1; chmod +x ~/bench/client-node.sh; ls ../dist/bin"
  log "client node ready"
}
if [ "$TOPOLOGY" = single ]; then
  { broker_setup && client_setup && scp_to "$CLIENT_PUBLIC_IP" "$HERE/remote/split-cpus.py" "/home/ec2-user/bench/" && ssh_client 'python3 ~/bench/split-cpus.py'; } > "$RESULTS_DIR/bootstrap-node.log" 2>&1 \
    || { tail -8 "$RESULTS_DIR/bootstrap-node.log" >&2; die "bootstrap FAILED (see bootstrap-node.log)"; }
  log "cores split: $(grep -o 'BROKER_CPUSET.*' "$RESULTS_DIR/bootstrap-node.log" | tail -1)"
else
  broker_setup > "$RESULTS_DIR/bootstrap-broker.log" 2>&1 & BP=$!
  client_setup > "$RESULTS_DIR/bootstrap-client.log" 2>&1 & CP=$!
  FAIL=0; wait $BP || { FAIL=1; log "broker bootstrap FAILED (see bootstrap-broker.log)"; }; wait $CP || { FAIL=1; log "client bootstrap FAILED (see bootstrap-client.log)"; }
  [ $FAIL = 0 ] || { tail -5 "$RESULTS_DIR"/bootstrap-*.log >&2; exit 1; }
fi
log "bootstrap complete"
