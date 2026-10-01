#!/usr/bin/env bash
# Adds the AeroStream native-protocol driver to an OpenMessaging Benchmark checkout (upstream, unmodified) so it can be built.
#   usage: apply.sh <path-to-omb-checkout>
# Copies this directory to <omb>/driver-aerostream and registers it as a Maven module in the root pom.xml (idempotent).
# It also adds the driver as a dependency of benchmark-framework, which is what puts the driver's jar into the distribution.
# Build afterwards with, for example:
#   mvn -q -B clean install -DskipTests -Dlicense.skip=true -Dspotless.check.skip=true -Dspotbugs.skip=true -Dcheckstyle.skip=true \
#       -pl benchmark-framework,driver-kafka,driver-aerostream,package -am
set -euo pipefail
OMB="${1:?path to an openmessaging-benchmark checkout}"; HERE="$(cd "$(dirname "$0")" && pwd)"
[ -f "$OMB/pom.xml" ] && [ -d "$OMB/driver-api" ] || { echo "not an OMB checkout: $OMB" >&2; exit 1; }
mkdir -p "$OMB/driver-aerostream"; rm -rf "$OMB/driver-aerostream/src"   # replace the sources only; never touch build output
cp -r "$HERE/src" "$HERE/pom.xml" "$HERE/aerostream-native.yaml" "$OMB/driver-aerostream/"
grep -q '<module>driver-aerostream</module>' "$OMB/pom.xml" || \
  sed -i 's#^\( *\)<module>driver-kafka</module>#\1<module>driver-kafka</module>\n\1<module>driver-aerostream</module>#' "$OMB/pom.xml"
# OMB puts a driver's jar into the distribution only if benchmark-framework depends on it, so add that dependency too.
python3 - "$OMB/benchmark-framework/pom.xml" <<'PY'
import re, sys
p = sys.argv[1]; s = open(p).read()
if "<artifactId>driver-aerostream</artifactId>" not in s:
    m = re.search(r"([ \t]*)<dependency>\s*<groupId>\$\{project\.groupId\}</groupId>\s*<artifactId>driver-kafka</artifactId>\s*<version>\$\{project\.version\}</version>\s*</dependency>\n", s)
    if not m: sys.exit("could not find the driver-kafka dependency block in " + p)
    block = m.group(0)
    s = s.replace(block, block + block.replace("driver-kafka", "driver-aerostream"), 1)
    open(p, "w").write(s)
PY
grep -n 'driver-aerostream' "$OMB/pom.xml" "$OMB/benchmark-framework/pom.xml"
