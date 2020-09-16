#!/bin/sh

set -euo pipefail

eval JAVA_OPTS=\"${JAVA_OPTS}\"

if [ -n "${JAVA_OPTS}" ]; then
  export JAVA_OPTS="${JAVA_OPTS_DEFAULT} ${JAVA_OPTS}"
else
  export JAVA_OPTS="${JAVA_OPTS_DEFAULT}"
fi

echo "########################################"
echo "# JAVA_OPTS=${JAVA_OPTS}"
echo "# starting now...."
echo "########################################"
set -x

java -server ${JAVA_OPTS} -jar /app/psi-app.jar