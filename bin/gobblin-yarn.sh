#!/usr/bin/env bash

FWDIR="$(cd `dirname $0`/..; pwd)"
FWDIR_LIB=${FWDIR}/lib
FWDIR_CONF=${FWDIR}/conf

. ${FWDIR_CONF}/gobblin-env.sh

for jarFile in `ls ${FWDIR_LIB}/*`
do
  GOBBLIN_JARS=${GOBBLIN_JARS}:${jarFile}
done

export HADOOP_USER_CLASSPATH_FIRST=true
CLASSPATH=${GOBBLIN_JARS}:${HADOOP_CONF_DIR}

COMMAND="${JAVA_HOME}/bin/java -cp ${CLASSPATH} -Xmx2g -Xms1g gobblin.yarn.GobblinYarnAppLauncher"

echo "Running command:"
echo "$COMMAND"
nohup ${COMMAND} &


