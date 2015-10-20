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
CLASSPATH=${FWDIR_CONF}:${GOBBLIN_JARS}:${YARN_CONF_DIR}:${HADOOP_YARN_HOME}/lib

COMMAND="${JAVA_HOME}/bin/java -cp ${CLASSPATH} -Xmx2g -Xms1g gobblin.yarn.GobblinYarnAppLauncher"

echo "Running command:"
echo "$COMMAND"
nohup ${COMMAND} &


