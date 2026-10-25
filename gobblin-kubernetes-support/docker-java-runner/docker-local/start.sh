#!/bin/bash

set -euo pipefail

echo $@

readonly JARS_LOCATION="$1"
shift

ls -lh "${JARS_LOCATION}"/*.jar

classpath="/runner.jar"
for jar in "${JARS_LOCATION}"/*.jar; do
    classpath="${classpath}:${jar}"
done
echo "classpath: $classpath"

java -cp "$classpath" com.trivago.de.kubernetes.javarunner.runner.Runner $@ || true

#echo "entering infinite loop"
#while true; do
#    sleep 1000
#done

