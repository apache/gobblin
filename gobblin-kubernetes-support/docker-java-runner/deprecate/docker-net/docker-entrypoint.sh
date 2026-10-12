#!/bin/bash

set -euo pipefail

ls -lh /jars/*.jar

classpath="/app.jar"
for jar in /jars/*.jar; do
    classpath="$classpath:$jar"
done

echo "CLASSPATH: $classpath"

exec java -cp "$classpath" com.trivago.de.kubernetes.javarunner.App

