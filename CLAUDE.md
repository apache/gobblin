# gobblin

Scope: work only within this directory. Do not search parent or sibling directories unless explicitly asked.

## Purpose
Apache Gobblin open-source fork — LinkedIn's ETL framework (sources, converters, writers).

## Language & Build
Java/Scala — Gradle
```bash
./gradlew -Prelease=true build -x forkedCheckstyleMain -x spotbugsMain
./gradlew :<module>:test --tests "com.example.MyClassTest"
```
