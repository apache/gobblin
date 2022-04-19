# Apache Gobblin 
[![Build Status](https://github.com/apache/gobblin/actions/workflows/build_and_test.yaml/badge.svg?branch=master)](https://travis-ci.org/apache/gobblin)
[![Documentation Status](https://readthedocs.org/projects/gobblin/badge/?version=latest)](https://gobblin.readthedocs.org/en/latest/?badge=latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.gobblin/gobblin-api/badge.svg)](https://search.maven.org/search?q=g:org.apache.gobblin)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-gobblin-brightgreen.svg)](http://stackoverflow.com/questions/tagged/gobblin)
[![Join us on Slack](https://img.shields.io/badge/slack-apache--gobblin-brightgreen.svg)]( https://join.slack.com/t/apache-gobblin/shared_invite/zt-vqgdztup-UUq8S6gGJqE6L5~9~JelNg)
[![codecov.io](https://codecov.io/github/apache/gobblin/branch/master/graph/badge.svg)](https://codecov.io/github/apache/gobblin)

Apache Gobblin is a highly scalable data management solution for structured and byte-oriented data in heterogeneous data ecosystems. 

### Capabilities
- Ingestion and export of data from a variety of sources and sinks into and out of the data lake. Gobblin is optimized and designed for ELT patterns with inline transformations on ingest (small t).
- Data Organization within the lake (e.g. compaction, partitioning, deduplication)
- Lifecycle Management of data within the lake (e.g. data retention)
- Compliance Management of data across the ecosystem (e.g. fine-grain data deletions)

### Highlights
- Battle tested at scale: Runs in production at petabyte-scale at companies like LinkedIn, PayPal, Verizon etc.
- Feature rich: Supports task partitioning, state management for incremental processing, atomic data publishing, data quality checking, job scheduling, fault tolerance etc.
- Supports stream and batch execution modes 
- Control Plane (Gobblin-as-a-service) supports programmatic triggering and orchestration of data plane operations. 

### Common Patterns used in production
- Stream / Batch ingestion of Kafka to Data Lake (HDFS, S3, ADLS)
- Bulk-loading serving stores from the Data Lake (e.g. HDFS -> Couchbase)
- Support for data sync across Federated Data Lake (HDFS <-> HDFS, HDFS <-> S3, S3 <-> ADLS)
- Integrate external vendor API-s (e.g. Salesforce, Dynamics etc.) with data store (HDFS, Couchbase etc)
- Enforcing Data retention policies and GDPR deletion on HDFS / ADLS


### Apache Gobblin is NOT
- A general purpose data transformation engine like Spark or Flink. Gobblin can delegate complex-data processing tasks to Spark, Hive etc. 
- A data storage system like Apache Kafka or HDFS. Gobblin integrates with these systems as sources or sinks. 
- A general-purpose workflow execution system like Airflow, Azkaban, Dagster, Luigi. 


# Requirements
* Java >= 1.8

If building the distribution with tests turned on:
* Maven version 3.5.3 

# Instructions to run Apache RAT (Release Audit Tool)
1. Extract the archive file to your local directory.
2. Run `./gradlew rat`. Report will be generated under build/rat/rat-report.html

# Instructions to build the distribution
1. Extract the archive file to your local directory.
2. Skip tests and build the distribution: 
Run `./gradlew build -x findbugsMain -x test -x rat -x checkstyleMain` 
The distribution will be created in build/gobblin-distribution/distributions directory.
(or)
3. Run tests and build the distribution (requires Maven): 
Run `./gradlew build` 
The distribution will be created in build/gobblin-distribution/distributions directory.

# Quick Links

  * [Gobblin documentation](https://gobblin.apache.org/docs/)
    * [Running Gobblin on Docker from your laptop](https://github.com/apache/gobblin/blob/master/gobblin-docs/user-guide/Docker-Integration.md)
    * [Getting started guide](https://gobblin.apache.org/docs/Getting-Started/)
    * [Gobblin architecture](https://gobblin.apache.org/docs/Gobblin-Architecture/)
  * Community Slack: [Get your invite](https://join.slack.com/t/apache-gobblin/shared_invite/zt-1723tsdhd-ITcAEsaQNpQvuQUFOgfHbQ)
  * [List of companies known to use Gobblin](https://gobblin.apache.org/docs/Powered-By/) 
  * [Sample project](https://github.com/apache/gobblin/tree/master/gobblin-example)
  * [How to build Gobblin from source code](https://gobblin.apache.org/docs/user-guide/Building-Gobblin/)
  * [Issue tracker - Apache Jira](https://issues.apache.org/jira/projects/GOBBLIN/issues/)
