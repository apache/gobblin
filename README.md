# Apache Gobblin 
[![Build Status](https://api.travis-ci.org/apache/incubator-gobblin.svg?branch=master)](https://travis-ci.org/apache/incubator-gobblin)
[![Documentation Status](https://readthedocs.org/projects/gobblin/badge/?version=latest)](https://gobblin.readthedocs.org/en/latest/?badge=latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.gobblin/gobblin-api/badge.svg)](https://search.maven.org/search?q=g:org.apache.gobblin)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-gobblin-brightgreen.svg)](http://stackoverflow.com/questions/tagged/gobblin)
[![Join us on Slack](https://img.shields.io/badge/slack-apache--gobblin-brightgreen.svg)](https://communityinviter.com/apps/apache-gobblin/apache-gobblin)
[![codecov.io](https://codecov.io/github/apache/incubator-gobblin/branch/master/graph/badge.svg)](https://codecov.io/github/apache/incubator-gobblin)

Apache Gobblin is a universal data ingestion framework for extracting, transforming, and loading large volume of data from a variety of data sources: databases, rest APIs, FTP/SFTP servers, filers, etc., onto Hadoop. 

Apache Gobblin handles the common routine tasks required for all data ingestion ETLs, including job/task scheduling, task partitioning, error handling, state management, data quality checking, data publishing, etc. 

Gobblin ingests data from different data sources in the same execution framework, and manages metadata of different sources all in one place. This, combined with other features such as auto scalability, fault tolerance, data quality assurance, extensibility, and the ability of handling data model evolution, makes Gobblin an easy-to-use, self-serving, and efficient data ingestion framework.

# Requirements
* Java >= 1.8

If building the distribution with tests turned on:
* Maven version 3.5.3 

# Instructions to run Apache RAT (Release Audit Tool)
1. Extract the archive file to your local directory.
2. Download gradle-wrapper.jar (version 2.13) and place it in the gradle/wrapper folder. See 'Instructions to download gradle wrapper' above.
3. Run `./gradlew rat`. Report will be generated under build/rat/rat-report.html

# Instructions to build the distribution
1. Extract the archive file to your local directory.
2. Download gradle-wrapper.jar (version 2.13) and place it in the gradle/wrapper folder. See 'Instructions to download gradle wrapper' above.
3. Skip tests and build the distribution: 
Run `./gradlew build -x findbugsMain -x test -x rat -x checkstyleMain` 
The distribution will be created in build/gobblin-distribution/distributions directory.
(or)
3. Run tests and build the distribution (requires Maven): 
Run `./gradlew build` 
The distribution will be created in build/gobblin-distribution/distributions directory.

# Quick Links

  * [Gobblin documentation](https://gobblin.apache.org/docs/)
    * [Getting started guide](https://gobblin.apache.org/docs/Getting-Started/)
    * [Gobblin architecture](https://gobblin.apache.org/docs/Gobblin-Architecture/)
  * Community Slack: [Sign up](https://join.slack.com/t/apache-gobblin/shared_invite/zt-hkwu51id-aVxL3bvtLdi778YHFV1b6A) or [login with existing account](https://apache-gobblin.slack.com) to `apache-gobblin` space on Slack
  * [List of companies known to use Gobblin](https://gobblin.apache.org/docs/Powered-By/) 
  * [Sample project](https://github.com/apache/incubator-gobblin/tree/master/gobblin-example)
  * [How to build Gobblin from source code](https://gobblin.apache.org/docs/user-guide/Building-Gobblin/)
  * [Issue tracker - Apache Jira](https://issues.apache.org/jira/projects/GOBBLIN/issues/)
