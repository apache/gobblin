# Apache Gobblin [![Build Status](https://api.travis-ci.org/apache/incubator-gobblin.svg?branch=master)](https://travis-ci.org/apache/incubator-gobblin) [![Documentation Status](https://readthedocs.org/projects/gobblin/badge/?version=latest)](http://gobblin.readthedocs.org/en/latest/?badge=latest) [![codecov.io](https://codecov.io/github/apache/incubator-gobblin/branch/master/graph/badge.svg)](https://codecov.io/github/apache/incubator-gobblin)

Apache Gobblin is a universal data ingestion framework for extracting, transforming, and loading large volume of data from a variety of data sources, e.g., databases, rest APIs, FTP/SFTP servers, filers, etc., onto Hadoop. Apache Gobblin handles the common routine tasks required for all data ingestion ETLs, including job/task scheduling, task partitioning, error handling, state management, data quality checking, data publishing, etc. Gobblin ingests data from different data sources in the same execution framework, and manages metadata of different sources all in one place. This, combined with other features such as auto scalability, fault tolerance, data quality assurance, extensibility, and the ability of handling data model evolution, makes Gobblin an easy-to-use, self-serving, and efficient data ingestion framework.

# Requirements
* Java >= 1.8 
* gradle-wrapper.jar version 2.13

If building the distribution with tests turned on:
* Maven version 3.5.3 

# Instructions to download gradle wrapper
Run the following command for downloading the gradle-wrapper.jar from Gobblin git repository to gradle/wrapper directory.

wget --no-check-certificate -P gradle/wrapper https://github.com/apache/incubator-gobblin/raw/0.12.0/gradle/wrapper/gradle-wrapper.jar
(or)
curl --insecure -L https://github.com/apache/incubator-gobblin/raw/0.12.0/gradle/wrapper/gradle-wrapper.jar > gradle/wrapper/gradle-wrapper.jar

Alternatively, you can download it manually from: 
https://github.com/apache/incubator-gobblin/blob/0.12.0/gradle/wrapper/gradle-wrapper.jar

Make sure that you download it to gradle/wrapper directory. 

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

  * Documentation: Check out the [Gobblin documentation](http://gobblin.readthedocs.org/en/latest/) for a complete description of Gobblin's features
  * Powered By: Check out the [list of companies](http://gobblin.readthedocs.io/en/latest/Powered-By/) known to use Gobblin
  * Architecture: The [Gobblin Architecture](http://gobblin.readthedocs.io/en/latest/Gobblin-Architecture/) page has a full explanation of Gobblin's architecture
  * Getting Started with Gobblin: Refer to the [Getting Started Guide](http://gobblin.readthedocs.org/en/latest/Getting-Started/) on how to get started with Gobblin
  * Building Gobblin (from master branch): Refer to the page [Building Gobblin](http://gobblin.readthedocs.io/en/latest/user-guide/Building-Gobblin/) for directions on how to build Gobblin
  * Javadocs: The full JavaDocs for each released version of Gobblin can be found [here](http://linkedin.github.io/gobblin/javadoc/latest/)
  * Gobblin chat room: Gitter chat room for Gobblin developers and users [here](https://gitter.im/gobblin/Lobby/)
  * Gobblin Issue Tracker can be found [here](https://issues.apache.org/jira/projects/GOBBLIN/issues/)
