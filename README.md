# Gobblin

Gobblin is a universal data ingestion framework for extracting, transforming, and loading large volume of data from a variety of data sources, e.g., databases, rest APIs, FTP/SFTP servers, filers, etc., onto Hadoop. Gobblin handles the common routine tasks required for all data ingestion ETLs, including job/task scheduling, task partitioning, error handling, state management, data quality checking, data publishing, etc. Gobblin ingests data from different data sources in the same execution framework, and manages metadata of different sources all in one place. This, combined with other features such as auto scalability, fault tolerance, data quality assurance, extensibility, and the ability of handling data model evolution, makes Gobblin an easy-to-use, self-serving, and efficient data ingestion framework.

## Documentation

Check out the Gobblin documentation at [https://github.com/linkedin/gobblin/wiki](https://github.com/linkedin/gobblin/wiki).

## Getting Started

### Building Gobblin

Download or clone the Gobblin repository (say, into `/path/to/gobblin`) and run the following command:

	$ cd /path/to/gobblin
	$ ./gradlew clean build

After Gobblin is successfully built, you will find a tarball named `gobblin-dist.tar.gz` under the project root directory. Copy the tarball out to somewhere and untar it, and you should see a directory named `gobblin-dist`, which initially contains three directories: `bin`, `conf`, and `lib`. Once Gobblin starts running, a new subdirectory `logs` will be created to store logs.

### Building against a Specific Hadoop Version

Gobblin uses the Hadoop core libraries to talk to HDFS as well as to run on Hadoop MapReduce. Because the protocols have changed in different versions of Hadoop, you must build Gobblin against the same version that your cluster runs. By default, Gobblin is built against version 1.2.1 of Hadoop 1, and against version 2.3.0 of Hadoop 2, but you can choose to build Gobblin against a different version of Hadoop.

The build command above will build Gobblin against the default version 1.2.1 of Hadoop 1. To build Gobblin against a different version of Hadoop 1, e.g., 1.2.0, run the following command:

	$ ./gradlew clean build -PhadoopVersion=1.2.0

To build Gobblin against the default version (2.3.0) of Hadoop 2, run the following command:

	$ ./gradlew clean build -PuseHadoop2

To build Gobblin against a different version of Hadoop 2, e.g., 2.2.0, run the following command:

	$ ./gradlew clean build -PuseHadoop2 -PhadoopVersion=2.2.0


### Running Gobblin

Out of the box, Gobblin can run either in standalone mode on a single box or on Hadoop MapReduce. Please refer to the page [Gobblin Deployment](https://github.com/linkedin/gobblin/wiki/Gobblin%20Deployment) in the documentation for an overview of the deployment modes and how to run Gobblin in different modes.

### Running the Examples

Please refer to the page [Getting Started](https://github.com/linkedin/gobblin/wiki/Getting%20Started)
in the documentation on how to run the examples.

## Configuration

Please refer to the page [Configuration Glossary](https://github.com/linkedin/gobblin/wiki/Configuration%20Properties%20Glossary)
in the documentation for an overview on the configuration properties of Gobblin.
