
**Gobblin is a universal data ingestion framework for extracting, transforming, and loading large volume of data from a variety of data sources, e.g., databases, rest APIs, FTP/SFTP servers, filers, etc., onto Hadoop. Gobblin handles the common routine tasks required for all data ingestion ETLs, including job/task scheduling, task partitioning, error handling, state management, data quality checking, data publishing, etc. Gobblin ingests data from different data sources in the same execution framework, and manages metadata of different sources all in one place. This, combined with other features such as auto scalability, fault tolerance, data quality assurance, extensibility, and the ability of handling data model evolution, makes Gobblin an easy-to-use, self-serving, and efficient data ingestion framework**.


Getting Started
----------------

### Building Gobblin ###

Download or clone the Gobblin repository (say, into */path/to/gobblin*) and run the following command:

	$ cd /path/to/gobblin
	$ ./gradlew clean build

After Gobblin is successfully built, you will find a tarball named *gobblin-dist.tar.gz* under the project root directory. Copy the tarball out to somewhere and untar it, and you should see a directory named *gobblin-dist*, which initially contains three directories: *bin*, *conf*, and *lib*. Once Gobblin starts running, a new subdirectory *logs* will be created to store logs.

Out of the box, Gobblin can run either in standalone mode on a single box or on Hadoop MapReduce.

### Running Gobblin in Standalone Mode ###

In the standalone mode, Gobblin starts a daemon process that runs the job scheduler. The job scheduler, upon startup, will pick up job configuration files from a user-defined directory and schedule the jobs to run. Tasks of each job run in a thread pool, whose size is configurable. An environment variable named GOBBLIN\_JOB\_CONFIG_DIR must be set to point to the directory where job configuration files are stored. Note that this job configuration directory is different from *gobblin-dist/conf*, which stores Gobblin system configuration files.

	GOBBLIN_WORK_DIR\
	    task-staging\ # Staging area where data pulled by individual tasks lands
	    task-output\  # Output area where data pulled by individual tasks lands
	    job-output\   # Final output area of data pulled by jobs
	    state-store\ # Persisted job/task state store
	    metrics\     # Metrics store (in the form of metric log files), one subdirectory per job.

Before starting the Gobblin standalone daemon, make sure the environment variable JAVA_HOME is properly set. To start the Gobblin standalone daemon, use the following command:

	bin/gobblin-standalone.sh start

Run the following command to stop the Gobblin standalone daemon:

	bin/gobblin-standalone.sh stop

### Running Gobblin on Hadoop MapReduce ###

On Hadoop MapReduce, Gobblin jobs run as Hadoop MapReduce jobs, and tasks run in the mappers. It is assumed that you already have Hadoop MapReduce and HDFS setup and running somewhere. Before launching any Gobblin MR jobs, check the Gobblin framework configuration file located at *conf/gobblin-mapreduce.properties* for property *fs.uri*, which defines the file system URI used. The default value is *file:///*, which points to the local file system. Change it to the right value depending on your Hadoop/HDFS setup. For example, if you have HDFS setup locally on port 9000, then set the property as follows:

	fs.uri=hdfs://localhost:9000/

All job data and persisted job/task states will be written to the specified file system. Before launching jobs, make sure the environment variable HADOOP\_BIN\_DIR is set to point to the bin directory under the Hadoop installation directory, and the environment variable GOBBLIN\_WORK\_DIR is set to point to the working directory of Gobblin. Note that the Gobblin working directory will be created on the file system specified above. To launch a Gobblin job on Hadoop MapReduce, run the following command. The logs are located under the logs directory (*gobblin-dist/logs*).

	bin/gobblin-mapreduce.sh <job tracker URL> <file system URL> <job configuration file>

For example, if you have Hadoop and HDFS setup locally on port 9001 and 9000, respectively, then the command should look like:

	bin/gobblin-mapreduce.sh localhost:9001 hdfs://localhost:9000/ <job configuration file>

This setup will have the minimum set of jars Gobblin needs to run the job added to the Hadoop DistributedCache for use in the mappers. If a job has additional jars needed for task executions (in the mappers), those jars can also be included by using the following job configuration property in the job configuration file:

	job.jars=<comma-separated list of jars the job depends on>


Motivation
---------------------------------
Company is moving more and more towards a data-driven decision making business model. Fast increasing number of business products are driven by business insights, including funnel analysis, lead generation, campaign evaluation, audiance targeting/retargeting, etc. Significant amount of data sources are needed to facilitate those analysis. Both internal and external data has to get pulled to support our product growth. Those includes external tracking events for ads or email campaigns, third party data of survey, standardized company or member information, wikis, comments, blogs, etc.

However, business product development iterates at a very fast pace. It directly results in the following challenges of development customized data ingestion pipeline for each individual use cases.

    1. Increasing number of new sources to be integrated
    2. Each source has unique requirements and constraints
    3. Customizing ingress pipelines becomes a TTM bottleneck
    4. Knowledge of developing customized pipeline is isolated
    5. Maintaining heterogeneous pipes is an operational headache

To address these challenges, we observed the pattern of those customized ingress pipelines, and found out:

    1. Customized ingress pipes share common flow pattern
    2. Data source specific requirements can be confined


Documentation
--------------
Check out the Gobblin documentation at [http://linkedin.github.io/gobblin/wiki](http://linkedin.github.io/gobblin/wiki).

