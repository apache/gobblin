Table of Contents
----------------

[TOC]

Gobblin commands & services
-----------

The Gobblin distribution comes with a script `bin/gobblin` for various admin/cli commands as well as ways to start/stop process for all modes of gobblin executions.
Here is the usage:  ( Please note that `JAVA_HOME` is required to be set prior to running the script. )

```bash
./bin/gobblin --help

gobblin.sh  <command> <params>
gobblin.sh  <service-name> <start|stop|status>

Argument Options:
    <commands>                       values: admin, cli, statestore-check, statestore-clean, historystore-manager, classpath
    <service>                        values: standalone, cluster-master, cluster-worker, aws, yarn, mapreduce, service.
    --cluster-name                   cluster name, also used by helix & other services. ( default: gobblin_cluster).
    --conf-dir <path-of-conf-dir>    default is '$GOBBLIN_HOME/conf/<mode-name>'.
    --log4j-conf <path-of-conf-file> default is '$GOBBLIN_HOME/conf/<mode-name>/log4j.properties'.
    --jt <resource manager URL>      Only for mapreduce mode: Job submission URL, if not set, taken from ${HADOOP_HOME}/conf.
    --fs <file system URL>           Only for mapreduce mode: Target file system, if not set, taken from ${HADOOP_HOME}/conf.
    --jvmopts <jvm or gc options>    String containing JVM flags to include, in addition to "-Xmx1g -Xms512m".
    --jars <csv list of extra jars>  Column-separated list of extra jars to put on the CLASSPATH.
    --enable-gc-logs                 enables gc logs & dumps.
    --help                           Display this help.
    --verbose                        Display full command used to start the process.
                                     Gobblin Version: 0.15.0
```

command line argument details:
* `--conf-dir`: specifies the path to directory containing gobblin system configuration files, ex: `application.conf` or `reference.conf`, `log4j.properties` and `quartz.properties`.
* `--log4j-conf`: specify the path of log4j config file to override the one in config directory (default is `<conf>/<gobblin-mode>/log4j.properties`. Gobblin uses [SLF4J](http://www.slf4j.org/) and the [slf4j-log4j12](http://mvnrepository.com/artifact/org.slf4j/slf4j-log4j12) binding for logging.
* `--jvmopts`: JVM parameters to override the default `-Xmx1g -Xms512m`.
* `--enable-gc-logs`: adds these JVM parameters:  ``` -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseCompressedOops -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$GOBBLIN_LOGS/ -Xloggc:$GOBBLIN_LOGS/gobblin-$GOBBLIN_MODE-gc.log ```

Gobblin Services 
-------------------
1. Mapreduce Mode:

    This mode is dependent on Hadoop (both MapReduce and HDFS) running locally or remote cluster. Before launching any Gobblin jobs on Hadoop MapReduce, check the Gobblin system configuration file located at `conf/mapreduce/application.properties` for property `fs.uri`, which defines the file system URI used. The default value is `hdfs://localhost:8020`, which points to the local HDFS on the default port 8020. Change it to the right value depending on your Hadoop/HDFS setup. For example, if you have HDFS setup somwhere on port 9000, then set the property as follows:
``` fs.uri=hdfs://<namenode host name>:9000/ ```
    * `--jt`: In mapreduce mode, provides resource manager URL
    * `--fs`: In mapreduce mode, provides file system type value for `fs.uri`

2. Standalone Mode
     

* Gobblin system configurations details can be found here: [Configuration Properties Glossary](user-guide/Configuration-Properties-Glossary).


All job data and persisted job/task states will be written to the specified file system. Before launching any jobs, make sure the environment variable `HADOOP_HOME` is set so that it can access hadoop binaries under `{HADOOP_HOME}/bin` and also working directory should be set with configuration `{gobblin.cluster.work.dir}`. Note that the Gobblin working directory will be created on the file system specified above. Below is a summary of the environment variables that may be set for deployment on Hadoop MapReduce:


This setup will have the minimum set of jars Gobblin needs to run the job added to the Hadoop `DistributedCache` for use in the mappers. If a job has additional jars needed for task executions (in the mappers), those jars can also be included by using the `--jars` option of `bin/gobblin-mapreduce.sh` or the following job configuration property in the job configuration file:

```
job.jars=<comma-separated list of jars the job depends on>
```

The `--projectversion` controls which version of the Gobblin jars to look for. Typically, this value is dynamically set during the build process. Users should use the `bin/gobblin-mapreduce.sh` script that is copied into the `gobblin-distribution-[project-version].tar.gz` file. This version of the script has the project version already set, in which case users do not need to specify the `--projectversion` parameter. If users want to use the `gobblin/bin/gobblin-mapreduce.sh` script they have to specify this parameter.

The `--logdir` parameter controls the directory where log files are written to. If not set log files are written under a the `./logs` directory.
A note on Hadoop classpath
-------------------------

When running `bin/gobblin`, the script automatically finds the classpath for the job. Although Hadoop jars are included in the Gobblin distribution, if `HADOOP_HOME` is set in the environment, Gobblin will instead use the classpath provided by the local Hadoop installation.

An important side effect of this is that (depending on the application) non-fully-qualified paths (like `/my/path`) will default to local file system if `HADOOP_HOME` is not set, while they will default to HDFS if the variable is set. When referring to local paths, it is always a good idea to use the fully qualified path (e.g. `file:///my/path`).

Gobblin ingestion applications
-----------------------------

Gobblin ingestion applications can be accessed through the command `run`:
```bash
bin/gobblin cli run [listQuickApps] [<quick-app>] -jobName <jobName> [OPTIONS]
```
For usage run `bin/gobblin cli run`.

`bin/gobblin cli run` uses [Embedded Gobblin](Gobblin-as-a-Library.md) and subclasses to run Gobblin ingestion jobs, giving CLI access to most functionality that could be achieved using `EmbeddedGobblin`. For example, the following command will run a Hello World job (it will print "Hello World 1 !" somewhere in the logs).
```bash
bin/gobblin cli run -jobName helloWorld -setTemplate resource:///templates/hello-world.template
```

Obviously, it is daunting to have to know the path to templates and exactly which configurations to set. The alternative is to use a quick app. Running:
```bash
bin/gobblin cli run listQuickApps
```
will provide with a list of available quick apps. To run a quick app:
```bash
bin/gobblin cli run <quick-app-name>
```
Quick apps may require additional arguments. For the usage of a particular app, run `bin/gobblin cli run <quick-app-name> -h`.

For example, consider the quick app distcp:
```bash
$ bin/gobblin cli run distcp -h
usage: gobblin cli run distcp [OPTIONS] <source> <target>
 -delete                         Delete files in target that don't exist
                                 on source.
 -deleteEmptyParentDirectories   If deleting files on target, also delete
                                 newly empty parent directories.
 -distributeJar <arg>
 -h,--help
 -l                              Uses log to print out erros in the base
                                 CLI code.
 -mrMode
 -setConfiguration <arg>
 -setJobTimeout <arg>
 -setLaunchTimeout <arg>
 -setShutdownTimeout <arg>
 -simulate
 -update                         Specifies files should be updated if
                                 they're different in the source.
 -useStateStore <arg>
```
This provides usage for the app distcp, as well as listing all available options. Distcp could then be run:
```bash
bin/gobblin cli run distcp file:///source/path file:///target/path
```

Developing quick apps for the CLI
--------------------------------------------

It is very easy to convert a subclass of `EmbeddedGobblin` into a quick application for Gobblin CLI. All that is needed is to implement a `EmbeddedGobblinCliFactory` which knows how instantiate the `EmbeddedGobblin` from a `CommandLine` object and annotate it with the `Alias` annotation. There are two utility classes that make this very easy:

* `PublicMethodsGobblinCliFactory`: this class will automatically infer CLI options from the public methods of a subclass of `EmbeddedGobblin`. All the developer has to do is implement the method `constructEmbeddedGobblin(CommandLine)` that calls the appropriate constructor of the desired `EmbeddedGobblin` subclass with parameters extracted from the CLI. Additionally, it is a good idea to override `getUsageString()` with the appropriate usage string. For an example, see `gobblin.runtime.embedded.EmbeddedGobblinDistcp.CliFactory`.
* `ConstructorAndPublicMethodsGobblinCliFactory`: this class does everything `PublicMethodsGobblinCliFactory` does, but it additionally automatically infers how to construct the `EmbeddedGobblin` object from a constructor annotated with `EmbeddedGobblinCliSupport`. For an example, see `gobblin.runtime.embedded.EmbeddedGobblin.CliFactory`.

Implementing new Gobblin commands
---------------------------------

To implement a new Gobblin command to list and execute using `bin/gobblin`, implement the class `gobblin.runtime.cli.CliApplication`, and annotate it with the `Alias` annotation. The Gobblin CLI will automatically find the command, and users can invoke it by the Alias value.
