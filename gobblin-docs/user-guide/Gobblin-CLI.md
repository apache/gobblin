Table of Contents
----------------

[TOC]

Gobblin Commands & Execution Modes
-----------

The Gobblin distribution comes with a script `./bin/gobblin` for all commands and services.
Here is the usage:  

```bash
Usage:
gobblin.sh  cli     <cli-command>    <params>
gobblin.sh  service <execution-mode> <start|stop|status>

Use "gobblin <cli|service> --help" for more information.         (Gobblin Version: 0.15.0)
```

For Gobblin CLI commands, run following:  
```bash
Usage:
gobblin.sh  cli     <cli-command>    <params>

options:
    cli-commands:
                passwordManager             Encrypt or decrypt strings for the password manager.
                decrypt                     Decryption utilities
                run                         Run a Gobblin application.
                config                      Query the config library
                jobs                        Command line job info and operations
                stateMigration              Command line tools for migrating state store
                job-state-to-json           To convert Job state to JSON
                cleaner                     Data retention utility
                keystore                    Examine JCE Keystore files
                watermarks                  Inspect streaming watermarks
                job-store-schema-manager    Database job history store schema manager

    --conf-dir <gobblin-conf-dir-path> Gobblon config path. default is '$GOBBLIN_HOME/conf/<exe-mode-name>'.
    --log4j-conf <path-of-log4j-file>  default is '<gobblin-conf-dir-path>/<execution-mode>/log4j.properties'.
    --jvmopts <jvm or gc options>      String containing JVM flags to include, in addition to "-Xmx1g -Xms512m".
    --jars <csv list of extra jars>    Column-separated list of extra jars to put on the CLASSPATH.
    --enable-gc-logs                   enables gc logs & dumps.
    --show-classpath                   prints gobblin runtime classpath.
    --help                             Display this help.
    --verbose                          Display full command used to start the process.
                                       Gobblin Version: 0.15.0
```


Argument details:
* `--conf-dir`: specifies the path to directory containing gobblin system configuration files, like `application.conf` or `reference.conf`, `log4j.properties` and `quartz.properties`.
* `--log4j-conf`: specify the path of log4j config file to override the one in config directory (default is `<conf>/<gobblin-mode>/log4j.properties`. Gobblin uses [SLF4J](http://www.slf4j.org/) and the [slf4j-log4j12](http://mvnrepository.com/artifact/org.slf4j/slf4j-log4j12) binding for logging.
* `--jvmopts`: to specify any JVM parameters, default is `-Xmx1g -Xms512m`.
* `--enable-gc-logs`: adds GC options to JVM parameters:  ``` -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseCompressedOops -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$GOBBLIN_LOGS/ -Xloggc:$GOBBLIN_LOGS/gobblin-$GOBBLIN_MODE-gc.log ```
* `--show-classpath`: It prints the full value of the classpath that gobblin uses.
* all other arguments are sell-explanatory.

Gobblin Commands
-------------------

Gobblin provides following CLI commands:

    Available commands:
        job-state-to-json	        To convert Job state to JSON
        jobs		                Command line job info and operations
        passwordManager		        Encrypt or decrypt strings for the password manager.
        run		                    Run a Gobblin application.
        decrypt		                Decryption utilities
        job-store-schema-manager	Database job history store schema manager
        stateMigration		        Command line tools for migrating state store
        keystore		            Examine JCE Keystore files
        config		                Query the config library
        watermarks		            Inspect streaming watermarks
        cleaner		                Data retention utility

Details on how to use `run` command: 
 
Gobblin ingestion applications can be accessed through the following command:

    gobblin cli run [listQuickApps] [<quick-app>] -jobName <jobName> [OPTIONS]

For usage run `./bin/gobblin cli run`.

`gobblin cli run` uses [Embedded Gobblin](Gobblin-as-a-Library.md) and subclasses to run Gobblin ingestion jobs, giving CLI access to most functionality that could be achieved using `EmbeddedGobblin`. For example, the following command will run a Hello World job (it will print "Hello World 1 !" somewhere in the logs).
    
    gobblin cli run -jobName helloWorld -setTemplate resource:///templates/hello-world.template

Obviously, it is daunting to have to know the path to templates and exactly which configurations to set. The alternative is to use a quick app. Running:

    gobblin cli run listQuickApps

will provide with a list of available quick apps. To run a quick app:
    
    gobblin cli run <quick-app-name>

Quick apps may require additional arguments. For the usage of a particular app, run `bin/gobblin cli run <quick-app-name> -h`.

For example, consider the quick app distcp:
```bash
$ gobblin cli run distcp -h
usage: gobblin cli run distcp [OPTIONS] <source> <target>
 -delete                         Delete files in target that don't exist
                                 on source.
 -deleteEmptyParentDirectories   If deleting files on target, also delete
                                 newly empty parent directories.
 -distributeJar <arg>
 -h,--help
 -l                              Uses log to print out erros in the base CLI code.
 -mrMode
 -setConfiguration <arg>
 -setJobTimeout <arg>
 -setLaunchTimeout <arg>
 -setShutdownTimeout <arg>
 -simulate
 -update                         Specifies files should be updated if they're different in the source.
 -useStateStore <arg>
```
This provides usage for the app distcp, as well as listing all available options. Distcp could then be run:
```bash
gobblin cli run distcp file:///source/path file:///target/path
```

Developing quick apps for the CLI
--------------------------------------------
    
It is very easy to convert a subclass of `EmbeddedGobblin` into a quick application for Gobblin CLI. All that is needed is to implement a `EmbeddedGobblinCliFactory` which knows how instantiate the `EmbeddedGobblin` from a `CommandLine` object and annotate it with the `Alias` annotation. There are two utility classes that make this very easy:

* `PublicMethodsGobblinCliFactory`: this class will automatically infer CLI options from the public methods of a subclass of `EmbeddedGobblin`. All the developer has to do is implement the method `constructEmbeddedGobblin(CommandLine)` that calls the appropriate constructor of the desired `EmbeddedGobblin` subclass with parameters extracted from the CLI. Additionally, it is a good idea to override `getUsageString()` with the appropriate usage string. For an example, see `gobblin.runtime.embedded.EmbeddedGobblinDistcp.CliFactory`.
* `ConstructorAndPublicMethodsGobblinCliFactory`: this class does everything `PublicMethodsGobblinCliFactory` does, but it additionally automatically infers how to construct the `EmbeddedGobblin` object from a constructor annotated with `EmbeddedGobblinCliSupport`. For an example, see `gobblin.runtime.embedded.EmbeddedGobblin.CliFactory`.

Implementing new Gobblin commands
---------------------------------

To implement a new Gobblin command to list and execute using `./bin/gobblin`, implement the class `gobblin.runtime.cli.CliApplication`, and annotate it with the `Alias` annotation. The Gobblin CLI will automatically find the command, and users can invoke it by the Alias value.



Gobblin Service Execution Modes ( as Daemon )
-------------------

For more info on Gobblin service execution modes, run `bin/gobblin service --help`: 
```bash
Usage:
gobblin.sh  service <execution-mode> <start|stop|status>

Argument Options:
    <execution-mode>                   standalone, cluster-master, cluster-worker, aws,
                                                 yarn, mapreduce, service-manager.

    --conf-dir <gobblin-conf-dir-path> Gobblon config path. default is '$GOBBLIN_HOME/conf/<exe-mode-name>'.
    --log4j-conf <path-of-log4j-file>  default is '<gobblin-conf-dir-path>/<execution-mode>/log4j.properties'.    --jvmopts <jvm or gc options>      String containing JVM flags to include, in addition to "-Xmx1g -Xms512m".
    --jars <csv list of extra jars>    Column-separated list of extra jars to put on the CLASSPATH.
    --enable-gc-logs                   enables gc logs & dumps.
    --show-classpath                   prints gobblin runtime classpath.
    --cluster-name                     Name of the cluster to be used by helix & other services. ( default: gobblin_cluster).
    --jt <resource manager URL>        Only for mapreduce mode: Job submission URL, if not set, taken from ${HADOOP_HOME}/conf.
    --fs <file system URL>             Only for mapreduce mode: Target file system, if not set, taken from ${HADOOP_HOME}/conf.
    --help                             Display this help.
    --verbose                          Display full command used to start the process.
                                       Gobblin Version: 0.15.0
```


1. Standalone:
    This mode starts all Gobblin services in single JVM on a single node. This mode is useful for development and light weight usage: 
    ```
    gobblin service standalone start
    ```
    For more details and architecture on each execution mode, refer [Standalone-Deployment](/gobblin-docs/user-guide/Gobblin-Deployment.md)
    
2. Mapreduce:

    This mode is dependent on Hadoop (both MapReduce and HDFS) running locally or remote cluster. Before launching any Gobblin jobs on Hadoop MapReduce, check the Gobblin system configuration file located at `conf/mapreduce/application.properties` for property `fs.uri`, which defines the file system URI used. The default value is `hdfs://localhost:8020`, which points to the local HDFS on the default port 8020. Change it to the right value depending on your Hadoop/HDFS setup. For example, if you have HDFS setup somwhere on port 9000, then set the property as follows:
``` fs.uri=hdfs://<namenode host name>:9000/ ```
    * `--jt`: resource manager URL
    * `--fs`: file system type value for `fs.uri`
    
    This mode will have the minimum set of Gobblin jars, selected using `libs/gobblin-<module_name>-$GOBBLIN_VERSION.jar`, which is passed as `-libjar` to hadoop command while running the job. These same set of jars also gets added to the Hadoop `DistributedCache` for use in the mappers. If a job has additional jars needed for task executions (in the mappers), those jars can also be included by using the `--jars` option or the following job configuration property in the job configuration file:
    ```
    job.jars=<comma-separated list of jars the job depends on>
    ```
    if `HADOOP_HOME` is set in the environment, Gobblin will add result of `hadoop classpath` prior to default `GOBBLIN_CLASSPATH` to give them precedence while running `bin/gobblin`. 
    
    All job data and persisted job/task states will be written to the specified file system. Before launching any jobs, make sure the environment variable `HADOOP_HOME` is set so that it can access hadoop binaries under `{HADOOP_HOME}/bin` and also working directory should be set with configuration `{gobblin.cluster.work.dir}`. Note that the Gobblin working directory will be created on the file system specified above.
    
    An important side effect of this is that (depending on the application) non-fully-qualified paths (like `/my/path`) will default to local file system if `HADOOP_HOME` is not set, while they will default to HDFS if the variable is set. When referring to local paths, it is always a good idea to use the fully qualified path (e.g. `file:///my/path`).
    

3. Cluster Mode (master & worker)
    This is a cluster mode consist of master and worker process. 
    ```
        gobblin service cluster-master start
        gobblin service cluster-worker start
    ```
    
4. AWS
   This mode starts Gobblin on AWS cloud cluster.
    ```
       gobblin service aws start
    ```

5. YARN
    This mode starts Gobblin on YARN cluster.
    ```
       gobblin service yarn start
    ```


Gobblin System Configurations
----------------------

Following values can be override by setting it in `gobblin-env.sh`

`GOBBLIN_LOGS` : by default the logs are written to `$GOBBLIN_HOME/logs`, it can be override by setting `GOBBLIN_LOGS`\
`GOBBLIN_VERSION` : by default gobblin version is set by the build process, it can be override by setting `GOBBLIN_VERSION`\


All Gobblin system configurations details can be found here: [Configuration Properties Glossary](user-guide/Configuration-Properties-Glossary).

