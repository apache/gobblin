Table of Contents
--------------------

[TOC]

Job Configuration Basics
--------------------
A Job configuration file is a text file with extension `.pull` or `.job` that defines the job properties that can be loaded into a Java [Properties](http://docs.oracle.com/javase/7/docs/api/java/util/Properties.html) object. Gobblin uses [commons-configuration](http://commons.apache.org/proper/commons-configuration/) to allow variable substitutions in job configuration files. You can find some example Gobblin job configuration files [here](https://github.com/apache/gobblin/tree/master/gobblin-core/src/main/resources). 

A Job configuration file typically includes the following properties, in additional to any mandatory configuration properties required by the custom [Gobblin Constructs](Gobblin-Architecture#gobblin-constructs) classes. For a complete reference of all configuration properties supported by Gobblin, please refer to [Configuration Properties Glossary](Configuration-Properties-Glossary).

* `job.name`: job name.
* `job.group`: the group the job belongs to.
* `source.class`: the `Source` class the job uses.
* `converter.classes`: a comma-separated list of `Converter` classes to use in the job. This property is optional.
* Quality checker related configuration properties: a Gobblin job typically has both row-level and task-level quality checkers specified. Please refer to [Quality Checker Properties](user-guide/Configuration-Properties-Glossary#Quality-Checker-Properties) for configuration properties related to quality checkers. 

Hierarchical Structure of Job Configuration Files
--------------------
It is often the case that a Gobblin instance runs many jobs and manages the job configuration files corresponding to those jobs. The jobs may belong to different job groups and are for different data sources. It is also highly likely that jobs for the same data source shares a lot of common properties. So it is very useful to support the following features:

* Job configuration files can be grouped by the job groups they belong to and put into different subdirectories under the root job configuration file directory.
* Common job properties shared among multiple jobs can be extracted out to a common properties file that will be applied into the job configurations of all these jobs. 

Gobblin supports the above features using a hierarchical structure to organize job configuration files under the root job configuration file directory. The basic idea is that there can be arbitrarily deep nesting of subdirectories under the root job configuration file directory. Each directory regardless how deep it is can have a single `.properties` file storing common properties that will be included when loading the job configuration files under the same directory or in any subdirectories. Below is an example directory structure.

```
root_job_config_dir/
  common.properties
  foo/
    foo1.job
    foo2.job
    foo.properties
  bar/
    bar1.job
    bar2.job
    bar.properties
    baz/
      baz1.pull
      baz2.pull
      baz.properties
```

In this example, `common.properties` will be included when loading `foo1.job`, `foo2.job`, `bar1.job`, `bar2.job`, `baz1.pull`, and `baz2.pull`. `foo.properties` will be included when loading `foo1.job` and `foo2.job` and properties set here are considered more special and will overwrite the same properties defined in `common.properties`. Similarly, `bar.properties` will be included when loading `bar1.job` and `bar2.job`, as well as `baz1.pull` and `baz2.pull`. `baz.properties` will be included when loading `baz1.pull` and `baz2.pull` and will overwrite the same properties defined in `bar.properties` and `common.properties`.

Password Encryption
--------------------
To avoid storing passwords in configuration files in plain text, Gobblin supports encryption of the password configuration properties. All such properties can be encrypted (and decrypted) using a master password. The master password is stored in a file available at runtime. The file can be on a local file system or HDFS and has restricted access.

The URI of the master password file is controlled by the configuration option `encrypt.key.loc` . By default, Gobblin will use [org.jasypt.util.password.BasicPasswordEncryptor](http://www.jasypt.org/api/jasypt/1.8/org/jasypt/util/password/BasicPasswordEncryptor.html). If you have installed the [JCE Unlimited Strength Policy](http://www.oracle.com/technetwork/java/javase/downloads/jce-7-download-432124.html), you can set
`encrypt.use.strong.encryptor=true` which will configure Gobblin to use [org.jasypt.util.password.StrongPasswordEncryptor](http://www.jasypt.org/api/jasypt/1.8/org/jasypt/util/password/StrongPasswordEncryptor.html).

Encrypted passwords can be generated using the `CLIPasswordEncryptor` tool.

    $ gradle :gobblin-utility:assemble
    $ cd build/gobblin-utility/distributions/
    $ tar -zxf gobblin-utility.tar.gz
    $ bin/gobblin_password_encryptor.sh 
      usage:
       -f <master password file>   file that contains the master password used
                                   to encrypt the plain password
       -h                          print this message
       -m <master password>        master password used to encrypt the plain
                                   password
       -p <plain password>         plain password to be encrypted
       -s                          use strong encryptor
    $ bin/gobblin_password_encryptor.sh -m Hello -p Bye
    ENC(AQWoQ2Ybe8KXDXwPOA1Ziw==)

If you are extending Gobblin and you want some of your configurations (e.g. the ones containing credentials) to support encryption, you can use `gobblin.password.PasswordManager.getInstance()` methods to get an instance of `PasswordManager`. You can then use `PasswordManager.readPassword(String)` which will transparently decrypt the value if needed, i.e. if it is in the form `ENC(...)` and a master password is provided.

Adding or Changing Job Configuration Files
--------------------
The Gobblin job scheduler in the standalone deployment monitors any changes to the job configuration file directory and reloads any new or updated job configuration files when detected. This allows adding new job configuration files or making changes to existing ones without bringing down the standalone instance. Currently, the following types of changes are monitored and supported:

* Adding a new job configuration file with a `.job` or `.pull` extension. The new job configuration file is loaded once it is detected. In the example hierarchical structure above, if a new job configuration file `baz3.pull` is added under `bar/baz`, it is loaded with properties included from `common.properties`, `bar.properties`, and `baz.properties` in that order.
* Changing an existing job configuration file with a `.job` or `.pull` extension. The job configuration file is reloaded once the change is detected. In the example above, if a change is made to `foo2.job`, it is reloaded with properties included from `common.properties` and `foo.properties` in that order.
* Changing an existing common properties file with a `.properties` extension. All job configuration files that include properties in the common properties file will be reloaded once the change is detected. In the example above, if `bar.properties` is updated, job configuration files `bar1.job`, `bar2.job`, `baz1.pull`, and `baz2.pull` will be reloaded. Properties from `bar.properties` will be included when loading `bar1.job` and `bar2.job`. Properties from `bar.properties` and `baz.properties` will be included when loading `baz1.pull` and `baz2.pull` in that order.

Note that this job configuration file change monitoring mechanism uses the `FileAlterationMonitor` of Apache's [commons-io](http://commons.apache.org/proper/commons-io/) with a custom `FileAlterationListener`. Regardless of how close two adjacent file system checks are, there are still chances that more than one files are changed between two file system checks. In case more than one file including at least one common properties file are changed between two adjacent checks, the reloading of affected job configuration files may be intermixed and applied in an order that is not desirable. This is because the order the listener is called on the changes is not controlled by Gobblin, but instead by the monitor itself. So the best practice to use this feature is to avoid making multiple changes together in a short period of time.   

Scheduled Jobs
--------------------
Gobblin ships with a job scheduler backed by a [Quartz](http://quartz-scheduler.org/) scheduler and supports Quartz's [cron triggers](http://quartz-scheduler.org/generated/2.2.1/html/qs-all/#page/Quartz_Scheduler_Documentation_Set%2Fco-trg_crontriggers.html%23). A job that is to be scheduled should have a cron schedule defined using the property `job.schedule`. Here is an example cron schedule that triggers every two minutes:

```
job.schedule=0 0/2 * * * ?
```

One Time Jobs
--------------------
Some Gobblin jobs may only need to be run once. A job without a cron schedule in the job configuration is considered a run-once job and will not be scheduled but run immediately after being loaded. A job with a cron schedule but also the property `job.runonce=true` specified in the job configuration is also treated as a run-once job and will only be run the first time the cron schedule is triggered.

Disabled Jobs
--------------------
A Gobblin job can be disabled by setting the property `job.disabled` to `true`. A disabled job will not be loaded nor scheduled to run.
