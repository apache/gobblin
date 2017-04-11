Table of Contents
----------------

[TOC]

Gobblin CLI
-----------

The Gobblin distribution contains a CLI at `bin/gobblin` providing CLI access to various Gobblin applications and commands. To run:
```bash
bin/gobblin
```
The usage is `bin/gobblin <command>` where the command specifies an application to run. Running `bin/gobblin -h` provides a list of available commands.

The special command `bin/gobblin classpath` is trapped by the bash script and simply displays the full classpath that Gobblin uses.

If running from an IDE, the main method for the CLI is `gobblin.runtime.cli.GobblinCli`.

A note on Hadoop classpath
-------------------------

When running `bin/gobblin`, the script automatically finds the classpath for the job. Although Hadoop jars are included in the Gobblin distribution, if `HADOOP_HOME` is set in the environment, Gobblin will instead use the classpath provided by the local Hadoop installation.

An important side effect of this is that (depending on the application) non-fully-qualified paths (like `/my/path`) will default to local file system if `HADOOP_HOME` is not set, while they will default to HDFS if the variable is set. When referring to local paths, it is always a good idea to use the fully qualified path (e.g. `file:///my/path`).

Gobblin ingestion applications
-----------------------------

Gobblin ingestion applications can be accessed through the command `run`:
```bash
bin/gobblin run [listQuickApps] [<quick-app>] -jobName <jobName> [OPTIONS]
```
For usage run `bin/gobblin run`.

`bin/gobblin run` uses [Embedded Gobblin](Gobblin-as-a-Library.md) and subclasses to run Gobblin ingestion jobs, giving CLI access to most functionality that could be achieved using `EmbeddedGobblin`. For example, the following command will run a Hello World job (it will print "Hello World 1 !" somewhere in the logs).
```bash
bin/gobblin run -jobName helloWorld -setTemplate resource:///templates/hello-world.template
```

Obviously, it is daunting to have to know the path to templates and exactly which configurations to set. The alternative is to use a quick app. Running:
```bash
bin/gobblin run listQuickApps
```
will provide with a list of available quick apps. To run a quick app:
```bash
bin/gobblin run <quick-app-name>
```
Quick apps may require additional arguments. For the usage of a particular app, run `bin/gobblin run <quick-app-name> -h`.

For example, consider the quick app distcp:
```bash
$ bin/gobblin run distcp -h
usage: gobblin run distcp [OPTIONS] <source> <target>
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
bin/gobblin run distcp file:///source/path file:///target/path
```

Developing quick apps for the CLI
--------------------------------------------

It is very easy to convert a subclass of `EmbeddedGobblin` into a quick application for Gobblin CLI. All that is needed is to implement a `EmbeddedGobblinCliFactory` which knows how instantiate the `EmbeddedGobblin` from a `CommandLine` object and annotate it with the `Alias` annotation. There are two utility classes that make this very easy:

* `PublicMethodsGobblinCliFactory`: this class will automatically infer CLI options from the public methods of a subclass of `EmbeddedGobblin`. All the developer has to do is implement the method `constructEmbeddedGobblin(CommandLine)` that calls the appropriate constructor of the desired `EmbeddedGobblin` subclass with parameters extracted from the CLI. Additionally, it is a good idea to override `getUsageString()` with the appropriate usage string. For an example, see `gobblin.runtime.embedded.EmbeddedGobblinDistcp.CliFactory`.
* `ConstructorAndPublicMethodsGobblinCliFactory`: this class does everything `PublicMethodsGobblinCliFactory` does, but it additionally automatically infers how to construct the `EmbeddedGobblin` object from a constructor annotated with `EmbeddedGobblinCliSupport`. For an example, see `gobblin.runtime.embedded.EmbeddedGobblin.CliFactory`.

Implementing new Gobblin commands
---------------------------------

To implement a new Gobblin command to list and execute using `bin/gobblin`, implement the class `gobblin.runtime.cli.CliApplication`, and annotate it with the `Alias` annotation. The Gobblin CLI will automatically find the command, and users can invoke it by the Alias value.
