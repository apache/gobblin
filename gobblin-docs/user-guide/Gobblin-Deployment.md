Table of Contents
--------------------

[TOC]

Gobblin Execution Modes Overview <a name="gobblin-execution-modes-Overview"></a>
--------------------------------------------------------------------------------
One important feature of Gobblin is that it can be run on different platforms. Currently, Gobblin can run in standalone mode (which runs on a single machine), and on Hadoop MapReduce mode (which runs on a Hadoop cluster). This page summarizes the different deployment modes of Gobblin. It is important to understand the architecture of Gobblin in a specific deployment mode, so this page also describes the architecture of each deployment mode.  

Gobblin supports Java 7 and up, but can only run on Hadoop 2.x. By default, Gobblin will build against Hadoop 2.x, run `./gradlew clean build`. More information on how to build Gobblin can be found [here](https://github.com/apache/gobblin/blob/master/README.md). All directories/paths referred below are relative to `gobblin-dist`.

To run gobblin in any of the following executuon mode using ```gobblin.sh```, refer [Gobblin-CLI](/gobblin-docs/user-guide/Gobblin-CLI.md) for the usage.


Standalone Architecture <a name="Standalone-Architecture"></a>
--------------------
The following diagram illustrates the Gobblin standalone architecture. In the standalone mode, a Gobblin instance runs in a single JVM and tasks run in a thread pool, the size of which is configurable. The standalone mode is good for light-weight data sources such as small databases. The standalone mode is also the default mode for trying and testing Gobblin. 

<p align="center"><img src=../../img/Gobblin-on-Single-Node.png alt="Gobblin on Single Node" width="700"></p>

In the standalone deployment, the `JobScheduler` runs as a daemon process that schedules and runs jobs using the so-called `JobLauncher`s. The `JobScheduler` maintains a thread pool in which a new `JobLauncher` is started for each job run. Gobblin ships with two types of `JobLauncher`s, namely, the `LocalJobLauncher` and `MRJobLauncher` for launching and running Gobblin jobs on a single machine and on Hadoop MapReduce, respectively. Which `JobLauncher` to use can be configured on a per-job basis, which means the `JobScheduler` can schedule and run jobs in different deployment modes. This section will focus on the `LocalJobLauncher` for launching and running Gobblin jobs on a single machine. The `MRJobLauncher` will be covered in a later section on the architecture of Gobblin on Hadoop MapReduce.  

Each `LocalJobLauncher` starts and manages a few components for executing tasks of a Gobblin job. Specifically, a `TaskExecutor` is responsible for executing tasks in a thread pool, whose size is configurable on a per-job basis. A `LocalTaskStateTracker` is responsible for keep tracking of the state of running tasks, and particularly updating the task metrics. The `LocalJobLauncher` follows the steps below to launch and run a Gobblin job:    

1. Starting the `TaskExecutor` and `LocalTaskStateTracker`.
2. Creating an instance of the `Source` class specified in the job configuration and getting the list of `WorkUnit`s to do.
3. Creating a task for each `WorkUnit` in the list, registering the task with the `LocalTaskStateTracker`, and submitting the task to the `TaskExecutor` to run.
4. Waiting for all the submitted tasks to finish.
5. Upon completion of all the submitted tasks, collecting tasks states and persisting them to the state store, and publishing the extracted data.  


MapReduce architecture <a name="MapReduce-Architecture"></a>
--------------------
The digram below shows the architecture of Gobblin on Hadoop MapReduce. As the diagram shows, a Gobblin job runs as a mapper-only MapReduce job that runs tasks of the Gobblin job in the mappers. The basic idea here is to use the mappers purely as _containers_ to run Gobblin tasks. This design also makes it easier to integrate with Yarn. Unlike in the standalone mode, task retries are not handled by Gobblin itself in the Hadoop MapReduce mode. Instead, Gobblin relies on the task retry mechanism of Hadoop MapReduce.  

<p align="center"><img src=../../img/Gobblin-on-Hadoop-MR.png alt="Gobblin on Hadoop MR" width="700"></p>

In this mode, a `MRJobLauncher` is used to launch and run a Gobblin job on Hadoop MapReduce, following the steps below:

1. Creating an instance of the `Source` class specified in the job configuration and getting the list of `WorkUnit`s to do.
2. Serializing each `WorkUnit` into a file on HDFS that will be read later by a mapper.
3. Creating a file that lists the paths of the files storing serialized `WorkUnit`s.
4. Creating and configuring a mapper-only Hadoop MapReduce job that takes the file created in step 3 as input.
5. Starting the MapReduce job to run on the cluster of choice and waiting for it to finish.
6. Upon completion of the MapReduce job, collecting tasks states and persisting them to the state store, and publishing the extracted data. 

A mapper in a Gobblin MapReduce job runs one or more tasks, depending on the number of `WorkUnit`s to do and the (optional) maximum number of mappers specified in the job configuration. If there is no maximum number of mappers specified in the job configuration, each `WorkUnit` corresponds to one task that is executed by one mapper and each mapper only runs one task. Otherwise, if a maximum number of mappers is specified and there are more `WorkUnit`s than the maximum number of mappers allowed, each mapper may handle more than one `WorkUnit`. There is also a special type of `WorkUnit`s named `MultiWorkUnit` that group multiple `WorkUnit`s to be executed together in one batch in a single mapper.

A mapper in a Gobblin MapReduce job follows the step below to run tasks assigned to it:

1. Starting the `TaskExecutor` that is responsible for executing tasks in a configurable-size thread pool and the `MRTaskStateTracker` that is responsible for keep tracking of the state of running tasks in the mapper. 
2. Reading the next input record that is the path to the file storing a serialized `WorkUnit`.
3. Deserializing the `WorkUnit` and adding it to the list of `WorkUnit`s to do. If the input is a `MultiWorkUnit`, the `WorkUnit`s it wraps are all added to the list. Steps 2 and 3 are repeated until all assigned `WorkUnit`s are deserialized and added to the list.
4. For each `WorkUnit` on the list of `WorkUnit`s to do, creating a task for the `WorkUnit`, registering the task with the `MRTaskStateTracker`, and submitting the task to the `TaskExecutor` to run. Note that the tasks may run in parallel if the `TaskExecutor` is [configured](Configuration-Properties-Glossary#taskexecutorthreadpoolsize) to have more than one thread in its thread pool.
4. Waiting for all the submitted tasks to finish.
5. Upon completion of all the submitted tasks, writing out the state of each task into a file that will be read by the `MRJobLauncher` when collecting task states.
6. Going back to step 2 and reading the next input record if available.

Master-Worker architecture
----------------------------------





AWS architecture
---------------





YARN architecture
---------------






Gobblin-As-A-Service  architecture
----------------------------------






