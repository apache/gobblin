GOBBLIN 0.7.0
-------------

#### Created Date: 05/11/2016

## Highlights

* Gobblin has deprecated support for Hadoop 1.x.x - Gobblin will build with Hadoop 2.3.0 by default, and will throw an exception if users try to build against 1.x.x versions
* Gobblin can now write data to Relational Databases via a new `JDBCWriter`; the Writer is capable of writing to any RDMS connectable via JDBC, however, our testing has focused on writing to MySQL
    * Documentation: http://gobblin.readthedocs.io/en/latest/user-guide/Gobblin-JDBC-Writer/
* Gobblin can now automatically register published datasets into Hive
    * Documentation: http://gobblin.readthedocs.io/en/latest/user-guide/Hive-Registration/
* Gobblin has a new adaptor that can do a distributed copy of data between Hadoop clusters (many improvements over Hadoop's Distcp Tool); it can also has upcoming support for replicating Hive tables between different Metastores
    * Documentation: http://gobblin.readthedocs.io/en/latest/adaptors/Gobblin-Distcp/
* Gobblin has a new configuration management system that is focused on configuring datasets in a reliable and sensible way; the system is largely based on Typesafe's Config library
    * Documentation: http://gobblin.readthedocs.io/en/latest/adaptors/Gobblin-Distcp/

## NEW FEATURES

* [Hive Registration] [PR 651] Hive registration initial commit
* [Runtime] [PR 674] Lifecycle Events for JobListeners
* [Hive Registration] [PR 684] Add inline Hive registration to Gobblin job
* [SFTP] [PR 686] Modified the SFTP extractor to also use password for connecting
* [Hive Registration] [PR 701] Reg compacted datasets in Hive
* [Retention] [PR 716] Use configClient to configure retention jobs
* [Hive Distcp] [PR 728] Hive dataset implementation for distcp.
* [Hive Distcp] [PR 749] Hivesource copyentity
* [Hive Distcp] [PR 757] Hive distcp: check target metastore to perform table syncs.
* [Hive Registration] [PR 773] Refactoring Hive registration to allow query-based approach
* [Config Management] [PR 774] Add HDFS config deployment tool
* [Avro to ORC] [PR 780] Flatten Avro Schema to make it optimal for ORC
* [Hive Distcp] [PR 801] Implemented Hive registration steps in Hive distcp.
* [Hive Registration] [PR 803] Add snapshot Hive registration policy
* [YARN] [PR 828] Add zookeeper based job lock for gobblin yarn
* [Kafka] [PR 835] Add kafka simple json source
* [Metrics] [PR 863] Metric reporters (Graphite, InfluxDB)
* [JDBC Writer] [PR 893] JDBC Writer
* [Config Management] [PR 928] Substitution of system and env variable in config management
* [Core] [PR 942] Allow disabling state store.
* [Avro to ORC] [PR 972] Avro2orc Source/Converter/Extractor/Publisher

## BUG FIXES

* [Distcp] [PR 645] Fix parent directory creation in distcp-ng
* [Admin Dashboard] [PR 646] Downgraded jetty version to be java 7 compatible
* [Admin Dashboard] [PR 648] Excluded old version of servlet-api artifact from Hadoop 2 dependencies
* [State Store] [PR 655] Fix hanging StateStoreCleaner
* [Publisher] [PR 657] Issue #561 - fix for BaseDataPublisher to mark WorkingState correctly
* [Core] [PR 661] Change ParallelRunner.close to wait for all futures to finish
* [Core] [PR 663] ParallelRunner catches exceptions correctly and has failure policies.
* [Build] [PR 664] Fix broken Gobblin version resolution ( fixes #662 )
* [Build] [PR 665] Gobblin-compaction tarball doesn't contain gobblin-compaction.jar
* [Core] [PR 670] Fixing FindBugs warnings
* [Core] [PR 676] Ensure that parallel runner waits for the underlying tasks to finish
* [Core] [PR 677] Fix race condition in FsStateStore
* [Compaction] [PR 680] Fix a ConcurrentModificationException in MRCompactor
* [Admin Dashboard] [PR 681] Fixed off by one issue when listing the job executions in Admin UI
* [Config Management] [PR 682] various bug fixes when integrate test with hdfs store
* [Core] [PR 690] Add missing jar to MR runner script
* [Distcp] [PR 691] Fix permissions for directories in distcp.
* [Core] [PR 700] Add missing jars to gobblin mapreduce runner, sort.
* [Core] [PR 706] Fixing CliOptions config file fs
* [Core] [PR 722] Fixing FindBugs warnings in gobblin-compaction
* [Build] [PR 743] Fixing skipTestGroup option
* [Build] [PR 775] Fix javadoc warnings by only adding linksOffline to projects that the current project depends on.
* [Core] [PR 797] Fixing Fork + Task Retry Logic #776
* [Distcp] [PR 884] Fix issue with replicating owner and permission of system directories in distcp
* [Data Management] [PR 887] Fix NPE in DateTimeDatasetVersionFinder
* [Data Management] [PR 888] Fix NPE in datasetversion finder
* [Core] [PR 903] The underlying Avro CodecFactory only matches lowercase codecs, so we should make sure they are lowercase before trying to find one
* [Compaction] [PR 952] Unified way to execute Hive and MR-based compaction jobs
* [Core] [PR 958] Fix parallelization of renameRecursively in PathUtils.
* [YARN] [PR 962] Cleanup the helix job when closing the GobblinHelixJobLauncher

## IMPROVEMENTS

* [Distcp] [PR 647] Add option to set group for distcp-ng
* [Build] [PR 650] Javadoc task should pick up system proxy settings
* [Distcp] [PR 669] Parallelized copy listing generation in distcp.
* [Data Management] [PR 671] Added ConfigurableCleanableDatasetFinder. Renamed some CleanableDatasets for clarification
* [Admin Dashboard] [PR 687] Enable AdminUI when running gobblin under yarn
* [Job Exec History] [PR 688] Added a log line when starting to write job execution history
* [Build] [PR 694] Adding throttled upload of sonatype packages
* [Metrics] [PR 698] Log which custom metric reporter class is wired up
* [Documentation] [PR 704] Remove @link tags from @see javadoc tags
* [Job History Store] [PR 705] Improve database history store performance
* [YARN] [PR 708] Fixed the file mode of the gobblin-yarn.sh script to match the other scripts.
* [Core] [PR 713] Don't send an email on shutdown when email notifications are disabled.
* [Admin Dashboard] [PR 717] More flexible Admin configuration
* [Core] [PR 727] Modified to add a configuration to skip previous run during FileBasedExtraction for full load
* [Core] [PR 733] Add ability to configure the encryption_key_loc filesystem
* [Build] [PR 737] Better travis scripts which support test error reporting
* [Core] [PR 741] Fix #740 for FsStateStore.createAlias and removing usage of FileUtil.copy
* [Core] [PR 759] Allow downloading other filetypes in FileBasedExtractor
* [Data Management] [PR 760] Per dataset retention blacklist
* [Retention] [PR 764] Ensure that jobs cleanup correctly
* [Core] [PR 766] Create GZIPFileDownloader.java
* [YARN] [PR 768] Switch LogCopier from ScheduledExecutorService to HashedWheelTimer
* [Core] [PR 772] Upgrading and re-enabling Findbugs
* [Kafka] [PR 777] Adding Parallelization to WorkUnit Creation in KafkaSource
* [Documentation] [PR 788] Initial commit for mkdocs and readthedocs integration
* [Kafka] [PR 789] Parallize late data copy
* [Config Management] [PR 794] Read current version of config store from metadata file
* [Build] [PR 799] Adding JaCoCo and Coveralls support for code coverage analysis
* [Core] [PR 808] Adding ApplicationLauncher to manage app services, including GobblinMetrics lifecyle
* [Data Management] [PR 812] Make generic version, version finder, version selection policy
* [Hive Registration] [PR 815] Improve Hive registration performance
* [Core] [PR 829] Adds support to `HadoopUtils` for overwriting files
* [Build] [PR 832] excluding hive-exec from gobblin-compaction
* [YARN] [PR 834] Enable the maximum log file size for Gobblin Yarn LogCopier to be configured
* [Compaction] [PR 847] Change default value of compaction.job.avro.single.input.schema to true
* [Distcp] [PR 849] Distcp partition filter and kerberos authentication
* [Kafka] [PR 856] Clean up KafkaSource
* [Core] [PR 872] Change BoundedBlockingRecordQueue to be backed by ArrayBlockingQueue
* [Distcp] [PR 873] Implement simulate mode in distcp.
* [Distcp] [PR 877] Stream datasets to distcp.
* [Hive Distcp] [PR 878] Distcp on Hive supports predicates for fast partition skips, and supports copying full directories recursively
* [Hive Registration] [PR 885] Add locking to Hive registration
* [Distcp] [PR 886] Purge distcp persist directory at the beginning of publish phase.
* [Distcp] [PR 889] Avro schema modification in distcp is executed only for URLs in the origin schema and authority
* [Hive Distcp] [PR 890] Dynamic partition filtering for distcp Hive.
* [Hive Registration] [PR 894] Enable multiple db and table names in Hive registration
* [Core] [PR 897] Make it possible to disable publishing in job by specifying empty job data publisher
* [Core] [PR 902] Make it possible to specify empty job data publisher
* [Distcp] [PR 906] Maximum size for distcp CopyContext cache.
* [Retention] [PR 908] Add typesafe support to glob version finder for audit retention
* [Core] [PR 913] Job state stored in distributed cache in MR mode.
* [Data Management] [PR 926] Make NewestKSelectionPolicy use Java Generics instead of FileSystemDatasetVersion
* [Core] [PR 932] Separate jobstate from taskstate and datasetstate
* [Documentation] [PR 937] Add documentation for topic specific partitioning configuration
* [Hive Distcp] [PR 940] Distcp hive registration metadata
* [Hive Distcp] [PR 941] Delete empty parent directories on Hive de-registration. Optimize deregistration
* [Distcp] [PR 944] Bin pack distcp-ng work units.
* [Data Management] [PR 947]  Make VersionSelectionPolicy to work with any DatasetVersion
* [Distcp] [PR 949] Parallelize renameRecursively for distcp.
* [Hive Distcp] [PR 950] Add delete methods when deregistering Hive partitions in distcp.
* [Data Management] [PR 951] Moving NonNewestKSelectionPolicy logic to NewestKSelectionPolicy
* [Hive Distcp] [PR 953] Added instrumentation to Hive copy.
* [Config Management] [PR 956] Make the default store for SimpleHDFSConfigStoreFactory configurable
* [Hive Distcp] [PR 959] Remove checksum from HiveDistcp copy listing.
* [Hive Distcp] [PR 960] Accelerate path diff in HiveCopyEntityHelper by reusing FileStatus.
* [Distcp] [PR 966] Set max work units per multiworkunit for distcp.
* [Core] [PR 970] Fixing rest of findbugs warnings, and setting findbugs to fail the build on new warnings
* [Distcp] [PR 971] Distcp ng handle directory structure copy
* [Core] [PR 974] Deprecating and removing support for Hadoop versions other than 2.x.x
* [Hive Distcp] [PR 975] Added whitelist and blacklist capabilities to HiveDatasetFinder.

## EXTERNAL CONTRIBUTIONS

We would like to thank all our external contributors for helping improve Gobblin.

* kadaan, Joel Baranick:
    - Various fixes to the ParallelRunner (PR 661, 676)
    - Lifecycle events for Gobblin Jobs (PR 674)
    - Various fixes and enhancgements for the Admin Dashboard (PR 681, 687, 717)
    - Various fixes to the build (PR 704, 755, 775, 842)
    - Improve Job Execution History Store performance, and use Flyway to track migration scripts (PR 705)
    - Various fixes to Gobblin-on-YARN (PR 713, 726, 735, 768, 834, 962)
    - Enhancement to the Password Manager to allow it to specify a the FileSystem to use (PR 733)
    - Enhancement to the Travis build so test failures print out the full stack trace of any failed tests (PR 737)
    - Various fixes to Gobblin-Metrics (PR 775)
    - Adding a Zookeeper based job-lock (PR 828)
    - Performance optimization for BoundedBlockingRecordQueues (PR 872)
* lbendig, Lorand Bendig:
    - Fix broken Gobblin version resolution (PR 664)
    - Gobblin-compaction tarball doesn't contain gobblin-compaction.jar (PR 655)
    - Null Configuration is passed to MRJobLauncher (PR 859)
    - Adding Metrics Reporters for InfluxDB and Graphite (PR 863)
    - Hive compactor: Fix ClassNotFoundException in ShutdownHookManager (PR 943)
    - Unified way to execute Hive and MR-based compaction jobs (PR 952)
* jinhyukchang, Jinhyuk Chang:
    - Adding a JDBC Writer for Gobblin (PR 893)
* rakanalh, Rakan Alhneiti
    - Add documentation for topic specific partitioning configuration (PR 937)
* muratoda
    - Kafka simple json source (PR 835, 711)
    - Add missing jars to gobblin mapreduce runner, sort (PR 700, 690)
* anandrishabh, Rishabh Anand
    - Create GZIPFileDownloader (PR 766)
* pldash, Plaban Dash
    - Modified to add a configuration to skip previous run during FileBasedExtraction for full load (PR 727)
    - Modified the SFTP extractor to also use password for connecting to the servers (PR 686)
* jeanrichard, Etienne Richard
    - Fix a ConcurrentModificationException in MRCompactor (PR 680)

GOBBLIN 0.6.2
=============

## NEW FEATURES
* [Admin Dashboard] Added a web based GUI for exploring running and finished jobs in a running Gobblin daemon (thanks Eric Ogren).
* [Admin Dashboard] Added a CLI for finding jobs in the job history store and seeing their run details (thanks Eric Ogren).
* [Configuration Management] WIP: Configuration management library. Will enable Gobblin to be dataset aware, ie. to dynamically load and apply different configurations to each dataset in a single Gobblin job.
** APIs: APIs for configuration stores and configuration client.
** Configuration Library: loads low level configurations from a configuration store, resolves configuration dependencies / imports, and performs value interpolation.
* [Distcp] Allow using *.ready files as markers for files that should be copied, and deletion of *.ready files once the file has been copied.
* [Distcp] Added file filters to recursive copyable dataset for distcp. Allows to only copy files satisfying a filter under a base directory.
* [Distcp] Copied files that fail to be published are persisted for future runs. Future runs can recover the already copied file instead of re-doing the byte transfer.
* [JDBC] Can use password encryption for JDBC sources.
* [YARN] Added email notifications on YARN application shutdown.
* [YARN] Added event notifications on YARN container status changes.
* [Metrics] Added metric filters based on name and type of the metrics.
* [Dataset Management] POC embedded sql for config-driven retention management.
* [Exactly Once] POC for Gobblin managed exactly once semantics on publisher.

## BUG FIXES
* **Core** File based source includes previously failed WorkUnits event if there are no new files in the source (thanks Joel Baranick).
* **Core** Ensure that output file list does not contain duplicates due to task retries (thanks Joel Baranick).
* **Core** Fix NPE in CliOptions.
* **Core/YARN** Limit Props -> Typesafe Config conversion to a few keys to prevent overwriting of certain properties.
* **Utility** Fixed writer mkdirs for S3.
* **Metrics** Made Scheduled Reporter threads into daemon threads to prevent hanging application.
* **Metrics** Fixed enqueuing of events on event reporters that was causing job failure if event frequency was too high.
* **Build** Fix POM dependencies on gobblin-rest-api.
* **Build** Added conjars and cloudera repository to all projects (fixes builds for certain users).
* **Build** Fix the distribution tarball creation (thanks Joel Baranick).
* **Build** Added option to exclude Hadoop and Hive jars from distribution tarball.
* **Build** Removed log4j.properties from runtime resources.
* **Compaction** Fixed main class in compaction manifest file (thanks Lorand Bendig).
* **JDBC** Correctly close JDBC connections.

## IMPROVEMENTS
* [Build] Add support for publishing libraries to maven local (thanks Joel Baranick).
* [Build] In preparation to Gradle 2 migration, added ext. prefix to custom gradle properties.
* [Build] Can generate project dependencies graph in dot format.
* [Metrics] Migrated Kafka reporter and Output stream reporter to Root Metrics Reporter managed reporting.
* [Metrics] The last metric emission in the application has a "final" tag for easier Hive identification.
* [Metrics] Metrics for Gobblin on YARN include cluster tags.
* [Hive] Upgraded Hive to version 1.0.1.
* [Distcp] Add file size to distcp success notifications.
* [Distcp] Each work unit in distcp contains exactly one Copyable File.
* [Distcp] Copy source can set upstream timestamps for SLA events emitted on publish time.
* [Scheduling] Added Gobblin Oozie config files.
* [Documentation] Improved javadocs.


GOBBLIN 0.6.1
-------------

## BUG FIXES

- **Build/release** Adding build instrumentation for generation of rest-api-* artifacts
- **Build/release** Various fixes to decrease reliance of unit tests on timing.

## OTHER IMPROVEMENTS

- **Core** Add stability annotations for APIs. We plan on starting to annotate interfaces/classes to specify how likely the API is to change.
- **Runtime** Made it an option for the job scheduler to wait for running jobs to complete
- **Runtime** Fixing dangling MetricContext creation in ForkOperator

## EXTERNAL CONTRIBUTIONS

- kadaan, joel.baranick:
  + Added a fix for a hadoop issue (https://issues.apache.org/jira/browse/HADOOP-12169) which affects the s3a filesystem and results in duplicate files appearing in the results of ListStatus. In the process, extracted a base class for all FsHelper classes based on the hadoop filesystem.


GOBBLIN 0.6.0
--------------

NEW FEATURES

* [Compaction] Added M/R compaction/de-duping for hourly data
* [Compaction] Added late data handling for hourly and daily M/R compaction: https://github.com/linkedin/gobblin/wiki/Compaction#handling-late-records; added support for triggering M/R compaction if late data exceeds a threshold
* [I/O] Added support for using Hive SerDe's through HiveWritableHdfsDataWriter
* [I/O] Added the concept of data partitioning to writers: https://github.com/linkedin/gobblin/wiki/Partitioned-Writers
* [Runtime] Added CliLocalJobLauncher for launching single jobs from the command line.
* [Converters] Added AvroSchemaFieldRemover that can remove specific fields from a (possibly recursive) Avro schema.
* [DQ] Added new row-level policies RecordTimestampLowerBoundPolicy and AvroRecordTimestampLowerBoundPolicy for checking if a record timestamp is too far in the past.
* [Kafka] Added schema registry API to KafkaAvroExtractor which enables supports for various Kafka schema registry implementations (e.g. Confluent's schema registry). 
* [Build/Release] Added build instrumentation to publish artifacts to Maven Central

BUG FIXES

* [Retention management] Trash handles deletes of files already existing in trash correctly.
* [Kafka] Fixed an issue that may cause Kafka adapter to miss data if the fork fails.

OTHER IMPROVEMENTS

* [Runtime] Added metrics for job executions
* [Metrics] Added a root metric context to keep track of GC of metrics and metric contexts and make sure those are properly reported
* [Compaction] Improve topic isolation in MRCompactor
* [Build/release] Java version compatibility raised to Java 7.
* [Runtime] Deprecated COMMIT_ON_PARTIAL_SUCCESS and added a new policy for successful extracts
* [Retention management] Async trash implementation for parallel deletions.
* [Metrics] Added tracking events emission when data gets published
* [Retention management] Added support for parallel execution to the dataset cleaner
* [Runtime] Update job execution info in the execution history store upon every task completion

INCUBATION

Note: these are new features which are under active development and may be subject to significant changes.

* [gobblin-ce] Adding support for Gobblin Continuous Execution on Yarn
* [distcp-ng] Started work on bulk transfer (file copies) using Gobblin
* [distcp-ng] Added a light-weight Hadoop FileSystem implementation for file transfer from SFTP
* [gobblin-config] Added API for dataset driven

EXTERNAL CONTRIBUTIONS

We would like to thank all our external contributors for helping improve Gobblin.

* kadaan, joel.baranick: 
    - Separate publisher filesystem from writer filesystem
    - Support for generating Idea projects with the correct language level (Java 7)
    - Fixed yarn conf path in gobblin-yarn.sh
* mwol(Maurice Wolter) 
    - Implemented new class AvroCombineFileSplit which stores the avro schema for each split, determined by the corresponding input file.
* cheleb(NOUGUIER Olivier)
    - Add support for maven install
* dvenkateshappa 
    - bugifx to RestApiExtractor.java
    - Added an excluding column list , which can be used for salesforce configuration with huge list of columns.
* klyr (Julien Barbot) 
    - bugfix to gobblin-mapreduce.sh
* gheo21 
    - Bumped kafka dependency to 2.11
* ahollenbach (Andrew Hollenbach)
   -  configuration improvements for standalone mode
* lbendig (Lorand Bendig)
   - fixed a bug in DatasetState creation
