GOBBLIN 0.11.0
-------------

###Created Date:7/19/2017

## HIGHLIGHTS 

* Introduced Java 8.
* Introduce ReactiveX to enable record level stream processing.
* Introduced Calcite to help sql building and processing.
* New Converters: HttpJoinConverter, FlattenNestedKeyConverter, AvroStringFieldEncryptorConverter, AvroToBytesConverter, BytesToAvroConverter
* New Http constructs: ApacheHttpClient, ApacheHttpAsyncClient, R2Client.
* New sources: RegexPartitionedAvroFileSource.

## NEW FEATURES 

* [Core] [PR 1909] Introduced ReactiveX to enable record level stream processing.
* [Core] [PR 2000] Added control messages to Gobblin stream. 
* [Core] [PR 1998] Added hex and base64 codecs support for JSON CredentialStore. 
* [Http] [PR 1881] [PR 1965] Added new http client (`ApacheHttpClient`, `ApacheHttpAsyncClient`, `R2Client`) .
* [Http] [PR 1881] Added default http/r2 request builder and handlers.
* [Converter] [PR 1943] Added `AvroHttpJoinConverter` to allow remote lookup by providing resource key from avro record.
* [Converter] [PR1837] [PR1978] Add `FlattenNestedKeyConverter` to extract nested attributes and copy it to the top-level.
* [Converter] [PR 1844] Added `AvroStringFieldEncryptorConverter` to encrypt a string field in place.
* [Converter] [PR 1916] Added `AvroToBytesConverter` and `BytesToAvroConverter` to convert an avro record to/from a byte array with underlying encoder.
* [Metadata] [PR 1871] Added metadata aware file system instrumentation.

## IMPROVEMENTS

* [Core] [PR 1958] Reused existing task execution thread pool for retrying in local execution mode.
* [Core] [PR 1987] Added configurable `EventMetadataGenerator` to generate additional metadata to emit in the timing events.
* [Core] [PR 1936] Added `FrontLoadedSampler` to sample records in error file during the quality check. 
* [Source] [PR 1959] Improved kafka offset fetch time via using a thread local kakfa consumer client for each thread in the `KafkaSource`.
* [Source] [PR 1836] Refactored `DatePartitionedAvroFileSource` to separate out the mechanism of retrieving files and add `RegexPartitionedAvroFileSource`.
* [Source] [PR 1948] Made dataset state store configurable in Kafka source.
* [Source] [PR 1986] Added partition and table information on `HiveWorkUnit`.
* [Extractor] [PR 1981] Introduced Calcite to help detect a join condition and fail corresponding task when extracting metadata using `JdbcExtractor`.
* [Extractor] [PR 1964] Allowed query which has SQL keywords as column names to be executed in `JdbcExtractor`.
* [Extractor] [PR 1962]Allowed user to add optional watermark predicates in `JdbcExtractor`.
* [Extractor] [PR 1886] [PR 1930] Introduced `DecodeableKafkaRecord` to wrap kafka records consumed through new kafka-client consumer APIs (0.9 and above).
* [Converter] [PR 1999] Use expected output avro schema to decode a byte array.
* [Compaction] [PR 1989] Added prioritization capability to Gobblin-built-in compaction flow.
* [Compaction] [PR 1899] Improved compaction verification by using `WorkUnitStream`.
* [Hive-Registration] [PR 1983] Reduce lock contention from multiple database and table examination in hive registration.
* [Encryption] [PR 1934] Allowed converter level encryption config so that multiple converters in a chain can have their own encryption config without impacting others.
* [CredentialStore][Eric Ogren] Added a test credential store and associated provider that can be used for integration testing.
* [CredentialStore] [Eric Ogren] Refactored `CredentialStore` factory into its own top-level class.
* [Distcp] [PR 1888] Added more metadata in the SLA events when Distcp is completed.
* [Distcp] [PR 1975] Added blacklist/whitelist filtering to `CopySource` as a secondary filtering after `DatasetFinder` filtering is applied.
* [Distcp] [PR 1997] Make Watermark checking configurable in Distcp flow.
* [Source] [PR 1941] Added a limit to the max number of files to pull on `FileBasedSource`. 
* [Source] [PR 1957] Added additional timers to kafka source and hive publisher.
* [Google] [PR 1889] Added retry logic for Google web master source. Keep the states in iterators and reset the extractor to restart from the very beginning if necessary.
* [ConfigStore] [PR 1893] [PR 1913] Integrated config store with KafkaSource and hive registration.
* [ConfigStore] [PR 1908] Integrated config store with `ValidationJob`.
* [ConfigStore] [PR 1927] Integrated config store with Distcp and retention jobs by introducing `ConfigBasedCleanabledDatasetFinder` and `ConfigBasedCopyableDatasetFinder`.
* [ConfigStore] [PR 1972] Made config client thread safe.
* [ConfigStore] [PR 1866] [PR 1887] Allowed ConfigClient to resolve dynamic tags.
* [ConfigStore] [PR 1956] [PR 1952] Created static config client for hive-registration to avoid repeated initialization.
* [Throttling] [PR 1862] Improved throttling and config library.
* [Throttling] [PR 1910] Added throttling control to `AsyncHttpWriter`.
* [Throttling] [PR 1910] Added throttling control to `R2Client`.
* [Avro2Orc] [PR 1827] Preserved  partition parameters during avro2orc conversion.
* [Avro2Orc] [PR 1855] Added hive settings to validation job for avro2orc.
* [Compliance] [PR 1918] Added lazy initialization of `HiveMetaStoreClientPool` for `HivePartitionFinder`

## BUGS FIXES

* [Core] [PR 1907] Fixed `FileSystemKey` which used invalid characters for configuration key.
* [Core] [PR 1935] Refactor cancel method in `AzkabanJobLauncher` to avoid state file loss in a shutdown hook.
* [Http/R2] [PR 1924] Fixed the shutdown hanging issue for `R2Client`.
* [Writer] [PR 1861] Avoided two jobs sharing same staging or output directory delete each other by adding a new jobId sub-directory.
* [Writer] [PR 1906] Prevented AsyncHttpWriter closing before buffer is empty.
* [Writer] [PR 1875] [PR 1880] Fixed a bug in copy writer.
* [Extractor] [PR 1925] Provided an option to promote an MySQL unsigned int to a bigint to handle large unsigned ints.
* [Distcp] [PR 1955] Updated avro.schema.url properly when Distcp copies data from partition level.
* [Distcp] [PR 1915] Added a missing line that resulted in files from the old location being deleted when a hive table is replaced.
* [Cluster] [PR 1864] Fixed NPE issue when Yarn container is killed.
* [Cluster] [PR 1838] Started to use `SpecExecutorInstanceConsumer` in the `StreamingJobConfigurationManager` if it is a service.
* [Cluster] [PR 1974] Fixed issue with job id generation in gobblin cluster when using the internal scheduler by cloning the properties that get mutated during job execution. This prevents the state in the scheduler from getting affected by the job execution. 
* [Compliance] [PR 1918] Initialized HiveMetaStoreClientPool lazily to make sure metastore connection won't be timed out in HivePartitionFinder.
* [Compliance] [PR 1960] Fix number type issue when submitting bytes written event.
* [Compliance] [PR 1860] Preserved the directory structure by suffixing path with timestamp.
* [Compliance] [PR 1872] Fixed GC issues  for gobblin-compliance.
* [Compliance] [PR 1884] Dropped staging table from the previous execution ComplianceRetentionJob.

## EXTERNAL CONTRIBUTIONS
We would like to thank all our external contributors for helping improve Gobblin.

* kadaan 
  - Change AWS security to credentials providers.(PR 1980)


GOBBLIN 0.10.0
-------------
###Created Date:05/01/2017

## HIGHLIGHTS 

* Gobblin-as-a-Service: Global orchestrator with REST API for submitting logical flow specifications. Logical flow specifications compile down to physical pipeline specs (Gobblin Jobs) that can run on one or more heterogeneous Gobblin deployments.
* Gobblin Throttling: Library and service to enforce global usage policies by various processes or applications. For example, Gobblin throttling allows limiting the aggregate QPS to a single Database of all MR applications.
* Gobblin Stream Mode: This release introduces support for running streaming ingestion pipelines that include all the standard Gobblin pipeline capabilities (converters, forks etc). Streaming sources (Kafka) and sinks (Kafka, Couchbase, Eventhub) are included.
* Gobblin compliance: Including functionality for purging datasets, Gobblin Compliance module allows for data purging to meet regulatory compliance requirements. (https://gobblin.readthedocs.io/en/latest/user-guide/Gobblin-Compliance/)
* New Writers: Couchbase (PR 1433), EventHub (PR 1537). 
* New Sources: Azure Data Lake (PR 1764)

## NEW FEATURES 

* [Source] [PR 1764]Added Azure Data Lake source.
* [Source] [PR 1762]Added Salesforce daily-based dynamic partitioning.
* [Source] [PR 1742]Enabled `QueryBasedExtractor` to retry from first iterator. 
* [Core] [PR 1772]Supported shorter dataset state store name to handle overlong dataURN. 
* [Core] [PR 1678]Introduced `GlobalMetadata` into data pipelines and updated corresponding Gobblin components.
* [Core] [PR 1709]Introduced custom Task interface and execution to Gobblin.
* [Core] [PR 1727]Introduced `MRTask` inherited from Task interface that runs an MR job.
* [Core] [PR 1457]Added token-based extractor. 
* [Core] [PR 1463]MySQL Database as state store. 
* [Core] [PR 1524]Zookeeper and HelixPropertyStore as state store. 
* [Core] [PR 1662]Added compression and encryption support to `SimpleDataWriter`. 
* [Cluster] [PR 1524]Scripts to launch Gobblin in standalone cluster mode. 
* [Encryption] [PR 1616]Added encryption support by introducing a StreamCodec objects that encode/decode bytestreams flowing through it.
* [Encryption] [PR 1690]Added `gobblin-crypto` module containing encryption-related interfaces for gobblin. 
* [Extractor] [PR 1518]Implemented Streaming extractor for stream source.
* [Distcp] [PR 1735]Enabled updating existing hive table for distcp, instead of deleting originally existed one.
* [Hive-Registration] [PR 1722]Added runtime table properties into Hive Registration.
* [Writer] [PR 1537]Implemented Eventhub synchronized data writer.
* [Writer] [PR 1819]Implemented asynchronized HTTP Writer. 


## IMPROVEMENTS

* [Build] [PR 1817]Light distribution package building.
* [Cluster] [PR 1599]Supported multiple Helix controllers for Gobblin standalone cluster manager for high availability.
* [Cluster] [PR 1613]Support Helix 0.6.7.
* [Cluster] [PR 1592]Added `ScheduledJobConfigurationManager` in gobblin-cluster to periodically consume from Kafka for new JobSpecs
* [Compaction] [PR 1760]Implemented general Gobblin-built-in compaction using customized gobblin task.
* [Converters] [PR 1780]Support `.gzip` extension for UnGzipConverter.
* [Converters] [PR 1701]Set streamcodec in encrypting converter explicitly.
* [Converter] [PR 1612]Implemented converter that samples records based on configured sampling ratio. 
* [Core] [PR 1739]Reduced memory usage when loading by adding `commonProps` to  `FsStateStore`.
* [Core] [PR 1741]Removed fork branch index, task ID and job ID from task metrics. 
* [Core] [PR 1649]Enabled events emission when `LimiterExtractorDecorator` failed to retrieve the record.
* [Core] [PR 1702]Implemented writer-side partitioner based on incoming set of records' `WorkUnitState`.   
* [Core] [PR 1518] [PR 1596]Enhanced Watermark components for streaming.  
* [Core] [PR 1534]Implemented converter to convert `.pull` files into `.conf` file using the corresponding template. 
* [Core] [PR 1505]Enabled creation and access `WorkUnits` and `TaskStates` through `StateStore` interface. 
* [Copy-Replication] [PR 1728]Added logic of `AbortOnSingleDatasetFailure` in distcp.
* [Metric] [PR 1782]Added Pinot-based completeness check verifier.
* [Publisher] [PR 1702]Enable collecting partition information and publish metadata files in each partition directory by default setting.
* [Source] [PR 1666] [PR 1733]Implemented source-side partitioner for `QueryBasedSource`, allowing user-specified partitions.
* [Runtime] [PR 1552]Optimized tasks execution in single branch by removing unnecessary data structure used in fork.
* [Runtime] [PR 1791]Support state persistence for partial commit.
* [Writer] [PR 1265]Replace DatePartitionedDailyAvroSource with configurable partitioning.


## BUGS FIXES

* [Core] [PR 1724]Fixed hanging embedded Gobblin when initialization fails.
* [Core] [PR 1736]Fixes of contention on shared object `SimpleDateFormat` among all pull jobs start simultaneously in multi-threads context.
* [Core] [PR 1665]Fixed threadpool leak in HttpWriter. 
* [Hive-Registration] [PR 1635]Fix NullPointerException when Deserializer is not properly initialized.
* [Metastore] [PR 986]Fixed `gobblin.metastore.DatabaseJobHistoryStore`'s vulnerability regarding to SQL injection.
* [Runtime] [PR 1801]Fixed `JobScheduler` failed when "jobconf.fullyQualifiedPath" is not set.
* [Runtime] [PR 1624]Fix speculative run for `SimpleDataWriter`. 
* [Source] [PR 1756]Enabled `UncheckedExecutionException` catching in HiveSource.


## EXTERNAL CONTRIBUTIONS

* enjoyear 
  - Fixed multi-threading bug in TimestampWatermark.(PR 1736)
  - Maintained and fixed google-related source issues. (PR 1771, PR 1765, PR 1742, PR 1628)
* kadaan 
  - Fixed `JobScheduler` failed when "jobconf.fullyQualifiedPath" is not set. (PR 1801)
  - Optimized tasks execution in single branch by removing unnecessary data structure used in fork. (PR 1552) 
* erwa 
  - Revert Hive version to 1.0.1, add AvroSerDe handling in HiveMetaStoreUtils.getDeserializer. (PR 1643)
  - Fix NullPointerException when Deserializer is not properly initialized. (PR 1635)
* howu
  - Refactor RestApiConnector and RestApiExtractor. (PR 1708)  
  - Update constructor of FlowConfigClient and FlowStatusClient. (PR 1734)
* jinhyukchang 
  - Added support for Azure Data Lake(ADL) as a source (PR 1764)
  - Added abortOnSingleDatasetFailure to CopyConfiguration. (PR 1728)
* wosiu
  - Fix speculative run for SimpleDataWriter. (PR 1624)


GOBBLIN 0.9.0
-------------

### Created Date: 12/13/2016

## Highlights

* Refactored project structure in Gobblin. If not importing dependencies transitively, you may need to import "gobblin-core-base".
* New sources: Google analytics / drive (PR 1301), Google webmaster (PR 1422), Oracle (PR 1304).
* New writers: Teradata (http://gobblin.readthedocs.io/en/latest/user-guide/Gobblin-JDBC-Writer/), object store (PR 1348).
* Retention job is more generic, allowing arbitrary actions on dataset versions (https://gobblin.readthedocs.io/en/latest/data-management/Gobblin-Retention).
* Docker integration (https://gobblin.readthedocs.io/en/latest/user-guide/Docker-Integration).
* Gobblin jobs can be run embedded into other applications (http://gobblin.readthedocs.io/en/latest/user-guide/Gobblin-as-a-Library/).
* Gobblin jobs can be run from CLI with full support for templates, plugins, etc. (http://gobblin.readthedocs.io/en/latest/user-guide/Gobblin-CLI/)
* Topology based data replication: users can specify a topology for their data copy in config store, Gobblin Distcp will handle replication. (PR 1278, PR 1306, PR 1328, PR 1405)
* Prioritization of work units when there is more work than can be run in a single job (PR 1283).
* Enabled speculative excecution in MR mode (PR 1347).

## NEW FEATURES

* [Writers] [PR 1181] Teradata Writer implemented.
* [Converters] [PR 1246] Added some new core converters: schema injector, avro to json string, json to string, string to bytes.
* [Testing] [PR 1247] Added end-to-end testing framework for Gobblin job execution.
* [Job Execution] [PR 1248] [PR 1249] Added Quartz scheduler for new Gobblin launch model.
* [Core] [PR 1278] Added dataset finder using Gobblin config library.
* [Retention] [PR 1279] Retention job can now apply other arbitrary actions to datasets (for example change ACL).
* [Core] [PR 1280] Added a converter for parsing GoldenGate messages.
* [Core] [PR 1283] Added utilities to prioritize work when there are more work units available than can be run in a single job.
* [Sources] [PR 1301] Added Google analytics and google drive sources.
* [Sources] [PR 1304] Added Oracle extractor.
* [Core] [PR 1305] Added a schema based partitioner.
* [Deploy] [PR 1308] Docker integration.
* [Core] [PR 1313] [PR 1331] Gobblin in embedded mode.
* [Core] [PR 1333] Support for plugins in Gobblin instances.
* [Core] [PR 1337] Kerberos login plugin implemented.
* [Core] [PR 1340] New Gobblin cli capable of using templates, plugins, etc.
* [Core] [PR 1347] Support speculative execution in MR mode.
* [Writers] [PR 1348] Object store writer.
* [Compaction] [PR 1354] Delta support in Gobblin compaction.
* [Core] [PR 1440] Added email notification plugin.
* [Sources] [PR 1422] Google webmaster source

## IMPROVEMENTS

* [Templating] [PR 1228] Templates read *.conf files as `Config` objects, allowing for better interpolation of configurations.
* [Core] [PR 1246] Wikipedia source changed to actually use state store.
* [Core] [PR 1246] Robustness improvements on `JobScheduler`, previously it silently failed on certain exceptions.
* [Core] [PR 1339] Gobblin can gracefully skip work units.
* [Build] [PR 1417] Refactoring of Kafka dependent classes into separate modules for improved dependency management.
* [Build] [PR 1424] Refactoring of Gobblin core module for improved dependency management.
* Improved documentation for various features.
* Fixed many intermittently failing unit tests (special thanks to htran1).
* Various bug fixes.

## EXTERNAL CONTRIBUTIONS
We would like to thank all our external contributors for helping improve Gobblin.

* lbendig
  - Teradata writer (PR 1181)
  - Oracle extractor (PR 1304)

* jsavolainen
  - Bug fixes in job configuration loading (PR 1259)

* klyr
  - Update lib versions for AWS (PR 1368)

* enjoyear
  - Google webmaster source

GOBBLIN 0.8.0
-------------

#### Created Date: 08/22/2016

## Highlights

* Gobblin can now convert avro to orc files through Hive. Documentation: http://gobblin.readthedocs.io/en/latest/adaptors/Hive-Avro-To-ORC-Converter/.
* Gobblin can now write data to Kafka using a new `KafkaWriter`. Documentation: http://gobblin.readthedocs.io/en/latest/sinks/Kafka/.
* Gobblin distcp can now replicate Hive tables between different Hive Metastores. Documentation: http://gobblin.readthedocs.io/en/latest/case-studies/Hive-Distcp/.
* Gobblin can now support hive based retentions. Documentation: http://gobblin.readthedocs.io/en/latest/data-management/Gobblin-Retention/.
* Gobblin can now support job templates, which reduces the efforts of writing a Gobblin job.
    Documentation: http://gobblin.readthedocs.io/en/latest/user-guide/Gobblin-template/.

## NEW FEATURES

* [Kafka] [PR 1016] Integration with Confluent Schema Registry, Confluent Deserializers, and Kafka Deserializers
* [Avro to ORC] [PR 1031] Adding Avro To ORC conversion logic and related framework modifications
* [General FileSystem Support] [PR 1066] Config file monitor for general file system
* [Avro to ORC] [PR 1068] Nested Avro to Nested ORC conversion support
* [General FileSystem Support] [PR 1073] extension of loading config file from general file system
* [AWS] [PR 1088] Gobblin on AWS
* [Kafka Writer] [PR 1089] Kafka writer
* [JDBC Extractor] [PR 1090] Teradata JDBC Extractor and Source
* [Avro to ORC] [PR 1093] Support for schema evolution, staging, selective column projection and compatibility check for Avro to ORC
* [Hive Retention] [PR 1106] Hive Based Retention
* [Job Templates] [PR 1145] Initial commit for job configuration template
* [Http Writer] [PR 1186] HttpWriter including SalesForceRestWriter, ThrottleWriter, etc
* [Avro to ORC] [PR 1188] Avro to orc data validation
* [Job Templates] [PR 1197] Kafka-template
* [Job Launcher] [PR 1203] New std driver2
* [Core] [PR 1216] Adding a simple console writer to gobblin

## BUG FIXES

* [YARN] [PR 982] Using new zk port numbers for unit tests
* [Kafka] [PR 996] Fix offset related bug in KafkaSource
* [Core] [PR 999] distcp-ng throws UnsupportedOperationException
* [Build] [PR 1001] Setting heaps size for gobblin-runtime tests due to OOM in some cases
* [Core] [PR 1002] Set explicit 755 permissions to state store
* [Core] [PR 1005] Fixing SOURCE_QUERYBASED_LOW_WATERMARK_BACKUP_SECS no default value
* [Config Management] [PR 1043] Fix includes order
* [JDBC Writer] [PR 1050] JDBCWriter. Bug fix on SQL statements. Bug fix on data type mapping.
* [Data Management] [PR 1051] Fix default blacklist key
* [Salesforce] [PR 1069] Adding security token to Salesforce bulk API login
* [Runtime] [PR 1078] Fixing possible NPE in SourceDecorator
* [Documentation] [PR 1081] Fixing search for Gobblin ReadTheDocs
* [Documentation] [PR 1107] Minor text formatting fix for README.md
* [Salesforce] [PR 1118] gobblin salesforce update to new proxy
* [Config Management] [PR 1135] Revert changes to ConfigUtils
* [Utility] [PR 1147] Capture exceptions correctly in HadoopUtilsTest.testSafeRenameRecursively
* [Salesforce] [PR 1152] Updated gobblin salesforce to resolve entity.source and extract.table.name
* [Build] [PR 1153] Make sure maven central repo is first; bug fixes
* [Utility] [PR 1154] Fix for failing createProxiedFileSystemUsingToken
* [Avro to ORC] [PR 1155] Changed Hive validation to make it compatible with old Hive version with auth turned on, and Hive query generation compile with new Hive version
* [Build] [PR 1156] Upgrade wix-embedded-mysql
* [Runtime] [PR 1157] Move test MR jobs dir to /tmp to avoid issues with DistributedCache
* [Distcp] [PR 1160] FIxed a race condition on CopyDataPublisher.
* [Metrics] [PR 1170] Not fail the task if metricsReport failed to be stopped
* [Metrics] [PR 1176] Added a backwards compatible constructor to SchemRegistryVersionWriter
* [Retention] [PR 1182] Throw exception when retention dataset finder fails to initialize
* [Retention] [PR 1202] Bug fix - Retention does not blacklist dataset
* [Runtime] [PR 1215] Fixed silent failures and hung application when a standalone service fails to initialize.
* [Example] [PR 1217] Fixing console writer example

## IMPROVEMENTS

* [YARN] [PR 978] Initial commit for gobblin-cluster; gobblin-yarn refactoring
* [Core] [PR 979] Initial commit for HTTP Writer APIs
* [Core] [PR 980] Add metadata after completion of job to a specific metadata directory
* [Hive Distcp] [PR 983] need to deregister existing table
* [Documentation] [PR 988] Adding documentation page for Gobblin Distcp
* [Documentation] [PR 989] Added retention docs
* [Documentation] [PR 991] Add Hive registration doc
* [Kafka] [PR 992] Making kafka metadata read more resillient to issues with the brokers
* [Documentation] [PR 993] open source wiki for config management
* [Data Management] [PR 998] Merge the two LongWatermarks
* [Hive Distcp] [PR 1003] Added the predicate check to skip full table diff if the existing table's registration time > source table's mod time
* [Distcp] [PR 1008] ETL-4470: Implementation of http filer puler using Distcp-ng
* [Documentation] [PR 1012] Document changes in PR#952
* [Documentation] [PR 1013] Update documents
* [Build] [PR 1023] Adding parallel test Travis VMs
* [Hive Registration] [PR 1027] Added configuration to Hive client for getting credentials.
* [Hive Registration] [PR 1034] Hive metastore initialization should support empty HCat uri ie default to platform defaults
* [Avro to ORC] [PR 1035] Use table schema and partition schema
* [Avro to ORC] [PR 1036] Hive metastore connection pool optimization, Fixes for: backward compatibility for Hive in AvroToOrc, schema parser deserialization from schema literal, database name in Hive DDL query generation, Hive metastore connection pool initialization NPE if Hcat uri is platform provided
* [Avro to ORC] [PR 1037] Add sla events for avro to orc conversion
* [Hive Registration] [PR 1038] Made Hive metastore connection auto returnable to connection pool after Hive dataset discovery
* [Avro to ORC] [PR 1044] Made HiveAvroToOrcConverter compatible with Hive v0.13 version
* [Hive Distcp] [PR 1045] Add bootstrap low watermark support for HiveSource in data management
* [Avro to ORC] [PR 1046] [Avro to ORC] Mark all workunits of a dataset as failed if one task fails
* [Hive Distcp] [PR 1053] Add lookback days for HiveSource
* [Hive Registration] [PR 1054] Converted Hive dereg / registration to post publish steps, fixed missing fileset.
* [Distcp] [PR 1055] Parallelize commit rebased
* [Hive Distcp] [PR 1056] Add lastDataPublishTime in hive table/partition properties
* [Runtime] [PR 1060] MR launcher does not write tasks to the jobstate file in HDFS.
* [Hive Distcp] [PR 1062] Enable AvroSchemaManager to read schema from Kafka schema registry
* [Hive Distcp] [PR 1067] Add a backfill hive source that does not check watermarks
* [Data Management] [PR 1071] Add ConvertibleHiveDataset and config store support to HiveDatasetFinder
* [Documentation] [PR 1082] Updating the README and other outdated docs to encourage use of Gobblin Releases
* [Avro to ORC] [PR 1087] Add support for nested and flattened orc conversion configuration
* [Kafka] [PR 1091] Confluent schema registry example for kafka writer
* [Json Converter] [PR 1092] Added JsonConverter to parse Json files to a format such that JsonIntermediateToAvro converter can parse
* [Avro to ORC] [PR 1095] Refactored to rename HiveAvroORCQueryUtils to HiveAvroORCQueryGenerator
* [Compaction] [PR 1096] Added simulate mode in Hive JDBC Connector to simulate query execution
* [Avro to ORC] [PR 1097] Added limit clause to Hive query generation to enable conversion validation of sample subset
* [Avro to ORC] [PR 1098] Added Azkaban job that can validate conversion result by comparing source and target Hive tables
* [Core] [PR 1102] Inter strings in deserialized States to reduce memory usage.
* [Documentation] [PR 1104] Added powered by section in wiki for companies using Gobblin
* [Documentation] [PR 1105] Added Gobblin meetup June 2016 presentations on Talks and Tech Blogs wiki
* [Documentation] [PR 1109] Updating the code contributions documentation
* [Documentation] [PR 1110] Added videos from June 2016 meetup to talks-and-tech-blogs wiki page
* [Documentation] [PR 1111] Made order of presentations chronological in talks-and-tech-blogs wiki page
* [Documentation] [PR 1112] Update Gobblin on AWS video presentation link with right start time in playback
* [Documentation] [PR 1113] Added Paypal to powered by wiki page
* [Documentation] [PR 1115] Adding Sandia National Labs to Powered-By page
* [Avro to ORC] [PR 1119] Changed concatenated queries string to list in Hive converter publisher
* [Avro to ORC] [PR 1120] Added Hive query generation to optionally support explicit database names
* [Avro to ORC] [PR 1122] Made changes to handle Hive-6129 (inverted exchange partition bug) and corresponding support for backward incompatible changes in Hive
* [Hive Distcp] [PR 1126] Make distcp publisher safer: renameRecursively fails appropriately, hive registration fails if location doesn't exist.
* [Avro to ORC] [PR 1127] Drop hourly partitions when daily data gets converted to ORC
* [Hive Registration] [PR 1128] Added events in hive-registration
* [Avro to ORC] [PR 1138] Change Hive Avro to ORC publish to use Gobblin constructs instead of Hive exchange partition query
* [Avro to ORC] [PR 1139] Added support to escape the Hive nested field names when derived from destination table as raw string
* [Data Management] [PR 1140] Moved WhitelistBlacklist from data-management to utility.
* [Avro to ORC] [PR 1141] Renamed partitionDir.prefixLocationHint to source.dataPathIdentifier to be more consistent with naming across Hive data conversion
* [Build] [PR 1142] Add gradle property withFindBugsXmlReport to enable XML FindBugs reports
* [Avro to ORC] [PR 1148] Support for distcp-ng registration time in isOlderThanLookback check and minor refactoring
* [Avro to ORC] [PR 1151] Changed Hive conversion validation job to use HIVE_DATASET_CONFIG_PREFIX consistent with HiveAvroToOrcSource
* [Avro to ORC] [PR 1163] Fail avro to orc valiation job on at least one failure
* [Hive Registration] [PR 1165] Add create time to newly registered Hive tables and partitions.
* [Hive Distcp] [PR 1167] Adding options in watermarkCopyableFileFilter and some refactoring
* [Metrics] [PR 1169] Gobblin metrics registers the base schemas instead of inferring them from events.
* [Avro to ORC] [PR 1171] Added more SLA event metadata to Avro to Orc conversion job
* [Avro to ORC] [PR 1172] Use camel case for event names
* [Avro to ORC] [PR 1173] Parallalize Avro to Orc validation job
* [Utility] [PR 1175] Schema files (schema.avsc) will be written with 774 permission.
* [Hive Distcp] [PR 1180] Add createtime when altering a table.
* [Job Templates] [PR 1183] change the key name of required.attributes
* [Job Templates] [PR 1184] Fixed name of ResourceBasedTemplate.
* [Job Templates] [PR 1185] Fix naming of template and template class file.
* [Avro to ORC] [PR 1189] cache data modTime to reduce too many HDFS calls
* [Hive Retention] [PR 1190] Add logs to hive retention. Support more DatasetFinder constructors
* [Data Management] [PR 1192] Add config store uri builder for hive datasets
* [Core] [PR 1204] Refactor methods between HadoopFsHelper and AvroFsHelper
* [Avro to ORC] [PR 1205] AvroToorc - Implemented a per partition watermark
* [Job Launcher] [PR 1206] Refactored SchedulerUtils into a new PullFileLoader that uses Config to load pull files.
* [Documentation] [PR 1207] template wiki doc added
* [Kafka] [PR 1210] Make topic suffix configurable for lookup in Confluent Schema Registry
* [Job Templates] [PR 1211] Restored template functionality removed accidentally. Add unit test for the functionality.
* [Kafka] [PR 1218] Making Kafka consumer configurable for Kafka extract
* [Runtime] [PR 1220] Refactored MR mode to use GobblinInputFormat.
* [Kafka Writer] [PR 1226] Making kafka writer more robust, adding tests
* [Job Templates] [PR 1228] Templates use config instead of properties.

## EXTERNAL CONTRIBUTIONS

We would like to thank all our external contributors for helping improve Gobblin.

* singhd10:
    -Add metadata after completion of job to a specific metadata directory (PR 980)
* shelocks:
    -Fixing SOURCE_QUERYBASED_LOW_WATERMARK_BACKUP_SECS no default value (PR 1005)
* lbendig,Lorand Bendig:
    -Document changes in PR#952 (PR 1012)
    -Make topic suffix configurable for lookup in Confluent Schema Registry (PR 1210)
* jinhyukchang, Jinhyuk Chang:
    -JDBCWriter. Bug fix on SQL statements. Bug fix on data type mapping. (PR 1050)
    -HttpWriter including SalesForceRestWriter, ThrottleWriter, etc (PR 1186)
* ypopov, Eugene Popov:
    -Teradata JDBC Extractor and Source (PR 1090)
* pldash
    -Added JsonConverter to parse Json files to a format such that JsonIntermediateToAvro converter can parse (PR 1092)

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
* [Build] [PR 665] Gobblin-compaction tarball doesn't contain gobblin-compaction.jar
* [Core] [PR 676] Ensure that parallel runner waits for the underlying tasks to finish
* [Core] [PR 677] Fix race condition in FsStateStore
* [Compaction] [PR 680] Fix a ConcurrentModificationException in MRCompactor
* [Admin Dashboard] [PR 681] Fixed off by one issue when listing the job executions in Admin UI
* [Config Management] [PR 682] various bug fixes when integrate test with hdfs store
* [Core] [PR 690] Add missing jar to MR runner script
* [Distcp] [PR 691] Fix permissions for directories in distcp.
* [Core] [PR 700] Add missing jars to gobblin mapreduce runner, sort.
* [Core] [PR 706] Fixing CliOptions config file fs
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
