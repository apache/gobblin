GOBBLIN 0.16.0
--------------

### Created Date: 11/08/2021

## HIGHLIGHTS
* New Kafka 1.1 Writer.  
* Support for logical types in Avro-to-ORC.  
* Multiple improvements and bug fixes as detailed below. 

## NEW FEATURES
* [Writer] [GOBBLIN-1325] Add Kafka 1.1 Module : Writer
* [Writer] [GOBBLIN-1250] ORC Writer
* [AvroToORC] [GOBBLIN-1024] Support logical types in Avro-to-ORC
* [CLI] [GOBBLIN-1267] Support for running a single ingestion job using Gobblin CLI
* [GaaS] [GOBBLIN-1421] Add running status gauge in DagManager

## IMPROVEMENTS
* [Cluster] [GOBBLIN-1329] Suppress Helix log as part of TaskResult
* [Cluster] [GOBBLIN-1243] Add common job properties in hashtable and not as `default` properties
* [Cluster] [GOBBLIN-1251] Propagate failure from statsTracker scheduling
* [Cluster] [GOBBLIN-1355] Make task interruption optional on Gobblin task cancellation
* [GaaS] [GOBBLIN-1370] Make flow resume a restli action instead of partial update
* [GaaS] [GOBBLIN-1453] Improve error reporting on flow configs that fail to compile
* [GaaS] [GOBBLIN-1456] GobblinServiceJobScheduler update logic
* [Source] [GOBBLIN-968] Honor file split size for HadoopFileInputSource
* [Source] [GOBBLIN-1373] Remove fixed sleep time in HighLevelConsumer
* [Extractor] [GOBBLIN-1058] Make emitTrackingEvents method accept additional PartitionsToTags map for ease of extension on metrics
* [Writer] [GOBBLIN-1338] Log and suppress exceptions in FsDataWriter.getFinalState
* [Hive Registration] [GOBBLIN-1263] Enable dataset-specific DBName for registration
* [Hive Registration] [GOBBLIN-1272] Handling empty string in config-store loading during hive-Registration
* [Hive Registration] [GOBBLIN-1278] Close HiveRegister in completion action usage
* [Hive Registration] [GOBBLIN-1309] Abort operation when registration against a view
* [Hive Registration] [GOBBLIN-1389] Logging with exception propagated on HiveRegister
* [ORC] [GOBBLIN-1305] Processing Split correctly in ORC RecordReader
* [ORC] [GOBBLIN-1264] Publish separate GOBBLIN-orc module
* [ORC] [GOBBLIN-1265] Use shadowed imports from ORC library with nohive classifier in GOBBLIN-orc module
* [ORC] [GOBBLIN-1270] Remove reference to determineSchemaOrReturnErrorSchema
* [ORC] [GOBBLIN-1290] Auto tune ORC writer parameters
* [ORC] [GOBBLIN-1330] Add support for decimal type in the GobblinOrcWriter
* [ORC] [GOBBLIN-1465] Refactor the GobblinOrcWriter to support using a different OrcValueWriter
* [Salesforce] [GOBBLIN-1298] Return JsonElement instead of null for the last element in resultChaining iterator
* [Salesforce] [GOBBLIN-1426] Let child classes use SalesforceConnector code
* [Core] [GOBBLIN-1495] NPE when trying to fetch Hadoop tokens for cluster with no remote namenodes
* [Core] [GOBBLIN-1249] Log failure info when MRTask is failed
* [State Store] [GOBBLIN-1380] Add retention to failed dag state store
* [Metastore] [GOBBLIN-1044] Enrich fork-failure information when task failed
* [Job Launcher] [GOBBLIN-1366] Add an option to skip initialization of hadoop tokens in the AzkabanJobLauncher
* [Util] [GOBBLIN-1227] Treat AccessDeniedException in RenameRecursively as an existence indicator
* [Util] [GOBBLIN-1392] Safe temporary file creation
* [Util] [GOBBLIN-1429] Add secure TrustManager for LDAP Utils
* [Runtime] [GOBBLIN-1372] Refactor runtime-jvm argument setting
* [Documentation] [GOBBLIN-1376] Add docker guide link to readme
* [Documentation] [GOBBLIN-1371] Improve Readme to reflect current capabilities
* [Documentation] [GOBBLIN-1501] IcebergMetadataWriter Documentation and cleanup
* [Documentation] [GOBBLIN-1275] Migrate Gitter links to Slack links in docs
* [Github] [GOBBLIN-1332] Move to CommunityInviter based links to prevent expiring Slack invites
* [Github] [GOBBLIN-1333] Update Github About section for Gobblin
* [Build] [GOBBLIN-1293] Upgrade Gradle from 4.9 to 5.6
* [Build] [GOBBLIN-1313] Create Github Actions to Automatically build and publish Docker Images
* [Build] [GOBBLIN-1346] Clean up deprecated Docker images, change latest tag to only be generated on releases
* [Build] [GOBBLIN-1361] Provide client-specific ivysettings.xml without conflicts with existed setting files

## BUG FIXES 
* [Bug] [GOBBLIN-831] Fix NPE in KafkaWorkUnitPacker when there is no WorkUnit created
* [Bug] [GOBBLIN-1042] ForkMetrics generates parent metric object with incorrect type
* [Bug] [GOBBLIN-1060] Fix YarnAppLauncher resource existence checking with wrong fs object
* [Bug] [GOBBLIN-1274] Fix a typo in import statement
* [Bug] [GOBBLIN-1348] Github actions fails due to restriction to only verified actions
* [Bug] [GOBBLIN-1353] Slack link in Readme has expired
* [Bug] [GOBBLIN-1354] Fix Documentation Build
* [Bug] [GOBBLIN-1362] Fix bug where state is set twice when no workunits created
* [Bug] [GOBBLIN-1407] Fix wrong google-http-client import
* [Bug] [GOBBLIN-1431] Fix unit-test TestSingleTask
* [Bug] [GOBBLIN-1432] JVM hangs on flushing events after OOM
* [Bug] [GOBBLIN-1433] Github Actions Tests break with SSL Handshake error for SQL
* [Bug] [GOBBLIN-1294] Alpine Linux Docker images no longer work
* [Bug] [GOBBLIN-1369] Fix wrong method name: exractSampleRecordCountFromQuery
* [Bug] [GOBBLIN-1379] Distcp hides real exception when retry happen

GOBBLIN 0.15.0
--------------

###Created Date: 13/10/2020

## HIGHLIGHTS
* New and more performant Gobblin ORC Writer
* Auto-scaling of Gobblin on Yarn.
* New MySQL based DAG state store.
* New FileSystem based Spec Producer and Job Status Retriever for GaaS. 
* Search GaaS flow configs using flow properties
* Flow level SLA and flow cancel features in GaaS
* Flow catalog that updates dynamically as FileSystem is modified. 
* A FileSystem based Job Configuration Manager.
* Version strategy support and enhancements for distcp of Hive and Config based datasets.
* Zuora Connector for Gobblin. 

## NEW FEATURES
* [GaaS] [GOBBLIN-1196] search flow configs using flow properties and/or other parameters
* [GaaS] [GOBBLIN-1115] Add flow level data movement authorization in gaas
* [GaaS] [GOBBLIN-1073] Add proxy user and requester quota to GaaS
* [GaaS] [GOBBLIN-847] Flow level sla
* [GaaS] [GOBBLIN-808] implement azkaban flow cancel when dag manager is enabled
* [GaaS] [GOBBLIN-790] DagStateStore MySQL
* [GaaS] [GOBBLIN-781] Skeleton for GaaS DR mode clean transition
* [GaaS] [GOBBLIN-775] Add job level retries for gobblin service
* [GaaS] [GOBBLIN-768] Add MySQL implementation of SpecStore
* [GaaS] [GOBBLIN-756] Add flow catalog that updates when filesystem is modified
* [GaaS] [GOBBLIN-725] Add a mysql based job status retriever
* [GaaS] [GOBBLIN-723] Add support to the LogCopier for copying from multiple source paths
* [GaaS] [GOBBLIN-708] Create SqlDatasetDescriptor for JDBC-sourced datasets
* [GaaS] [GOBBLIN-673] Implement a FS based JobStatusRetriever for GaaS Flows.
* [Cluster] [GOBBLIN-1162] Provide an option to allow slow containers to commit suicide
* [Cluster] [GOBBLIN-1031] Gobblin-on-Yarn locally running Azkaban job skeleton
* [Cluster] [GOBBLIN-789] Implement a FileSystem based SpecProducer.
* [Cluster] [GOBBLIN-762] Add automatic scaling for Gobblin on YARN
* [Cluster] [GOBBLIN-742] Implement a FileSystem based JobConfigurationManager
* [Cluster] [GOBBLIN-737] Add support for Helix quota-based task scheduling
* [Cluster] [GOBBLIN-649] Add task driver cluster
* [Writer] [GOBBLIN-1250] Open Sourcing ORC writer
* [Salesforce] [GOBBLIN-865] Add feature that enables PK-chunking in partition
* [Compaction] [GOBBLIN-699] Orc compaction impl.
* [Core] [GOBBLIN-677] Allow early termination of Gobblin jobs based on a predicate on the job progress
* [Distcp] [GOBBLIN-772] Implement Schema Comparison Strategy during Disctp
* [Distcp] [GOBBLIN-729] Add version strategy support for HiveCopyDataset
* [Distcp] [GOBBLIN-712] Add version strategy pickup for ConfigBasedDataset distcp workflow
* [Source] [GOBBLIN-716] Add lineage in FileBasedSource
* [Source] [GOBBLIN-628] Zuora Connector
* [Hive Registration] [GOBBLIN-693] Add ORC hive serde manager

## IMPROVEMENTS
* [Cluster] [GOBBLIN-1260] add some logs in GobblinHelixTask
* [Cluster] [GOBBLIN-1251] Propagate exception in TaskStateTracker for caller to trigger Helix retry
* [Cluster] [GOBBLIN-1213] add common job properties to jobProps using putAll
* [Cluster] [GOBBLIN-1209] Provide an option to configure the java tmp dir to the Yarn cache location
* [Cluster] [GOBBLIN-1199] convert seconds to ms because helix api take time in ms
* [Cluster] [GOBBLIN-1192] Commit suicide if Helix Task creation failed after retry
* [Cluster] [GOBBLIN-1191] Reuse Helix instance names when containers are released by Gobblin Application Master
* [Cluster] [GOBBLIN-1184] publish gobblin-cluster-test to artifactory
* [Cluster] [GOBBLIN-1183] Enable additional yarn class path set for app master
* [Cluster] [GOBBLIN-1178] Propagation of exception from task creation to helix and request for new container
* [Cluster] [GOBBLIN-1177] Provide a config for overprovisioning Gobblin Yarn containers by a configurable amount
* [Cluster] [GOBBLIN-1175] Provide an option to all GobblinYarnAppLauncher to detach from Yarn application
* [Cluster] [GOBBLIN-1165] Add config to enable user to set additional yarn classpathes
* [Cluster] [GOBBLIN-1152] enable helix instance only if it is a participant
* [Cluster] [GOBBLIN-1141] add support for common job properties in helix job scheduler
* [Cluster] [GOBBLIN-1136] Make LogCopier be able to refresh FileSystem for long running job use cases
* [Cluster] [GOBBLIN-1122] Bump up helix-lib version
* [Cluster] [GOBBLIN-1120] Reinitialize HelixManager when Helix participant check throws an exception
* [Cluster] [GOBBLIN-1107] Lazily initialize Helix TaskStateModelFactory in GobblinTaskRunner
* [Cluster] [GOBBLIN-1099] Handle orphaned Yarn containers in Gobblin-on-Yarn clusters
* [Cluster] [GOBBLIN-1078] Adding condition to ensure cancellation happened after run
* [Cluster] [GOBBLIN-1076] Make Gobblin cluster working directories configurable
* [Cluster] [GOBBLIN-1072] Being more conservative on releasing containers
* [Cluster] [GOBBLIN-1071] Retry task initialization
* [Cluster] [GOBBLIN-1052] Create a spec consumer path if it does not exist in FS SpecConsumer
* [Cluster] [GOBBLIN-1048] Provide an option to pass and set System properties via Gobblin Cluster application config
* [Cluster] [GOBBLIN-1047] Add Helix and Yarn container metadata to all task events emitted by Gobblin Helix tasks
* [Cluster] [GOBBLIN-1044] Enrich fork-failure information when task failed
* [Cluster] [GOBBLIN-1043] Implement a Helix assigned participant check as a CommitStep
* [Cluster] [GOBBLIN-1036] Add hadoop override configurations when instantiating FileSystem object in GobblinTaskRunner and GobblinClusterManager
* [Cluster] [GOBBLIN-1032] Provide Helix instance tags config to GobblinYarnTaskRunner
* [Cluster] [GOBBLIN-1030] Refactor AbstractYarnSecurityManager to expose method for sending token file update message
* [Cluster] [GOBBLIN-1029] Maintain the last GC stats to accurately report the difference in each interval
* [Cluster] [GOBBLIN-1018] Report GC counts and durations from Gobblin containers metrics service
* [Cluster] [GOBBLIN-1016] Allow Gobblin Application Master to join Helix cluster in PARTICIPANT mode when Helix cluster is managed
* [Cluster] [GOBBLIN-996] Add support for managed Helix clusters for Gobblin-on-Yarn applications
* [Cluster] [GOBBLIN-973] Increase timeout for copying Gobblin workunits to the workunit state store in GobblinHelixJobLauncher
* [Cluster] [GOBBLIN-967] Change token refresh method in YarnContainerSecirityManager
* [Cluster] [GOBBLIN-916] Make ContainerLaunchContext instantiation in YarnService more efficient
* [Cluster] [GOBBLIN-913] Add MySQL and configurations to cluster
* [Cluster] [GOBBLIN-904] Provide an option to reuse an existing Helix cluster on Gobblin-Yarn application launch
* [Cluster] [Gobblin-902] Enable gobblin yarn app luncher class configurable
* [Cluster] [GOBBLIN-875] Emit container health metrics when running in cluster mode
* [Cluster] [GOBBLIN-846] Enhance LogCopier service to handle continuous YARN log aggregation
* [Cluster] [GOBBLIN-836] Expose container logs location via system property to be used in log4j configuration for Gobblin-on-Yarn applications
* [Cluster] [GOBBLIN-817] Implement a workaround for Helix Workflow being stuck in STOPPING state.
* [Cluster] [GOBBLIN-834] Provide config for setting ACLs to control visibility of Gobblin-on-Yarn application logs
* [Cluster] [GOBBLIN-816] Implement a workaround to abort Helix TaskDriver#getWorkflows() after a timeout
* [Cluster] [GOBBLIN-798] Clean up workflows from Helix when the Gobblin application master starts
* [Cluster] [GOBBLIN-795] Make JobCatalog optional for FsJobConfigurationManager
* [Cluster] [GOBBLIN-780] Handle scenarios that cause the YarnAutoScalingManager to be stuck
* [Cluster] [GOBBLIN-777] Remove container request after container allocation
* [Cluster] [GOBBLIN-776] Add a utility method to return Helix WorflowId given a Gobblin job name.
* [Cluster] [GOBBLIN-770] Add JVM configuration to avoid exhausting YARN container memory
* [Cluster] [GOBBLIN-762] Add automatic scaling for Gobblin on YARN
* [Cluster] [GOBBLIN-744] Support cancellation of a Helix workflow via a DELETE Spec.
* [Cluster] [GOBBLIN-742] Implement a FileSystem based JobConfigurationManager.
* [Cluster] [GOBBLIN-743] Initialize Gobblin application master services with dynamic config
* [Cluster] [GOBBLIN-739] Add a way to propagate the Azkaban job config to Gobblin on YARN
* [Cluster] [GOBBLIN-737] Add support for Helix quota-based task scheduling
* [Cluster] [GOBBLIN-732] Pass UGI credentials to the app master and load dynamic config in workers
* [Cluster] [GOBBLIN-720] Always delete state store
* [Cluster] [GOBBLIN-723] Add support to the LogCopier for copying from multiple source paths
* [Cluster] [GOBBLIN-703] Allow planning job to be running in non-blocking mode
* [Cluster] [GOBBLIN-679] Refactor GobblinHelixTask metrics
* [Cluster] [GOBBLIN-655] Allow helix job to have a job type.
* [Cluster] [GOBBLIN-652] Add helix metrics
* [Cluster] [GOBBLIN-649] Add task driver cluster
* [Cluster] [GOBBLIN-647] Move early stop logic to task driver instance.
* [Standalone] [GOBBLIN-1267] Gobblin cli: Add a quickApp called oneShot to run a single command in standalone and MR mode
* [Standalone] [GOBBLIN-903] Initialize docker file for gobblin-standalone and fix docker compose
* [Standalone] [GOBBLIN-707] rewrite gobblin script to combine all modes and command
* [Standalone] [GOBBLIN-883] Add docker files and compose
* [GaaS] [GOBBLIN-1258] Add error message in status for unauthorized flows
* [GaaS] [GOBBLIN-1269] add metrics field in JobStatus schema
* [GaaS] [GOBBLIN-1268] track WORK_UNITS_PREPARATION timer event also in gaas
* [GaaS] [GOBBLIN-1262] Update flow execution as failed if it was skipped due to concurrently running flow
* [GaaS] [GOBBLIN-1241] Allow nodes/edges in HOCON format and delay resolution to allow overriding
* [GaaS] [GOBBLIN-1255] Wait for compiler to be healthy before scheduling flows on startup
* [GaaS] [GOBBLIN-1254] Skip undecodeable message in KafkaAvroJobStatusMonitor
* [GaaS] [GOBBLIN-1253] Update running jobs counter on Gobblin service restart
* [GaaS] [GOBBLIN-1252] Provide a default flow SLA for Gobblin Service flows
* [GaaS] [GOBBLIN-1230] add option to add spec executor configs to gaas job
* [GaaS] [GOBBLIN-1198] status cleaner
* [GaaS] [GOBBLIN-1168] add metrics in all SpecStore implementations
* [GaaS] [GOBBLIN-1154] Improve gaas error messages
* [GaaS] [GOBBLIN-1150] spec catalog table schema change
* [GaaS] [GOBBLIN-1149] Abstract out method for constructing descriptor from config
* [GaaS] [GOBBLIN-1144] remove specs from gobblin service job scheduler
* [GaaS] [GOBBLIN-1137] Add API for getting list of proxy users from an azkaban project
* [GaaS] [GOBBLIN-1135] added back flow remove feature for spec executors when dag manager is not enabled codesyle changes
* [GaaS] [GOBBLIN-1132] move the logic of requester list verification to RequesterService implementation
* [GaaS] [GOBBLIN-1130] Add API for adding proxy user to azkaban project
* [GaaS] [GOBBLIN-1125] Add metrics to measure job status state store performance in Gobblin Service
* [GaaS] [GOBBLIN-1123] Report orchestration delay for Gobblin Service flows
* [GaaS] [GOBBLIN-1105] some refactoring and make MysqlJobStatusStateStore implements DatasetStateStore
* [GaaS] [GOBBLIN-1090] send compiled_skip metrics
* [GaaS] [GOBBLIN-1086] Add job orchestrated time, use job start/prepare time to set job start time in GaaS jobs
* [GaaS] [GOBBLIN-1084] Refresh flowgraph when templates are modified
* [GaaS] [GOBBLIN-1082] compile a flow before storing it in spec catalog
* [GaaS] [GOBBLIN-1075] Add option to return latest failed flows
* [GaaS] [GOBBLIN-1074] Sort job status array when returning flow status
* [GaaS] [GOBBLIN-1067] Add SFTP DataNode type in Gobblin-as-a-Service (GaaS) FlowGraph
* [GaaS] [GOBBLIN-1051] Emit Helix Leader Metrics
* [GaaS] [GOBBLIN-1050] Verify requester when updating/deleting FlowConfig
* [GaaS] [GOBBLIN-1038] Set default dataset descriptor configs based on the DataNode
* [GaaS] [GOBBLIN-1035] make hive dataset descriptor accepts regexed db and tables
* [GaaS] [GOBBLIN-1027] add metrics for users running gaas jobs
* [GaaS] [GOBBLIN-1017] Deprecate FlowStatus in favor of FlowExecution and add endpoint to kill flows
* [GaaS] [GOBBLIN-1003] HiveDataNode node addition to support adl and abfs URI for gobblin-Service
* [GaaS] [GOBBLIN-988] Implement LocalFSJobStatusRetriever
* [GaaS] [GOBBLIN-958] make hive flow edge accept multiple tables
* [GaaS] [GOBBLIN-953] Add scoped config for app launcher created by gobblin service
* [GaaS] [GOBBLIN-948] add hive data node and descriptor
* [GaaS] [GOBBLIN-946] Add HttpDatasetDescriptor and HttpDataNode to Gobblin Service
* [GaaS] [GOBBLIN-932] Create deployment for Azure, clean up existing deployments
* [GaaS] [GOBBLIN-925] Create option to log outputs to console, fix docker-compose
* [GaaS] [GOBBLIN-917] kill orphan gaas jobs
* [GaaS] [GOBBLIN-914] accept more tracking events in gaas
* [GaaS] [GOBBLIN-906] Initializes kubernetes cluster for GaaS and Gobblin Standalone
* [GaaS] [GOBBLIN-897] adds local FS spec executor to write jobs to a local dir
* [GaaS] [GOBBLIN-894] Add option to combine datasets into a single flow
* [GaaS] [GOBBLIN-882] Modify application config so that GaaS runs
* [GaaS] [GOBBLIN-881] Add job tag field that can be used to filter job statuses
* [GaaS] [GOBBLIN-870] Adding abfs scheme
* [GaaS] [GOBBLIN-860] Process flow-level events for setting/retrieving flow status
* [GaaS] [GOBBLIN-856] make job status monitor a top level service
* [GaaS] [GOBBLIN-855] persist dag after addspec
* [GaaS] [GOBBLIN-853] Support multiple paths specified in flow config
* [GaaS] [GOBBLIN-837] refactor FlowConfigV2Client and FlowStatusClient to allow child classes modify respective RequestBuilders
* [GaaS] [GOBBLIN-828] Make dynamic config override job config
* [GaaS] [GOBBLIN-810] Include flow edge ID in job name
* [GaaS] [GOBBLIN-796] Add support partial updates for flowConfig
* [GaaS] [GOBBLIN-793] Separate SpecSerDe from SpecCatalogs and add GsonSpecSerDe
* [GaaS] [GOBBLIN-792] submit a GobblinTrackingEvent when jobs are compiled but not yet orchestrated
* [GaaS] [GOBBLIN-782] Add dynamic config to JobSpec
* [GaaS] [GOBBLIN-781] Skeleton for GaaS DR mode clean transition
* [GaaS] [GOBBLIN-790] DagStateStore MySQL
* [GaaS] [GOBBLIN-786] Separate SerDe library in DagStateStore out for GaaS-wide sharing
* [GaaS] [GOBBLIN-779] make job status retriever configurable
* [GaaS] [GOBBLIN-775] Add job level retries for gobblin service
* [GaaS] [GOBBLIN-773] handle job cancellation case in status monitor
* [GaaS] [GOBBLIN-771] add a few metrics for gobblin service
* [GaaS] [GOBBLIN-768] Add MySQL implementation of SpecStore
* [GaaS] [GOBBLIN-765] Remove a duplicate leading period character from the config key for SqlDataNode
* [GaaS] [GOBBLIN-756] Add flow catalog that updates when filesystem is modified
* [GaaS] [GOBBLIN-748] Craftsmanship code cleaning in Gobblin Service Code
* [GaaS] [GOBBLIN-746] Async loading FlowSpec
* [GaaS] [GOBBLIN-730] added job start and end time in flow status retriever
* [GaaS] [GOBBLIN-731] Make deserialization of FlowSpec more robust
* [GaaS] [GOBBLIN-725] add a mysql based job status retriever
* [GaaS] [GOBBLIN-722] add option to unschedule a flow set schedule even if the job is already scheduled
* [GaaS] [GOBBLIN-720] Always delete state store
* [GaaS] [GOBBLIN-713] Lazy load job specification from job catalog to avoid OOM issue.
* [GaaS] [GOBBLIN-709] Provide an option to disallow concurrent flow executions in Gobblin-as-a-Service
* [GaaS] [GOBBLIN-708] Create SqlDatasetDescriptor for JDBC-sourced datasets.
* [GaaS] [GOBBLIN-698] Enhance logging to print job and flow details when a job is orchestrated by GaaS
* [GaaS] [GOBBLIN-696] Provide an "explain" option to return a compiled flow when a flow config is added.
* [GaaS] [GOBBLIN-692] Add support to query last K flow executions in Gobblin-as-a-Service (GaaS)
* [GaaS] [GOBBLIN-687] Pass TopologySpec map to DagManager to allow reuse of SpecExecutors during DAG deserialization
* [GaaS] [GOBBLIN-683] Add azkaban client retry logic.
* [GaaS] [GOBBLIN-681] increase max size of job name
* [GaaS] [GOBBLIN-688] Make FsJobStatusRetriever config more scoped.
* [GaaS] [GOBBLIN-678] Make flow.executionId available in the GaaS Flow config for use in job templates.
* [GaaS] [GOBBLIN-675] Enhance FSDatasetDescriptor definition to include partition config, encryption level and compaction config.
* [GaaS] [GOBBLIN-673] Implement a FS based JobStatusRetriever for GaaS Flows.
* [GaaS] [GOBBLIN-667] Pass encrypt.key.loc configuration to GitFlowGraphMonitor.
* [GaaS] [GOBBLIN-664] Refactor Azkaban Client for session refresh.
* [GaaS] [GOBBLIN-662] Enhance SSH-based access to Git to enable/disable host key checking.
* [GaaS] [GOBBLIN-658] Submit a JobFailed event when an exception is encountered during Job orchestration in Gobblin service.
* [GaaS] [GOBBLIN-653] Create JobSucceededTimer tracking event to accurately track successful Gobblin jobs.
* [GaaS] [GOBBLIN-646] Refactor MultiHopFlowCompiler to use SpecExecutor configs from TopologySpecMap.
* [GaaS] [GOBBLIN-644] Add metrics reporting config dynamically to compiled flows in MultiHopFlowCompiler.
* [GaaS] [GOBBLIN-639] Change method to static for RequesterService serder                                                                                                          
* [GaaS] [GOBBLIN-638] Submit more timing events from GaaS to accurately track flow/job status.
* [GaaS] [GOBBLIN-636] Use FS scheme and relative URIs for specifying job template locations in GaaS.
* [Compaction] [GOBBLIN-1231] Make re-compaction be able to write to a new folder based on the executCount
* [Compaction] [GOBBLIN-1223] Change the criteria for re-compaction, limit the time for re-compaction
* [Compaction] [GOBBLIN-1214] Move the fallback of in-eligible shuffleKey to driver
* [Compaction] [GOBBLIN-1201] Add datset.urn in GTE for MRCompactionTask
* [Compaction] [GOBBLIN-1190] Fallback to full schema if configured shuffle schema is not available
* [Compaction] [GOBBLIN-1126] Make ORC compaction shuffle key configurable
* [Compaction] [GOBBLIN-1133] Add CompactionSuiteBaseWithConfigurableCompleteAction to make complete action configurable
* [Compaction] [GOBBLIN-1045] Emit more events in compaction job
* [Compaction] [GOBBLIN-1012] Implement CompactionWithWatermarkSuite
* [Compaction] [GOBBLIN-763] Support fields removal for compaction dedup key schema
* [Compaction] [GOBBLIN-848] Make initialization of CompactionSource extensible with certain protection
* [Compaction] [GOBBLIN-699] Orc compaction impl.
* [Compaction] [GOBBLIN-691] Make format-specific component pluggable in compaction
* [Compaction] [GOBBLIN-1011] adjust compaction flow to work with virtual partition
* [Compaction] [GOBBLIN-1158] Use input dir to document old files instead of file pathes to reduce memory cost in Compaction configurator
* [Compaction] [GOBBLIN-1117] Enable record count verification for ORC format
* [Compaction] [GOBBLIN-884] Support ORC schema evolution across mappers in MR mode
* [Hive Registration] [GOBBLIN-1263] Dataset specific Database name for registration
* [Hive Registration] [GOBBLIN-1206] Only populate path to dest-table if src-table has it as storageParam
* [Hive Registration] [GOBBLIN-1145] add path in serde props
* [Hive Registration] [GOBBLIN-1006] Enable configurable case-preserving and schema source-of-truth in table level properties
* [Hive Registration] [GOBBLIN-993] Support job level hive configuration override
* [Hive Registration] [GOBBLIN-986] persist the existing property of iceberg
* [Hive Registration] [GOBBLIN-954] Added support to swap different HiveRegistrationPublishers
* [Hive Registration] [GOBBLIN-941] Enhance DDL to add column and column.types with case-preserving schema
* [Hive Registration] [GOBBLIN-912] Enable TTL caching on Hive Metastore client connection
* [Hive Registration] [GOBBLIN-877] Add column metadata for partition for inline hive registration
* [Hive Registration] [GOBBLIN-861] Skip getPartition() call to Hive Metastore when a partition already exists
* [Hive Registration] [GOBBLIN-851] Provide capability to disable Hive partition schema registration.
* [Hive Registration] [GOBBLIN-852] Reorganize the code for hive registration to isolate function
* [Hive Registration] [GOBBLIN-753] Refactor HiveRegistrationPolicyBase to surface configStore object
* [Hive Registration] [GOBBLIN-705] create method to merging tblProps from existing hive meta table
* [Hive Registration] [GOBBLIN-704] Add serde attributes for orc
* [Hive Registration] [GOBBLIN-693] Add ORC hive serde manager
* [Hive Registration] [GOBBLIN-1148] improve hive test coverage
* [Hive Registration] [GOBBLIN-921] Make pull/push mode when registering partition to be configurable
* [Hive Registration] [GOBBLIN-893] Make format-check in ORC-registration optional and by-default disabled
* [Distcp] [GOBBLIN-1227] Treat AccessDeniedException in RenameRecursively as an existence indicator
* [Distcp] [GOBBLIN-1221] Preserve source file's ModTime by configuration
* [Distcp] [GOBBLIN-1216] Embedded Hive Distcp
* [Distcp] [GOBBLIN-1203] Adding configurations for staging directory in Embedded Distcp template
* [Distcp] [GOBBLIN-1142] Hive Distcp support filter on partitioned or snapshot tables
* [Distcp] [GOBBLIN-1057] Optimize unnecessary RPCs in distcp-ng
* [Distcp] [GOBBLIN-1001] Implement TimePartitionGlobFinder
* [Distcp] [GOBBLIN-962] Refactor RecursiveCopyableDataset.
* [Distcp] [GOBBLIN-961] Bypass locked directories when calculating src watermark
* [Distcp] [GOBBLIN-910] Added a unix timestamp recursive copyable dataset finder
* [Distcp] [GOBBLIN-899] Add config in replication config to determine wheter schema cehck enable for the dataset
* [Distcp] [GOBBLIN-888] Make yyyy-MM-dd-HH-mm recognizable in TimeAwareRecursiveCopyableDataset
* [Distcp] [GOBBLIN-784] Allow setting replication factor in distcp
* [Distcp] [GOBBLIN-772] Implement Schema Comparison Strategy during Disctp
* [Distcp] [GOBBLIN-751] Make enforced file size matching to be configurable
* [Distcp] [GOBBLIN-729] Add version strategy support for HiveCopyDataset
* [Distcp] [GOBBLIN-726] Enable schema check 
* [Distcp] [GOBBLIN-712] Add version strategy pickup for ConfigBasedDataset distcp workflow
* [Distcp] [GOBBLIN-697] Implementation of data file versioning and preservation in distcp.
* [Distcp] [GOBBLIN-598] Add documentation on split enabled distcp (config glossary & gobblin distcp page)
* [Kafka] [GOBBLIN-1229] Make topic specific state available to Kafka workunit packer[]
* [Kafka] [GOBBLIN-1143] Add a generic wrapper producer client to communicate with Kafka
* [Kafka] [GOBBLIN-1112] Implement a new HttpMethodRetryHandler that allows retrying a HTTP method on transient network errors
* [Kafka] [GOBBLIN-1064] Make KafkaAvroSchemaRegistry extendable
* [Kafka] [GOBBLIN-1040] HighLevelConsumer re-design
* [Kafka] [GOBBLIN-970] Pass metric context from the KafkaSource to the KafkaWorkUnitPacker for emission of metrics from the packer
* [Kafka] [GOBBLIN-886] add callback to kafka apis
* [Kafka] [GOBBLIN-857] Extending getTopicsFromConfigStore to accept topicName directly
* [Kafka] [GOBBLIN-684] Ensure buffered messages are flushed before close() in KafkaProducerPusher
* [Kafka] [GOBBLIN-651] Ensure ordered delivery of Kafka events from KeyValueProducerPusher for kafka-08.
* [Kafka] [GOBBLIN-650] Ensure ordered delivery of Kafka events from KeyValueProducerPusher.
* [Kafka] [GOBBLIN-642] Implement KafkaAvroEventKeyValueReporter
* [Kafka] [GOBBLIN-640] Add a Kafka producer pusher that supports keyed messages
* [Avro-to-ORC] [GOBBLIN-1046] Make /final subdir configurable in ORC-conversion output
* [Avro-to-ORC] [GOBBLIN-1024] Supporting Avro logical type recognition in Avro-to-ORC transformation
* [Avro-to-ORC] [GOBBLIN-999] Separate Hive-Avro type related constants out of Avro2ORC specific module to make it re-usable
* [Avro-to-ORC] [GOBBLIN-975] Add flag to enable/disable avro type check in AvroToOrc
* [Avro-to-ORC] [GOBBLIN-755] add delimiter to hive queries
* [Salesforce] [GOBBLIN-1202] Add retry for REST API call
* [Salesforce] [GOBBLIN-1186] explicitly set source.querybased.salesforce.is.soft.deletes.pull.disabled for simple mode
* [Salesforce] [GOBBLIN-1179] Add typed config in salesforce
* [Salesforce] [GOBBLIN-1101] Enhance bulk api retry for ExceedQuota
* [Salesforce] [GOBBLIN-1025] Add retry for PK-Chuking iterator
* [Salesforce] [GOBBLIN-995] Add function to instantiate the BulkConnection in SFDC connector
* [Salesforce] [GOBBLIN-862] Security token encryption support in SFDC connector
* [Salesforce] [GOBBLIN-813] Make SFDC connector support encrypted Salesforce client id and client secret
* [Salesforce] [GOBBLIN-778] Moving config creation to a separate method
* [Global Throttling] [GOBBLIN-764] Allow injection of Rest.li configurations for throttling client and fixed unit test.
* [Global Throttling] [GOBBLIN-760] Improve retrying behavior of throttling client
* [Global Throttling] [GOBBLIN-749] Add logging to limiter server.
* [Global Throttling] [GOBBLIN-724] Throttling server delays responses for throttling causing too many connections
* [Source] [GOBBLIN-1174] Fail job on FileBasedSource ls invalid source directory
* [Source] [GOBBLIN-1056] Refactor to allow customizing client pool population in KafkaSource
* [Source] [GOBBLIN-1054] Refactor HiveSource to make partition filter extensible
* [Source] [GOBBLIN-879] Refactor bin-packer for better code reuse
* [Source] [GOBBLIN-874] Make WorkUnitPacker and SizeEstimator pluggable
* [Source] [GOBBLIN-738] Open a way to customize decoding KafkaConsumerRecord
* [Source] [GOBBLIN-716] Add lineage in FileBasedSource
* [Extractor] [GOBBLIN-1207] Clear references to potentially large objects in Fork, FileBasedExtractor, and HiveWritableHdfsDataWriter
* [Extractor] [GOBBLIN-1100] Set average fetch time in the KafkaExtractor even when metrics are disabled
* [Extractor] [GOBBLIN-1087] Track and report histogram of observed lag from Gobblin Kafka pipeline
* [Extractor] [GOBBLIN-1079] set extract.is.full property
* [Extractor] [GOBBLIN-1058] Refactor method emitting GTE for ease of adding new tags
* [Extractor] [GOBBLIN-1000] Add min and max LogAppendTime to tracking events emitted from Gobblin Kafka Extractor
* [Extractor] [GOBBLIN-989] Track and report record level SLA in Gobblin Kafka Extractor tracking event
* [Extractor] [GOBBLIN-955] Expose a method to get average record size in KafkaExtractorStatsTracker
* [Extractor] [GOBBLIN-945] Refactor Kafka extractor statistics tracking to allow code reuse across both batch and streaming execution modes
* [Extractor] [GOBBLIN-915] Allow user customize the Extract timezone.
* [Extractor] [GOBBLIN-890] Makeing ExtractID timeZone Configurable
* [Extractor] [GOBBLIN-887] Generialize UniversalKafkaSource to accept Extractor that not extending KafkaExtractor
* [Extractor] [GOBBLIN-876] Expose metrics() API in GobblinKafkaConsumerClient to allow consume metrics to be reported
* [Extractor] [GOBBLIN-873] Add offset look-back option in Kafka consumer
* [Extractor] [GOBBLIN-738] Open a way to customize decoding KafkaConsumerRecord
* [Extractor] [GOBBLIN-717] Filter Out Empty MultiWorkUnits
* [Extractor] [GOBBLIN-706] Enable dynamic mappers
* [Extractor] [GOBBLIN-833] Make SFTP connection timeout-table
* [Converter] [GOBBLIN-1080] Add configuration to add schema creation time in converter
* [Converter] [GOBBLIN-1081] Adding support of timestamp data type for CsvToJsonConverter
* [Converter] [GOBBLIN-1066] field projection with namespace
* [Converter] [GOBBLIN-983] use java string library for string format
* [Converter] [GOBBLIN-957] Add recursion eliminating code, converter for Avro
* [Converter] [GOBBLIN-933] add support for array of unions in json schema_new
* [Converter] [GOBBLIN-896] Clone schema and field props in AvroSchemaFieldRemover
* [Converter] [GOBBLIN-757] Adding utility functions to support decoration of Avro Generic Records
* [Converter] [GOBBLIN-755] Add delimiter to hive queries
* [Converter] [GOBBLIN-733] Instrument Avro Converters to allow converter metrics emission in both batch and streaming modes.
* [Converter] [GOBBLIN-686] Enhance schema comparison
* [Converter] [GOBBLIN-676] Add record metadata support to the RecordEnvelope
* [Quality Checker] [GOBBLIN-1119] Enable close-on-flush for quality-checker's err-file
* [Quality Checker] [GOBBLIN-1089] Refactor policyChecker for extensibility
* [Quality Checker] [GOBBLIN-971] Enable speculative execution awareness for RowQualityChecker
* [Writer] [GOBBLIN-1181] Make parquet-proto compileOnly dependency
* [Writer] [GOBBLIN-1155] Make socket connect timeout configurable for couchbase writer
* [Writer] [GOBBLIN-1147] Use one dfsClient in FsDataWriter to to rename and exists check to avoid inconsistency
* [Writer] [GOBBLIN-1146] Allow configuring autocommit in JDBCWriters
* [Writer] [GOBBLIN-1015] Adding support for direct Avro and Protobuf writes in Parquet format
* [Writer] [GOBBLIN-1008] Upgrading parquet dependency to org.apache.parquet. Fixing tests
* [Writer] [GOBBLIN-928] Craftsmanship cleaning and bumping up ORC version
* [Writer] [GOBBLIN-911] Make profiling of HiveWritableHdfsDataWriter easier by injecting jobConf
* [Writer] [GOBBLIN-880] Bump CouchbaseWriter Couchbase SDK version + write docs + cert based auth + enable TTL + dnsSrv
* [Writer] [GOBBLIN-859] let writer pass latest schema to WorkUnitState
* [Writer] [GOBBLIN-820] Add keyed write capability to Kafka writer
* [Writer] [GOBBLIN-769] Support string record timestamp in TimeBasedAvroWriterPartitioner
* [Writer] [GOBBLIN-767] Support different time units in TimeBasedWriterPartitioner
* [Writer] [GOBBLIN-736] Skip flush and control message handlers on closed writers in the CloseOnFlushWriterWrapper
* [Writer] [GOBBLIN-727] Skip commit in CloseOnFlushWriterWrapper if a commit has already been invoked on the underlying writer.
* [Writer] [GOBBLIN-695] Adding utility functions to generate Avro/ORC binary using json
* [Writer] [GOBBLIN-630] Add a concrete implementation for Postgres writer
* [Core] [GOBBLIN-1217] start metrics reporting with a few map-reduce properties
* [Core] [GOBBLIN-1189] Relax the condition for the increasing ingestion latency check
* [Core] [GOBBLIN-1049] Move workunit commit logic to the end of publish().
* [Core] [GOBBLIN-774] Send nack when a control message handler fails in Fork
* [Core] [GOBBLIN-721] Remove additional ack. Simplify watermark manager
* [Core] [GOBBLIN-706] enable dynamic mappers
* [Core] [GOBBLIN-677] Allow early termination of Gobblin jobs based on a predicate on the job progress
* [Core] [GOBBLIN-676] Add record metadata support to the RecordEnvelope
* [Core] [GOBBLIN-653] Create JobSucceededTimer tracking event to accurately track successful Gobblin jobs.
* [Runtime] [GOBBLIN-1249] More failure info when MRTask wrapped up
* [Runtime] [GOBBLIN-1232] One liner: Enable verbose mode for waitForCompletion call
* [Runtime] [GOBBLIN-1271] add MultiEventMetadataGenerator
* [Runtime] [GOBBLIN-1266] Refactor dataset lineage code to allow Lineage event emission in streaming mode
* [Runtime] [GOBBLIN-1041] send metrics for workunit creation time
* [Runtime] [GOBBLIN-992] Make parallelRunner timeout configurable in MRJobLauncher
* [Runtime] [GOBBLIN-976] Add dynamic config to the state before instantiating metrics reporter in MRJobLauncher
* [Runtime] [GOBBLIN-964] Add the enum JOB_SUCCEEDED to org.apache.gobblin.metrics.event.EventName]
* [Runtime] [GOBBLIN-938] Make job-template resolution available in all JobLaunchers
* [Runtime] [GOBBLIN-908] Customized Progress to enable speculative execution
* [Runtime] [GOBBLIN-766] Emit WorkUnitsCreated Count Event for MR deployed jobs.
* [Runtime] [GOBBLIN-864] add job error message in job state
* [Runtime] [GOBBLIN-787] Add an option to include the task start time in the output file name
* [Runtime] [GOBBLIN-766] Emit Workunits Created event
* [Runtime] [GOBBLIN-774] Send nack when a control message handler fails in Fork
* [Runtime] [GOBBLIN-713] Lazy load job specification from job catalog to avoid OOM issue.
* [Runtime] [GOBBLIN-685] Add dump jstack for EmbeddedGobblin
* [Gobblin Metrics] [GOBBLIN-1127] Provide an option to make metric reporting instantiation failure fatal
* [Gobblin Metrics] [GOBBLIN-1116] Avoid registering schema with schema registry during MetricReporting initialization from cluster workers
* [Gobblin Metrics] [GOBBLIN-800] Remove the metric context cache from GobblinMetricsRegistry
* [Gobblin Metrics] [GOBBLIN-802] change gauge metrics context to RootMetricsContext
* [Gobblin Metrics] [GOBBLIN-758] Added new reporters to emit MetricReport and GobblinTrackingEvent without serializing them. Also added random key generator for reporters.
* [Gobblin Metrics] [GOBBLIN-827] Add more events
* [Gobblin Metrics] [GOBBLIN-807] TimingEvent is now closeable, extends GobblinEventBuilder
* [Util] [GOBBLIN-757] Adding utility functions to support decoration of Avro Generic Records
* [Util] [GOBBLIN-695] Adding utility functions to generate Avro/ORC binary using json
* [REST] [GOBBLIN-1261] Migrate .pdsc schemas to .pdl
* [Data Management] [GOBBLIN-1233] Add case-aware support in WhitelistBlacklist and other small fixes
* [Data Management] [GOBBLIN-1222] Create right abstraction to assemble dataset staging dir for Hive dataset finder
* [Job Templates] [GOBBLIN-960] Resolving multiple templates in top-level
* [Job Templates] [GOBBLIN-701] Add secure templates (duplicate of #2571)
* [Retention] [GOBBLIN-1185] Enable dataset cleaner to emit kafka events
* [Retention] [GOBBLIN-806] Enable metrics reporter during dataset discovery for retention job
* [Retention] [GOBBLIN-682] Create a new constructor for DatasetCleanerJob.
* [MySQL] [GOBBLIN-1108] bump up mysql-connector
* [Config] [GOBBLIN-1157] get a json representation of object if config type is different
* [State Store] [GOBBLIN-1151] use gson in place of jackson for serialize/deserialize
* [Config Store] [GOBBLIN-761] Only instantiate topic-specific configStore object when topic.name is available
* [Embedded] [GOBBLIN-685] Add dump jstack for EmbeddedGobblin
* [Apache] [GOBBLIN-1245] Update CHANGELOG and NOTICE files in preparation for 0.…
* [Apache] [GOBBLIN-1246] Modify build scripts to fix failures in Nexus artifact …
* [Apache] [GOBBLIN-1244] Add new file patterns to rat exclusion list[]
* [Apache] [GOBBLIN-1275] Changing gitter link to slack invite link
* [Apache] [GOBBLIN-1246] Modify build scripts to fix failures in Nexus artifact publishing
* [Apache] [GOBBLIN-1244] Add new file patterns to rat exclusion list
* [Apache] [GOBBLIN-1215] adding travis retry
* [Apache] [GOBBLIN-1159] Added code to publish gobblin artifacts to bintray
* [Apache] [GOBBLIN-1172] Migrate to Ubuntu 18 with openjdk8 for Travis
* [Apache] [GOBBLIN-641] Reserve version 0.15.0 for next release
* [Build] [GOBBLIN-1287] Cleanup code duplication across tests that choose a random open port for spinning up an embedded mysql server
* [Build] [GOBBLIN-1284] Fix flaky tests causing local build failures
* [Build] [GOBBLIN-1279] Fix flaky unit tests involving REST.li calls to FlowConfig endpoint in Gobblin-as-a-Service
* [Build] [GOBBLIN-1277] Added travis_retry function to deploy phase
* [Build] [GOBBLIN-1265] Switch the dependency of gobblin-orc with the shadow jar instead
* [Build] [GOBBLIN-1264] Add gobblin-orc as publishing module
* [Build] [GOBBLIN-1256] Exclude log4j related jars in compile but include those in test
* [Build] [GOBBLIN-1247] Disable flaky unit tests causing intermittent build failures
* [Build] [GOBBLIN-1235] Migrate Log4J to SLF4J to provide a clean log environment for downstream users
* [Build] [GOBBLIN-1234] Fix unit tests that fail on local build after MySQL v8 bump up
* [Build] [GOBBLIN-1226] remove lombok from runtime classpath
* [Build] [GOBBLIN-1176] create gobblin-all module resolving full dependency tree
* [Build] [GOBBLIN-829] Executing jacocReport before uploading to codecov
* [Build] [GOBBLIN-821] Adding Codecov
* [Build] [GOBBLIN-735] Relocate all google classes to cover protobuf and guava dependency in orc-dep jar
* [Documentation] [GOBBLIN-1094] Added documentation of High level consumer
* [Documentation] [GOBBLIN-1053] Update Readme with new badges and links
* [Documentation] [GOBBLIN-669] Configuration Properties Glossary section of Docs hard to read
* [Documentation] [GOBBLIN-598] Add documentation on split enabled distcp (config glossary & gobblin distcp page)

## BUG FIXES
* [Bug] [GOBBLIN-1270] Fix reference to determineSchemaOrReturnErrorSchema which is backward incompatible
* [Bug] [GOBBLIN-1248] Fix discrepancy between table schema and file schema
* [Bug] [GOBBLIN-1228] Do not localize token file on new TaskRunner launch
* [Bug] [GOBBLIN-1276] Emit additional logs from Gobblin task execution for improved debuggability
* [Bug] [GOBBLIN-1272] Fixing bug in loading config-store when the entry is empty
* [Bug] [GOBBLIN-1274] fix import
* [Bug] [GOBBLIN-1257] Fix the handling of collection field types during ORC schema up-conversion in compaction
* [Bug] [GOBBLIN-1234] Fix unit tests that fail on local build after MySQL v8 bump up
* [Bug] [GOBBLIN-1220] Log improvement
* [Bug] [GOBBLIN-1219] do not schedule flow spec from slave instance of GobblinServiceJobScheduler
* [Bug] [GOBBLIN-1218] deserialize flow config properties using old method for backward compatibility
* [Bug] [GOBBLIN-1212] Handle non-Primitive type eligibility-check for shuffle key properly
* [Bug] [GOBBLIN-1210] Force AM to read from token file to update token when start up (replace PR)
* [Bug] [GOBBLIN-1213] Skip job state deserialization during SingleFailInCreationTask instantiation
* [Bug] [GOBBLIN-1211] Track unused Helix instances in a thread-safe manner
* [Bug] [GOBBLIN-1208] Fix - restApiRetryLimit cannot be set to 0
* [Bug] [GOBBLIN-1200] Fix bug when local network throttling distcp jobs
* [Bug] [GOBBLIN-1197] Attempting resolving race condition among different tests' port allocation
* [Bug] [GOBBLIN-1193] Ensure that ingestion latency is 0 when no records are consumed by Kafka Extractor
* [Bug] [GOBBLIN-1188] fix log message for SFDC iterators
* [Bug] [GOBBLIN-1180] Removed dependencies on gobblin-parquet
* [Bug] [GOBBLIN-1170] Add missing booleanWritable type
* [Bug] [GOBBLIN-1173] Prevent propagating exceptions from the finally block in KafkaSource#getWorkUnits
* [Bug] [GOBBLIN-1160] No spec delete on gobblin service start
* [Bug] [GOBBLIN-1163] Fix travis formatting error
* [Bug] [GOBBLIN-1124] Add exception error message.
* [Bug] [GOBBLIN-1121] Fix Issue that YarnService use the old token to acquire new container
* [Bug] [GOBBLIN-1118] Bump up ORC version to 1.6.2 to pick up ORC-569
* [Bug] [GOBBLIN-1113] Carry forward requester list property when updating flowconfig
* [Bug] [GOBBLIN-1114] OrcValueMapper schema evolution up-conversion recursive
* [Bug] [GOBBLIN-1111] CsvToJsonConverterV2 should not print out raw data in the log
* [Bug] [GOBBLIN-1110] fix deadlock in job cancellation replacing deprecated class MessageHandlerFactory with MultiTypeMessageHandlerFactory
* [Bug] [GOBBLIN-1109] partial rollback of PR#2836
* [Bug] [GOBBLIN-1106] do not remove requester list
* [Bug] [GOBBLIN-1102] Add link to GIP
* [Bug] [GOBBLIN-1100] Change access modifier for generateTagsForPartitions to accomodate with
* [Bug] [GOBBLIN-1098] Remove commons-lang and slf4j from the orc-dep fat jar
* [Bug] [GOBBLIN-1097] ResultChainingIterator.add should check if the argument iterator is null
* [Bug] [GOBBLIN-1096] Work with DST change in compaction watermark
* [Bug] [GOBBLIN-1092] added some logs, fix checkstyle, removed some redundant code
* [Bug] [GOBBLIN-1091] Pass Yarn application id as part of AppMaster and YarnTaskRunner's start up command
* [Bug] [GOBBLIN-1088] Don't lowercase partition pattern config
* [Bug] [GOBBLIN-1085] fix compaction initialization
* [Bug] [GOBBLIN-1077] Fix bug in HiveDataset.resolveConfig
* [Bug] [GOBBLIN-1069] Add NPE check in handleContainerCompletion method
* [Bug] [GOBBLIN-1065] Fix SSL verification issue for macOS
* [Bug] [GOBBLIN-1063] add log
* [Bug] [GOBBLIN-1062] Add log when loading dags from state store
* [Bug] [GOBBLIN-1060] Fix wrong fileSystem object in YarnApplauncher
* [Bug] [GOBBLIN-1042] Fix ForkMetric incorrect return type of parent metric object and relevant unit tests
* [Bug] [GOBBLIN-1037] Disable UnixTimestampRecursiveCopyableDatasetTest
* [Bug] [GOBBLIN-1034] Ensure underlying writers are expired from the PartitionedDataWriter cache to avoid accumulation of writers for long running Gobblin jobs
* [Bug] [GOBBLIN-1019] Change jcenter url to https
* [Bug] [GOBBLIN-1013] Fix prepare_release_config build step on Windows
* [Bug] [GOBBLIN-1014] Fix error handling in gobblin.sh
* [Bug] [GOBBLIN-1002] Set state id when deserializing state from Gobblin state store
* [Bug] [GOBBLIN-998] ExecutionStatus should be reset to PENDING before a job retries
* [Bug] [GOBBLIN-997] Add serialVersionUID to FlowSpec for backwards compatibility
* [Bug] [GOBBLIN-994] fix wrong import org.testng.collections.Lists
* [Bug] [GOBBLIN-990] Don't allow creation of flow config that already exists
* [Bug] [GOBBLIN-987] Reject unrecognized Enum symbols in JsonRecordAvroSchemaToAvroConverter
* [Bug] [GOBBLIN-981] Handle backward compatibility issue in HiveSource
* [Bug] [GOBBLIN-980] Fix AzkabanClient always throwing exception when cancelling flow
* [Bug] [GOBBLIN-978] Use job start time instead of flow start time to kill jobs stuck in ORCHESTRATED state
* [Bug] [GOBBLIN-977] Update the misleading comments in BaseAbstractTask
* [Bug] [GOBBLIN-974] Avoid updating job/flow status if messages arrive out of order
* [Bug] [GOBBLIN-972] Make DEFAULT_NUM_THREADS in DagManager public
* [Bug] [GOBBLIN-969] Bump orc version for bug fixes
* [Bug] [GOBBLIN-966] Check if no partitions have been processed by KafkaExtractor in close() method to avoid ArrayIndexOutOfBoundsException
* [Bug] [GOBBLIN-963] Remove duplicated copies of TaskContext/TaskState when constructing TaskIFaceWrapper
* [Bug] [GOBBLIN-956] Continue loading dags until queue is drained
* [Bug] [GOBBLIN-950] Avoid persisting dag right after loading it on startup
* [Bug] [GOBBLIN-940] Add synchronization on workunit persistency before Helix job launching
* [Bug] [GOBBLIN-937] fix help text and align it with variable names
* [Bug] [GOBBLIN-924] Get rid of orc.schema.literal in ORC-ingestion and registration
* [Bug] [GOBBLIN-923] Fix Array and Map JsonElement converters to handle nullable elements
* [Bug] [GOBBLIN-922] fix start sla time unit conversion issue
* [Bug] [GOBBLIN-919] Using apache commons Pair API
* [Bug] [GOBBLIN-909] Return error message for unresolved substitutions in explain query
* [Bug] [GOBBLIN-905] Fixes issue where newly added jobs would crash in gobblin standalone's job conf folder
* [Bug] [GOBBLIN-895] Fixes Gobblin Standalone configs and scripts so that the user guide is accurate
* [Bug] [GOBBLIN-892] reverting back bad changes done in PR 2720
* [Bug] [GOBBLIN-891] Fixing Couchbase writer docs
* [Bug] [GOBBLIN-887] Fix the FileContext wrong fsUri issue.
* [Bug] [GOBBLIN-885] Fix orc-Compaction bug in non-dedup mode and add unit-test
* [Bug] [GOBBLIN-872] Only use one CouchbaseEnvironment instane per JVM
* [Bug] [GOBBLIN-868] Check flow status instead of job status to determine if flow is running
* [Bug] [GOBBLIN-863] Handle race condition issue for hive registration
* [Bug] [GOBBLIN-841] make some fields public
* [Bug] [GOBBLIN-840] avoid creating a flow execution id by both master and slave gaas
* [Bug] [GOBBLIN-839] Catch all exceptions when getting flow template at runtime
* [Bug] [GOBBLIN-838] Fix Ivy-based ConfigStoreUtils and add Unit Test
* [Bug] [GOBBLIN-825] Initialize message schema at object construction rather than creating a new instance for every message
* [Bug] [GOBBLIN-809] Fix RateBasedLimitter factory to get double instead of long rate for initialization
* [Bug] [GOBBLIN-805] Fix dag being cleaned twice
* [Bug] [GOBBLIN-804] Fix config member variable not being set
* [Bug] [GOBBLIN-799] Fix bug in AvroSchemaCheckDefaultStrategy
* [Bug] [GOBBLIN-798] Clean up workflows from Helix when the Gobblin application dies
* [Bug] [GOBBLIN-794] fix JsonIntermedidateToAvroConverter for nested array/object use cases
* [Bug] [GOBBLIN-791] Fix hanging stream on error in asynchronous execution model
* [Bug] [GOBBLIN-785] remove wrapper isPartition function, use table.isPartitioned instead
* [Bug] [GOBBLIN-783] Fix the double referencing issue for job type config.
* [Bug] [GOBBLIN-780] Handle scenarios that cause the YarnAutoScalingManager to be stuck
* [Bug] [GOBBLIN-777] Remove container request after container allocation
* [Bug] [GOBBLIN-765] Remove a duplicate leading period character from the config key for SqlDataNode
* [Bug] [GOBBLIN-761] Only instantiate topic-specific configStore object when topic.name is available
* [Bug] [GOBBLIN-754] Clean old version of multi-hop compiler
* [Bug] [GOBBLIN-752] Fix a bug in QPS throttling policy where it was incorrectly indicating permits were impossible to satisfy.
* [Bug] [GOBBLIN-747] Check schema
* [Bug] [GOBBLIN-740] Remove setting retentionPolicy on every Point write
* [Bug] [GOBBLIN-736] Skip flush and control message handlers on closed writers in the CloseOnFlushWriterWrapper
* [Bug] [GOBBLIN-734] Fix speculative safety checking in HiveWritable writer
* [Bug] [GOBBLIN-731] Make deserialization of FlowSpec more robust
* [Bug] [GOBBLIN-727] Skip commit in CloseOnFlushWriterWrapper if a commit has already been invoked on the underlying writer.
* [Bug] [GOBBLIN-726] enable schema check for ticket ETL-8753
* [Bug] [GOBBLIN-721] Gobblin streaming recipe is broken
* [Bug] [GOBBLIN-719] fix invalid git links for classes in docs
* [Bug] [GOBBLIN-717] Filter Out Empty MultiWorkUnits
* [Bug] [GOBBLIN-702] Compaction fix for reuse of OrcStruct
* [Bug] [GOBBLIN-690] Fix the planning job relaunch name match.
* [Bug] [GOBBLIN-689] catch unchecked exceptions in KafkaSource
* [Bug] [GOBBLIN-684] Ensure buffered messages are flushed before close() in KafkaProducerPusher
* [Bug] [GOBBLIN-680] Enhance error handling on task creation
* [Bug] [GOBBLIN-674] Skip initialization of GitMonitoringService when gobblin template dirs is empty.
* [Bug] [GOBBLIN-671] Close the underlying writer when a HiveWritableHdfsDataWriter is closed
* [Bug] [GOBBLIN-670] Ensure MultiHopFlowCompiler is initialized when job template catalog location is not provided.
* [Bug] [GOBBLIN-667] Pass encrypt.key.loc configuration to GitFlowGraphMonitor.
* [Bug] [GOBBLIN-666] Data too long for column 'property_key'
* [Bug] [GOBBLIN-665] Throw an exception if job orchestration fails on a SpecExecutor.
* [Bug] [GOBBLIN-663] Fix Typesafe config resolution failure for non-HOCON strings
* [Bug] [GOBBLIN-661] Prevent jobs resubmission after manager failure
* [Bug] [GOBBLIN-660] Fix OracleExtractor datatype mapping
* [Bug] [GOBBLIN-659] Ensure MultiHopFlowCompiler is properly initialized before attempting flow orchestration.
* [Bug] [GOBBLIN-654] Fix the argument order of JobStatusRetriever APIs to reflect actual usage.
* [Bug] [GOBBLIN-645] Fix some typos as reading thru code
* [Bug] [GOBBLIN-643] Fix NPE when closing KafkaExtractor
* [Bug] [GOBBLIN-593] fix NPE in task cancel
* [Bug] [GOBBLIN-571] Fix parquet schema for complex types

GOBBLIN 0.14.0
-------------

###Created Date: 27/11/2018

## HIGHLIGHTS 

* Multi-hop support in Gobblin-as-a-Service with in built workflow manager. 
* Gobblin-as-a-Service integration with Azkaban. 
* New Elasticsearch writer. 
* Block level distcp-ng copy support. 

## NEW FEATURES 

* [GaaS] [GOBBLIN-590] Workflow Manager in Gobblin-as-a-Service (GaaS)
* [GaaS] [GOBBLIN-572] Azkaban support in GaaS
* [GaaS] [GOBBLIN-552] Add multicast option to the MultiHopFlowCompiler
* [GaaS] [GOBBLIN-528] Multihop Flow Compiler for Gobblin-as-a-Service (GaaS)
* [Writer] [GOBBLIN-17] Elasticsearch Writer
* [Distcp-NG] [GOBBLIN-598]	Block level distcp enhancement 

## IMPROVEMENTS

* [GaaS] [GOBBLIN-638] Submit more timing events from GaaS to accurately track flow/job status
* [GaaS] [GOBBLIN-637] Create dag checkpoint dir on initialization to avoid NPE
* [GaaS] [GOBBLIN-636] Use FS scheme and relative URIs for specifying job template locations in GaaS
* [GaaS] [GOBBLIN-635] Add metadata tags to Gobblin Tracking Event for Azkaban jobs triggered using Gobblin-as-a-Service (GaaS)
* [GaaS] [GOBBLIN-634] Gobblin as a Service needs to know who sends the request
* [GaaS] [GOBBLIN-633] Provide HOCON support for flow requests to GaaS 
* [GaaS] [GOBBLIN-632] Add a template for an incremental avro ingestion job
* [GaaS] [GOBBLIN-624] Handle dataset retention in multi-hop flow compiler 
* [GaaS] [GOBBLIN-616] Add ability to fork jobs when concatenating Dags
* [GaaS] [GOBBLIN-614] Allow multiple flow failure options in DagManager
* [GaaS] [GOBBLIN-612] Disable commit hash checkpointing for GitFlowGraphMonitor 
* [GaaS] [GOBBLIN-611] Ensure node events are processed before edge events in GitFlowGraphMonitor
* [GaaS] [GOBBLIN-610] Add support for secure access to Git in GitMonitoringService
* [GaaS] [GOBBLIN-604] Map dependencies in job templates to the job names in compiled JobSpecs
* [GaaS] [GOBBLIN-603] Add ServiceManager to manage GitFlowGraphMonitor in multihop flow compiler
* [GaaS] [GOBBLIN-602] Allow AzkabanProducer to be customized
* [GaaS] [GOBBLIN-601] Add cancellation to AzkabanClient
* [GaaS] [GOBBLIN-591] Allow user to pass in the customized http client for azkaban client
* [GaaS] [GOBBLIN-559] Implement FlowGraph as a concurrent data structure
* [GaaS] [GOBBLIN-558] Add Gobblin Tracking Events in GaaS
* [GaaS] [GOBBLIN-518] Ability to cancel a running job in gobblin service
* [Cluster] [GOBBLIN-625] Distributed job launcher doesn't have helix tagging support
* [Cluster] [GOBBLIN-617] Add distributed job launcher metrics and some refactoring
* [Cluster] [GOBBLIN-584] Fix the helix key configuration naming
* [Core] [GOBBLIN-639] Change serder method to static from RequesterService
* [Core] [GOBBLIN-621] Add general utilities
* [Core] [GOBBLIN-607] Reduce logging in gobblin-runtime tests
* [Core] [GOBBLIN-564] Implement partition descriptor
* [Core] [GOBBLIN-563] Upgrade Gradle to 4.9
* [Core] [GOBBLIN-562] Add SharedHiveConfKey to HiveConf Factory
* [Core] [GOBBLIN-561] Handle data completeness checks for data partitions with no records
* [State Store] [GOBBLIN-622] Avoid to serialize all previous workunits in SourceState to save both memory and diskspace
* [Source] [GOBBLIN-631] Add option to use timezone for TimeAwareDatasetFinder
* [Source] [GOBBLIN-615] Make LWM==HWM a valid interval in QueryBaseSource
* [Source] [GOBBLIN-573] Add option to use finer level granularity at the hour level for TimeAwareDatasetfinder
* [Source] [GOBBLIN-547] Extend DatePartitionedNestedRetriever to support recursive loading of daily partition folder
* [Extractor] [GOBBLIN-585] Return expectedRecordCount of 1 for FileAwareInputStreamExtractor
* [Writer] [GOBBLIN-627] Allow files in task output directory to be overwritten when renaming files with record count
* [Writer] [GOBBLIN-608] Allow user to configure the fork operation timeout
* [Writer] [GOBBLIN-582] Allow file overwrite when copying from task staging to task output for FileAwareInputStreamDataWriter
* [Writer] [GOBBLIN-549] Add configurability for Couchbase writer
* [Retention] [GOBBLIN-586] Feature to apply retention in remote HDFS
* [Kafka] [GOBBLIN-640] Add a Kafka producer pusher that supports keyed messages
* [Kafka] [GOBBLIN-629] Skip updating statistics for Kafka partitions in KafkaExtractor when topic is skipped 
* [Hive Registration] [GOBBLIN-557] Reuse HiveConf among Hive registration by resource broker
* [Distcp] [GOBBLIN-576] Send partition level lineage in hive distcp
* [Config Store] [GOBBLIN-609] Change config store to append to path part of URI
* [Config Store] [GOBBLIN-567] Create config store that downloads and reads from a local jar
* [Gobblin Metrics] [GOBBLIN-592]	Disable FileFailureEventReport by configuration
* [Gobblin Metrics] [GOBBLIN-589] Add more Gobblin tracking metrics in KafkaExtractorTopicMetadata event

## BUGS FIXES

* [Bug] [GOBBLIN-626]	Fix the wrong planning job tag key
* [Bug] [GOBBLIN-620]	NPE when catalog metrics is not enabled
* [Bug] [GOBBLIN-619]	Fix the metrics name for GobblinHelixJobScheduler
* [Bug] [GOBBLIN-618]	Remove unnecessary methods from StandardMetricsBridge
* [Bug] [GOBBLIN-606]	Fix duplicate addition of FlowEdge to path for single-hop paths in Multi-hop flow compiler
* [Bug] [GOBBLIN-605]	Avoid setting the flowExecutionId twice during path finding in Multi-hop flow compilation
* [Bug] [GOBBLIN-600]	Fix build issue due to duplicate method signatures in FlowStatusTest
* [Bug] [GOBBLIN-599]	Handle task creation errors in GobblinMultiTaskAttempt
* [Bug] [GOBBLIN-597]	Skip submitting a GaaS job if it is already running
* [Bug] [GOBBLIN-588]	Remove final decorator to allow password be overwritten
* [Bug] [GOBBLIN-580]	Infinite loop in Google Search Console(webmaster) connector
* [Bug] [GOBBLIN-578]	Comparison to None should be 'expr is None'
* [Bug] [GOBBLIN-577]	pep-0020 - Readability counts
* [Bug] [GOBBLIN-575]	Remove scala dependencies
* [Bug] [GOBBLIN-574]	elasticsearch-dep module failed to build
* [Bug] [GOBBLIN-569]	Address the pylint warnings in github-pr-change-log.py
* [Bug] [GOBBLIN-566] HiveMetastoreBasedRegister incorrectly issues an alter_partition when it should do an add_partition
* [Bug] [GOBBLIN-556]	Gobblin AvroUtils reads and writes UTF rather than chars
* [Bug] [GOBBLIN-554]	Change signature of SpecCompiler#compileFlow() to return a DAG of JobSpecs instead of a HashMap
* [Bug] [GOBBLIN-553]	Fix FileAwareInputStreamDataWriterTest access to public key
* [Bug] [GOBBLIN-551]	JdbcExtractor code in gobblin-sql module using separate connections for `extractMetadata`, `getMaxWatermark` & `getSourceCount`
* [Bug] [GOBBLIN-550]	When RuntimeException occurred, alwaysDelete flag doesn't work
* [Bug] [GOBBLIN-548]	Fix bug in GobblinHelixJobLauncherTest
* [Bug] [GOBBLIN-546]	Use appropriate Lists package
* [Bug] [GOBBLIN-538]	Return flow execution id on submitting new flow

GOBBLIN 0.13.0
-------------

###Created Date: 27/07/2018

## HIGHLIGHTS 

* Git based FlowGraph monitor in GaaS.
* GPG encryption support.
* Base work on multi-hop work in Gobblin-as-a-Service. 
* More versatile and high available Gobblin Cluster. 
* Migration to new Helix version and its improved task framework.
* Database based state-store support along with migration support.

## NEW FEATURES 

* [GaaS] [GOBBLIN-505] Implement a Git-based FlowGraph Monitor
* [Encryption] [GOBBLIN-521] Add support for encryption in the GPGCodec

## IMPROVEMENTS

* [GaaS] [GOBBLIN-535] Add second hop for distributed job launcher
* [GaaS] [GOBBLIN-532] Always delete jobSpec no matter if the job is successful or not
* [GaaS] [GOBBLIN-516] Propagate Accurate Error Message in Construction of CopyRoute
* [GaaS] [GOBBLIN-495] FlowSpec should be deleted if this is run once flow
* [GaaS] [GOBBLIN-491] Create a FlowGraph representation for multi-hop support in Gobblin-as-a-Service
* [GaaS] [GOBBLIN-490] Add planning job execution launcher 
* [GaaS] [GOBBLIN-458] Refactor flowConfig resource handler to avoid single restli request handled partially on one machine and then forward to another machine. 
* [GaaS] [GOBBLIN-453] Make the rest port configurable via property file.
* [Cluster] [GOBBLIN-539] Set expiry time on helix work flow
* [Cluster] [GOBBLIN-534] Use Helix WorkFlow instead of JobQueue
* [Cluster] [GOBBLIN-533] Upgrade Helix to 0.8.1
* [Cluster] [GOBBLIN-510] Decouple JobExecutionLauncher and JobExecutionDriver
* [Cluster] [GOBBLIN-508] Ensure that in AWSConfigManager the files are extracted within the output directory
* [Cluster] [GOBBLIN-506] Job tagging support in Gobblin cluster
* [Cluster] [GOBBLIN-480] Allow job distribution cluster to be separated from cluster manager cluster
* [Cluster] [GOBBLIN-476] Add helix task timeout
* [Cluster] [GOBBLIN-455] Yarn launcher does not honor jvmflags and jars arguments
* [Cluster] [GOBBLIN-452] Logging related Improvement in Gobblin Cluster
* [Core] [GOBBLIN-537] Dump workunits to logs for debugging
* [Core] [GOBBLIN-499] Log the job name with the tracking URL for easier debugging
* [Core] [GOBBLIN-489] Implement PusherFactory
* [Core] [GOBBLIN-484] Propagate fork exception to task commit
* [Core] [GOBBLIN-470] Improve MRTask error log to contain Job Url
* [Core] [GOBBLIN-460] Gobblin will skip all future tasks if the first n tasks complete before the n+1th is scheduled
* [Core] [GOBBLIN-447] Always mark custom tasks as complete even if they throw exception in run()
* [State Store] [GOBBLIN-456] Add option to delete job state store
* [State Store] [GOBBLIN-454] Add retention support to the MysqlDatasetStateStore
* [State Store] [GOBBLIN-446] Add support for migrating state for all jobs in a job store
* [State Store] [GOBBLIN-432] Share the DataSource used by the MySQL state stores
* [Source] [GOBBLIN-520] Allow user customize their own fileSetWorkUnitGenerator
* [Source] [GOBBLIN-492] Make LoopingDatasetFinderSource easy to embed different Iterator
* [Source] [GOBBLIN-473] Allow user to configure different lookback time for different datasets
* [Source] [GOBBLIN-471] DatasetFinderSource should allow skipping datasets
* [Source] [GOBBLIN-464] Enhance LoopingDatasetFinderSource to support global watermark and per-dataset watermark
* [Source] [GOBBLIN-448] Add glob pattern blacklist in ConfigurableGlobDatasetFinder
* [Source] [GOBBLIN-440] SQLServer source uses "source.querybased.schema" as database name
* [Extractor] [GOBBLIN-536] Allow user to configure connection string properties in mysql extractor
* [Extractor] [GOBBLIN-483] Allow join operations if metadata check is disabled
* [Extractor] [GOBBLIN-479] Extract records as String instead of jsonObject 
* [Converter] [GOBBLIN-98] HiveSerDeConverter. Write to ORC records duplication with queue.capacity=1
* [Writer] [GOBBLIN-509] Ensure that tar data writer writes within output directory
* [Writer] [GOBBLIN-494] Allow retrywriter to be disabled
* [Writer] [GOBBLIN-488] Make `AsyncRequest` aware of records
* [Retention] [GOBBLIN-469] Add Task for running the DatasetCleaner
* [Hive-Registration] [GOBBLIN-502] Make HiveMetastoreClient PoolCache's TTL configurable
* [Kafka] [GOBBLIN-507] Change URL format in KafkaAuditHttpClient to query Kafka audit server.
* [Kafka] [GOBBLIN-481] Missing Alias Annotation on Class KafkaSimpleJsonExtractor
* [Kafka] [GOBBLIN-465] Add support for client certificate auth
* [Kafka] [GOBBLIN-433] Gobblin tries to query schema registry for non existing Kafka partitions
* [Avro-to-ORC] [GOBBLIN-529] Add missing test dependency to gobblin-data-management
* [Avro-to-ORC] [GOBBLIN-496] Support nullable unions in AvroUtils.getFieldSchema
* [Avro-to-ORC] [GOBBLIN-478] Lineage events are not getting emitted during Avro2ORC conversion
* [Avro-to-ORC] [GOBBLIN-463] Change lineage event for Avro2Orc conversion to have underlying FileSystem as platform
* [Salesforce] [GOBBLIN-513] Add support for queryAll when using the Salesforce bulk API
* [Salesforce] [GOBBLIN-486] Change access modifiers for SalesforceWriter to protected to help extend on top of it
* [Salesforce] [GOBBLIN-466] Reuse same connector for Salesforce dynamic partitioning
* [Salesforce] [GOBBLIN-436] Salesforce doesn't have default constructor
* [Salesforce] [GOBBLIN-434] Salesforce connector support refresh token grant
* [Salesforce] [GOBBLIN-430] Add lineage in SalesforceSource
* [Salesforce] [GOBBLIN-423] Limit records or bucket counts for dynamic probing
* [Compaction] [GOBBLIN-445] Add task output directory for staging compaction result
* [Compaction] [GOBBLIN-412] Compression parameters are not propagated to Hadoop
* [Hive Registration] [GOBBLIN-485] AvroSchemaManager does not support using schema generated from Hive columns 
* [Documentation] [GOBBLIN-482] Add http write documentation
* [Documentation] [GOBBLIN-352] Add example for using gobblin-parquet module
* [Apache] [GOBBLIN-517] Add missing apache license info
* [Encryption] [GOBBLIN-459] Support string decryption and arrays of strings
* [Encryption] [GOBBLIN-444] Add support to rotate master keys for encryption/decryption
* [Encryption] [GOBBLIN-293] Remove stream materialization in GPGFileDecryptor

## BUGS FIXES

* [Bug] [GOBBLIN-522] Multiple build issues
* [Bug] [GOBBLIN-514] AvroUtils#parseSchemaFromFile fails when characters are written with Modified UTF-8 encoding
* [Bug] [GOBBLIN-511] Fix Findbugs warnings in Gobblin Service
* [Bug] [GOBBLIN-504] HiveMetastoreClientPool has findbugsMain issue due to unprotected static variable initialization
* [Bug] [GOBBLIN-503] ForkThrowableHolder doesn't aggregate throwable in right condition
* [Bug] [GOBBLIN-501] Fix NPE thrown from read after EOF of LazyMaterializeDecryptorInputStream
* [Bug] [GOBBLIN-497] GobblinHelixJobScheduler should not start scheduling before the scheduler service is up
* [Bug] [GOBBLIN-493] Fix build issue in GithubDataEventTypesPartitioner
* [Bug] [GOBBLIN-468] Enums don't work in json to avro conversion
* [Bug] [GOBBLIN-467] Json to avro conversion broken for records within arrays
* [Bug] [GOBBLIN-461] Disable PasswordManager Tests as they fail often on travis
* [Bug] [GOBBLIN-451] Fix casting error when exception is thrown in conversion
* [Bug] [GOBBLIN-435] Fix data publisher created from job broker not closed

GOBBLIN 0.12.0
-------------

###Created Date: 1/03/2018

## HIGHLIGHTS 

* First Apache Release.
* Improved Gobblin-as-a-Service. 
* Improved Global Throttling. 
* Improved Gobblin Cluster. 
* Enhanced stream processing. 
* New Converters: JsonToParquet, GrokToJson, JsonToAvro.
* New Sources: RegexPartitionedAvroFileSource, new SalesforceWriter.
* New Extractors: PostgresqlExtractor, EnvelopePayloadExtractor.
* New Writers: ParquetHdfsDataWriter, eventually consistent FS support.

## NEW FEATURES 

* [GaaS] [GOBBLIN-232] Create Azkaban Orchestrator for Gobblin-as-a-Service
* [GaaS] [GOBBLIN-213] Add scheduler service to GobblinServiceManager
* [GaaS] [GOBBLIN-3] Implementation of Flow compiler with multiple hops
* [GaaS] [GOBBLIN-204] Add a service that fetches GaaS flow configs from a git repository
* [GaaS] [GOBBLIN-292] Add kafka09 support for service and cluster job spec communication
* [Global Throttling] [GOBBLIN-287] Support service-level throttling quotas
* [Cluster] [GOBBLIN-390] Allow child process to be launched with log4j options
* [Cluster] [GOBBLIN-382] Support storing job.state file in mysql state store for standalone cluster
* [State Store] [GOBBLIN-199] GOBBLIN-56 Add state store entry listing API
* [State Store] [GOBBLIN-200] GOBBLIN-56 State store dataset cleaner using state store listing API
* [Extractor] [GOBBLIN-203] Postgresql Extractor
* [Extractor] [GOBBLIN-238] Implement EnvelopePayloadExtractor and EnvelopePayloadDeserializer
* [Converter] [GOBBLIN-427] Add decryption converters
* [Converter] [GOBBLIN-248] Converter for Json to Parquet
* [Converter] [GOBBLIN-231] Grok to Json Converter
* [Converter] [GOBBLIN-221] Add Json to Avro converter
* [Writer] [GOBBLIN-255] ParquetHdfsDataWriter
* [Writer] [GOBBLIN-36] New salesforce writer
* [Encryption] [GOBBLIN-224] Gobblin doesn't support keyring based GPG file decryption
* [Kafka] [GOBBLIN-190] Kafka Sink replication factor and partition creation.
* [Avro-to-ORC] [GOBBLIN-181] Modify Avro2ORC flow to materialize Hive views

## IMPROVEMENTS

* [GaaS] [GOBBLIN-418] Change Gobblin Service behavior to not call addSpec for preexisting specs on FlowCatalog start up
* [GaaS] [GOBBLIN-415] Check for the value of configuration key flow.runImmediately in Job config.
* [GaaS] [GOBBLIN-406] GaaS Delete job state on spec delete
* [GaaS] [GOBBLIN-404] Disable immediate execution of all flows in FlowCatalog on Gobblin Service restart
* [GaaS] [GOBBLIN-280] Add new SpecCompiler compatible constructor to AzkabanSpecExecutor
* [GaaS] [GOBBLIN-299] Add deletion support to Azkaban Orchestrator
* [GaaS] [GOBBLIN-262] Make multihopcompiler use the first user specified template
* [GaaS] [GOBBLIN-281] Fix logging in gobblin-service
* [GaaS] [GOBBLIN-273] Add failure monitoring
* [GaaS] [GOBBLIN-304] Remove versioning from Gobblin-as-a-Service flow specs
* [Global Throttling] [GOBBLIN-424] Gobblin job broker does not get closed if job fails
* [Global Throttling] [GOBBLIN-334] Implement SharedResourceFactory for LineageInfo
* [Global Throttling] [GOBBLIN-264] Add a SharedResourceFactory for creating shared DataPublishers
* [Global Throttling] [GOBBLIN-251] Having UpdateProviderFactory able to instantiate FileSystem with URI
* [Global Throtlting] [GOBBLIN-236] Add a ControlMessage injector as a RecordStreamProcessor
* [Global Throttling] [GOBBLIN-24] Allow disabling global throttling. Fix a race condition in BatchedPer…
* [Cluster] [GOBBLIN-429] Pass jvm options to child process for task isolation
* [Cluster] [GOBBLIN-428] Fix delete spec in cluster
* [Cluster] [GOBBLIN-419] Add more metrics for cluster job scheduling
* [Cluster] [GOBBLIN-416] Allow user to configure java options to launch child process for cluster task isolation
* [Cluster] [GOBBLIN-402] Add more metrics for gobblin cluster and fix the getJobs slowness issue
* [Cluster] [GOBBLIN-398] Upgrade helix to 0.6.9
* [Cluster] [GOBBLIN-388] Allow classpath to be configured for JVM based task execution in gobblin cluster
* [Cluster] [GOBBLIN-381] Add ability to filter hidden directories for ConfigBasedDatasets
* [Cluster] [GOBBLIN-377] Add debug logging to print out job configuration in gobblin cluster
* [Cluster] [GOBBLIN-372] Workaround helix workflow deletion bug that removes workflows with a matching prefix
* [Cluster] [GOBBLIN-369] Clean up the helix job queue after the job execution is complete
* [Cluster] [GOBBLIN-302] Handle stuck Helix workflow
* [Cluster] [GOBBLIN-207] Job package made publicly accessible for Gobblin AWS
* [Cluster] [GOBBLIN-329] Add a basic cluster integration test
* [Cluster] [GOBBLIN-325] Add a Source and Extractor for stress testing
* [Cluster] [GOBBLIN-324] Add a configuration to configure the cluster working directory
* [Cluster] [GOBBLIN-257] Remove old jobs' run data
* [Cluster] [GOBBLIN-202] Add better metrics to gobblin to support AWS autoscaling
* [Cluster] [GOBBLIN-320] Add metrics to GobblinHelixJobScheduler
* [Cluster] [GOBBLIN-185] Design for gobblin job level gracefully shutdown
* [Cluster] [GOBBLIN-11] Fix for #1822 and #1823
* [Cluster] [GOBBLIN-10] Fix_for_#1850_and_#1851
* [Cluster] [GOBBLIN-349] Add guages for gobblin cluster metrics
* [Core] [GOBBLIN-426] Change signature of AzkabanJobLauncher.initJobListener from private to protected
* [Core] [GOBBLIN-177] Allow error limit to skip records which are not convertible
* [Core] [GOBBLIN-333] Remove reference to log4j in WriterUtils
* [Core] [GOBBLIN-332] Implement fetching hive tokens in tokenUtils
* [Core] [GOBBLIN-330] Generate Kerberos Principal dynamically
* [Core] [GOBBLIN-319] Add DatasetResolver to transform raw Gobblin dataset to application specific dataset
* [Core] [GOBBLIN-317] Add dynamic configuration injection in the mappers
* [Core] [GOBBLIN-310] Skip rerunning completed tasks on mapper reattempts
* [Core] [GOBBLIN-300] Use 1.7.7 form of Schema.createUnion() API that takes in a list
* [Core] [GOBBLIN-294] Change logging level of refection utilities
* [Core] [GOBBLIN-271] Move the grok converter to the gobblin-grok module
* [Core] [GOBBLIN-252] Add some azkaban related constants
* [Core] [GOBBLIN-240] Adding three more Azkaban tags
* [Core] [GOBBLIN-186] Add support for using the Kerberos authentication plugin without a GobblinDriverInstance
* [Core] [GOBBLIN-179] Make migrated Gobblin code work with old state files
* [Core] [GOBBLIN-178] Migrate Gobblin codebase from gobblin to org.apache.gobblin package
* [State Store] [GOBBLIN-409] Set collation to latin1_bin for the MySql state store backing table
* [State Store] [GOBBLIN-335] Increase blob size in MySQL state store
* [State Store] [GOBBLIN-270] State Migration script
* [State Store] [GOBBLIN-230] Convert old package name to new name in old states
* [Source] [GOBBLIN-422] FileBasedSource needs fs snapshot update of previously failed workunits with latest snapshot
* [Source] [GOBBLIN-421] Add parameterized type for Pusher message type
* [Source] [GOBBLIN-408] Add more info to the KafkaExtractorTopicMetadata event for tracking execution times and rates
* [Source] [GOBBLIN-399] Refactor HiveSource#shouldCreateWorkunit() to accept table as parameter
* [Source] [GOBBLIN-396] Date partition based json to avro source
* [Source] [GOBBLIN-395] Add lineage for copying config based dataset
* [Source] [GOBBLIN-365] Add lookback days config property for CopyableGlobDatasetFinder
* [Source] [GOBBLIN-296] Kafka json source and writer
* [Source] [GOBBLIN-245] Create topic specific extract of a WorkUnit in KafkaSource
* [Source] [GOBBLIN-210] Implement a source based on Dataset Finder
* [Extractor] [GOBBLIN-197] Modify JDBCExtractor to support reading clob columns as strings
* [Converter] [GOBBLIN-417] AvroR2JoinConverter passes in the contenttype for Rest.li protocol version
* [Converter] [GOBBLIN-228] Add config property to ignore fields in JsonRecordAvroSchemaToAvroConverter
* [Converter] [GOBBLIN-226] Nested schema support in JsonStringToJsonIntermediateConverter and JsonIntermediateToAvroConverter
* [Writer] [GOBBLIN-362] Improve DDL on staging table creation for MySQL to also have properties from destination table
* [Writer] [GOBBLIN-361] Support Nested nullable Record type for JDBCWriter
* [Writer] [GOBBLIN-314] Validate filesize when copying in writer
* [Writer] [GOBBLIN-171] Add a writer wrapper that closes the wrapped writer and creates a new one
* [Writer] [GOBBLIN-6] Support eventual consistent filesystems like S3
* [Compaction] [GOBBLIN-354] Support DynamicConfig in AzkabanCompactionJobLauncher
* [Retention] [GOBBLIN-348] Hdfs Modified Time based Version Finder for Hive Tables
* [Hive-Registration] [GOBBLIN-342] Option to set hive metastore uri in Hiveregister
* [Kafka] [GOBBLIN-331] Add sharedConfig support for the KafkaDataWriters
* [Kafka] [GOBBLIN-312] Pass extra kafka configuration to the KafkaConsumer in KafkaSimpleStreamingSource
* [Kafka] [GOBBLIN-198] Configuration to disable switching the Kafka topic's and Avro schema's names before registering schema
* [Kafka] [GOBBLIN-195] Ability to switch Avro schema namespace switch before registering with Kafka Avro Schema registry
* [Avro-to-ORC] [GOBBLIN-313] Option to explicitly set group name for staging and final destination directories for Avro-To-Orc conversion
* [Avro-to-ORC] [GOBBLIN-297] Changing access modifier to Protected for HiveSource and Watermarker classes
* [Metrics] [GOBBLIN-326] Gobblin metrics constructor only provides default constructor for Codhale metrics
* [Metrics] [GOBBLIN-189] Add additional information in events for gobblintrackingevent_distcp_ng to show published dataset path
* [Metrics] [GOBBLIN-307] Implement lineage event as LineageEventBuilder in gobblin
* [Metrics] [GOBBLIN-261] Add kafka lineage event
* [Metrics] [GOBBLIN-182] Emit Lineage Events for Query Based Sources
* [Metrics] [GOBBLIN-22] Graphite prefix in configuration
* [Metrics] [GOBBLIN-358] Add logs for GobblinMetrics
* [Salesforce] [GOBBLIN-288] Add finer-grain dynamic partition generation for Salesforce
* [Salesforce] [GOBBLIN-265] Add support for PK chunking to gobblin-salesforce
* [Compaction] [GOBBLIN-413] compaction should use the same time range check
* [Compaction] [GOBBLIN-256] Improve logging for gobblin compaction
* [Hive Registration] [GOBBLIN-266] Improve Hive Task setup
* [Hive Registration] [GOBBLIN-253] Hive materializer enhancements
* [Hive Registration] [GOBBLIN-172] Pipelined Hive Registration thru. TastStateCollectorService
* [Config] [GOBBLIN-209] Add support for HOCO global files
* [DisctpNG] [GOBBLIN-410] Support REPLACE_TABLE_AND_PARTITIONS for Hive copies
* [DisctpNG] [GOBBLIN-379] Submit an event when DistCp job resource requirements exceed a hard bound.
* [DistcpNG] [GOBBLIN-173] Add pattern support for job-level blacklist in distcpNG/replication
* [DistcpNG] [GOBBLIN-8] Add simple distcp job publishing to S3 as an example
* [DistcpNG] [GOBBLIN-5] Make Watermark checking configurable in distcpNG-replication
* [Documentation] [GOBBLIN-351] Add docs for ParquetHdfsDataWriter
* [Documentation] [GOBBLIN-249] Documenting source schema specification
* [Documentation] [GOBBLIN-282] Support templates on Gobblin Azkaban launcher
* [Documentation] [GOBBLIN-170] Updating documentation to include Apache with Gobblin
* [Documentation] [GOBBLIN-25] Gobblin data-management run script and example configuration
* [Documentation] [GOBBLIN-339] Example to illustrate how to build custom source and extractor in Gobblin.
* [Documentation] [GOBBLIN-305] Add csv-kafka and kafka-hdfs template
* [Apache] [GOBBLIN-384] Update Python version in gobblin-pr
* [Apache] [GOBBLIN-371] In gobblin_pr, Jira resolution fails if python jira package is not installed
* [Apache] [GOBBLIN-169] Ability to curate licenses of all Gobblin dependencies
* [Apache] [GOBBLIN-168] Standardize Github PR template for Gobblin
* [Apache] [GOBBLIN-167] Add dev tooling for signing releases
* [Apache] [GOBBLIN-166] Add dev tooling for simplifying the Github PR workflow
* [Apache] [GOBBLIN-163] Setup Wiki for Gobblin
* [Apache] [GOBBLIN-162] Setup new PR process for Gobblin
* [Apache] [GOBBLIN-161] Migrate all Gobblin issues from Github to Apache
* [Apache] [GOBBLIN-160] Move mailing lists to Apache
* [Apache] [GOBBLIN-65] Add com.linkedin.gobblin to alias resolver
* [Apache] [GOBBLIN-38] Create workunitstream for CompactionSource
* [Apache] [GOBBLIN-2] Setup Apache Gobblin's website
* [Apache] [GOBBLIN-1] Move Gobblin codebase to Apache
* [AdminUI] [GOBBLIN-9] Improve AdminUI and RestService with better sorting, filtering, auto-updates, etc.
* [Streaming] [GOBBLIN-4] Added control messages to Gobblin stream.

## BUGS FIXES

* [Bug] [GOBBLIN-414] Add lineage event for convertible hive datasets
* [Bug] [GOBBLIN-411] Fix bug in FIFO based pull file loader
* [Bug] [GOBBLIN-407] Job output is being written to _append directories for full snapshots
* [Bug] [GOBBLIN-405] Fix race condition with access to immediately invalidated resources
* [Bug] [GOBBLIN-403] Fix the NPE issue due to uninitialized kafkajobmonitor metrics
* [Bug] [GOBBLIN-401] Provide a constructor for CombineSelectionPolicy with only the selection config as argument
* [Bug] [GOBBLIN-397] Create a new dataset version selection policy for filtering dataset versions that have "hidden" paths
* [Bug] [GOBBLIN-392] Load all dataset states when getLatestDatasetStatesByUrns() is called
* [Bug] [GOBBLIN-391] Use the DataPublisherFactory to allow sharing publishers in SafeDatasetCommit
* [Bug] [GOBBLIN-378] Ensure task only publish data when the state is successful in the earlier processing
* [Bug] [GOBBLIN-364] Exclude JobState from WorkUnit created by PartitionedFileSourceBase
* [Bug] [GOBBLIN-363] Clean up the joblevel subdir in the _taskstate directory in Gobblin Cluster after a job is done
* [Bug] [GOBBLIN-360] Helix not pruning old Zookeeper data
* [Bug] [GOBBLIN-359] Logged Job/Task info from TaskExecutor threads sometimes does not match the task running
* [Bug] [GOBBLIN-357] Poor logging when zookeeper connection is lost
* [Bug] [GOBBLIN-356] hanging when retrieving kafka schema
* [Bug] [GOBBLIN-353] Fix low watermark overridden by high watermark in SalesforceSource
* [Bug] [GOBBLIN-347] KafkaPusher is not closed when GobblinMetrics.stopReporting is called
* [Bug] [GOBBLIN-344] Fix help method getResolver in LineageInfo is private
* [Bug] [GOBBLIN-343] Table and db regexp does not work in HiverRegistrationPolicyBase
* [Bug] [GOBBLIN-341] Fix logger name to correct class prefix after apache package change
* [Bug] [GOBBLIN-338] HiveAvroManagerSerde failed if external table was on different fs
* [Bug] [GOBBLIN-337] HiveConf token signature bug
* [Bug] [GOBBLIN-328] GobblinClusterKillTest failed. Not able to find expected output files.
* [Bug] [GOBBLIN-322] Cluster mode failed to start. Failed to find a log4j config file
* [Bug] [GOBBLIN-321] CSV to HDFS ISSUE
* [Bug] [GOBBLIN-315] Fix shaded avro is used in LineageEventBuilder
* [Bug] [GOBBLIN-309] Bug fixing for contention of adding jar file into HDFS
* [Bug] [GOBBLIN-308] Gobblin cluster bootup hangs
* [Bug] [GOBBLIN-306] Exception when using fork followed by converters with EmbeddedGoblin
* [Bug] [GOBBLIN-303] Compaction can generate zero sized output when MR is in speculative mode
* [Bug] [GOBBLIN-301] Fix the key GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS
* [Bug] [GOBBLIN-295] Make missing nullable fields default to null in json to avro converter
* [Bug] [GOBBLIN-291] Remove unnecessary listing and reading of flowSpecs
* [Bug] [GOBBLIN-289] Gobblin only partially decrypt the PGP file using keyring
* [Bug] [GOBBLIN-286] Fix bug where non hive dataset throw NPE during dataset publish
* [Bug] [GOBBLIN-285] KafkaExtractor does not compute avgMillisPerRecord when partition pull is interrupted
* [Bug] [GOBBLIN-284] Add retry in SalesforceExtractor to handle transient network errors
* [Bug] [GOBBLIN-283] Refactor EnvelopePayloadConverter to support multi fields conversion
* [Bug] [GOBBLIN-279] pull file unable to reuse the json property.
* [Bug] [GOBBLIN-278] Fix sending lineage event for KafkaSource
* [Bug] [GOBBLIN-276] Change setActive order to prevent flow spec loss
* [Bug] [GOBBLIN-275] Use listStatus instead of globStatus for finding persisted files
* [Bug] [GOBBLIN-274] Fix wait for salesforce batch completion
* [Bug] [GOBBLIN-268] Unique job uri and job name generation for GaaS
* [Bug] [GOBBLIN-267] HiveSource creates workunit even when update time is before maxLookBackDays
* [Bug] [GOBBLIN-263] TaskExecutor metrics are calculated incorrectly
* [Bug] [GOBBLIN-260] Salesforce dynamic partitioning bugs
* [Bug] [GOBBLIN-259] Support writing Kafka messages to db/table file path
* [Bug] [GOBBLIN-258] Try to remove the tmp output path from wrong fs before compaction
* [Bug] [GOBBLIN-254] Add config key to update watermark when a partition is empty
* [Bug] [GOBBLIN-247] avro-to-orc conversion validation job should fail only on data mismatch
* [Bug] [GOBBLIN-244] Need additional info for gobblin tracking hourly-deduped
* [Bug] [GOBBLIN-241] Allow multiple datasets send different lineage event for kafka
* [Bug] [GOBBLIN-237] Update property names in JsonRecordAvroSchemaToAvroConverter
* [Bug] [GOBBLIN-235] Prevent log warnings when TaskStateCollectorService has no task states detected
* [Bug] [GOBBLIN-234] Add a ControlMessageInjector that generates metadata update control messages
* [Bug] [GOBBLIN-233] Add concurrent map to avoid multiple job submission from GobblinHelixJobScheduler
* [Bug] [GOBBLIN-229] Gobblin cluster doesn't clean up job state file upon job completion
* [Bug] [GOBBLIN-225] Fix cloning of ControlMessages in PartitionDataWriterMessageHandler
* [Bug] [GOBBLIN-223] CsvToJsonConverter should throw DataConversionException
* [Bug] [GOBBLIN-222] Fix silent failure in loading incompatible state store
* [Bug] [GOBBLIN-220] FileAwareInputDataStreamWriter only logs file names when a copy completes successfully
* [Bug] [GOBBLIN-219] Check for copyright header
* [Bug] [GOBBLIN-218] Ensure runImmediately is honored in Gobblin as a Service
* [Bug] [GOBBLIN-217] Fix gobblin-admin module to use correct idString
* [Bug] [GOBBLIN-215] hasJoinOperation failed when SQL statement has limit keyword
* [Bug] [GOBBLIN-214] Filtering doesn't work in FileListUtils:listFilesRecursively
* [Bug] [GOBBLIN-212] Exception handling of TaskStateCollectorServiceHandler
* [Bug] [GOBBLIN-208] JobCatalogs should fallback to system configuration
* [Bug] [GOBBLIN-206] Remove extra close of CloseOnFlushWriterWrapper
* [Bug] [GOBBLIN-205] Fix Replication bug in Push Mode
* [Bug] [GOBBLIN-194] NPE in BaseDataPublisher if writer partitions are enabled and metadata filename is not set
* [Bug] [GOBBLIN-193] AbstractAvroToOrcConverter throws NoObjectException when trying to fetch partition info from table when partition doesn't exist
* [Bug] [GOBBLIN-192] Gobblin AWS hardcodes the log4j config
* [Bug] [GOBBLIN-191] Make sure cron scheduler works and tune schedule period
* [Bug] [GOBBLIN-184] Call the flush method of CloseOnFlushWriterWrapper when a FlushControlMessage is received
* [Bug] [GOBBLIN-183] Gobblin data management copy empty directories
* [Bug] [GOBBLIN-176] Gobblin build is failing with missing dependency jetty-http
* [Bug] [GOBBLIN-175] String is not escaped while creating hive query for avro_to_orc conversion.
* [Bug] [GOBBLIN-174] fix distcp-ng so it does not remove existing target files
* [Bug] [GOBBLIN-165] Fix URI is not absolute issue in SFTP
* [Bug] [GOBBLIN-159] Gobblin Cluster graceful shutdown of master and workers
* [Bug] [GOBBLIN-129] AdminUI performs too many requests when update is pressed
* [Bug] [GOBBLIN-127] Admin UI duration chart is sorted incorrectly
* [Bug] [GOBBLIN-109] Remove need for current.jst
* [Bug] [GOBBLIN-87] Gobblin runOnce not working correctly
* [Bug] [GOBBLIN-79] Add config to specify database for JDBC source
* [Bug] [GOBBLIN-54] How to use oozie to schedule gobblin with mapreduce mode, not the local mode
* [Bug] [GOBBLIN-48] java.lang.IllegalArgumentException when using extract.limit.enabled
* [Bug] [GOBBLIN-40] Job History DB Schema had not been updated to reflect new LauncherType
* [Bug] [GOBBLIN-39] JobHistoryDB migration files have been incorrectly modified
* [Bug] [GOBBLIN-37] Gobblin-Master Build failed
* [Bug] [GOBBLIN-33] StateStores persists Task and WorkUnit state to state.store.fs.uri
* [Bug] [GOBBLIN-32] StateStores created with rootDir that is incompatible with state.store.type
* [Bug] [GOBBLIN-31] Reflections concurrency issue
* [Bug] [GOBBLIN-30] Reflections errors when scanning classpath and encountering missing/invalid file paths.
* [Bug] [GOBBLIN-29] GobblinHelixJobScheduler should be able to be run without default configuration manager
* [Bug] [GOBBLIN-27] SQL Server - incomplete JDBC URL


GOBBLIN 0.11.0
-------------

###Created Date:7/19/2017

## HIGHLIGHTS 

* Introduced Java 8.
* Introduced ReactiveX to enable record level stream processing.
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
* [Compaction] Added late data handling for hourly and daily M/R compaction: https://github.com/apache/gobblin/wiki/Compaction#handling-late-records; added support for triggering M/R compaction if late data exceeds a threshold
* [I/O] Added support for using Hive SerDe's through HiveWritableHdfsDataWriter
* [I/O] Added the concept of data partitioning to writers: https://github.com/apache/gobblin/wiki/Partitioned-Writers
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
