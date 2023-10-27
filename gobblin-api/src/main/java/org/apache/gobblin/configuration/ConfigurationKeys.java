/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.configuration;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;


/**
 * A central place for all Gobblin configuration property keys.
 */
public class ConfigurationKeys {

  /**
   * System configuration properties.
   */
  // Default file system URI for all file storage
  // Overwritable by more specific configuration properties
  public static final String FS_URI_KEY = "fs.uri";

  // Local file system URI
  public static final String LOCAL_FS_URI = "file:///";

  // Comma-separated list of framework jars to include
  public static final String FRAMEWORK_JAR_FILES_KEY = "framework.jars";

  public static final String PST_TIMEZONE_NAME = "America/Los_Angeles";

  /**
   * State store configuration properties.
   */
  // State store type.  References an alias or factory class name
  public static final String STATE_STORE_TYPE_KEY = "state.store.type";
  public static final String DATASET_STATE_STORE_PREFIX = "dataset";
  public static final String DATASET_STATE_STORE_TYPE_KEY = DATASET_STATE_STORE_PREFIX + ".state.store.type";
  public static final String STATE_STORE_FACTORY_CLASS_KEY = "state.store.factory.class";
  public static final String INTERMEDIATE_STATE_STORE_PREFIX = "intermediate";
  public static final String INTERMEDIATE_STATE_STORE_TYPE_KEY = INTERMEDIATE_STATE_STORE_PREFIX + ".state.store.type";
  public static final String DEFAULT_STATE_STORE_TYPE = "fs";
  public static final String STATE_STORE_TYPE_NOOP = "noop";
  // are the job.state files stored using the state store?
  public static final String JOB_STATE_IN_STATE_STORE = "state.store.jobStateInStateStore";
  public static final boolean DEFAULT_JOB_STATE_IN_STATE_STORE = false;

  public static final String CONFIG_RUNTIME_PREFIX = "gobblin.config.runtime.";
  // Root directory where task state files are stored
  public static final String STATE_STORE_ROOT_DIR_KEY = "state.store.dir";
  // File system URI for file-system-based task store
  public static final String STATE_STORE_FS_URI_KEY = "state.store.fs.uri";
  // Thread pool size for listing dataset state store
  public static final String THREADPOOL_SIZE_OF_LISTING_FS_DATASET_STATESTORE =
      "state.store.threadpoolSizeOfListingFsDatasetStateStore";
  public static final int DEFAULT_THREADPOOL_SIZE_OF_LISTING_FS_DATASET_STATESTORE = 10;
  // Enable / disable state store
  public static final String STATE_STORE_ENABLED = "state.store.enabled";
  public static final String STATE_STORE_COMPRESSED_VALUES_KEY = "state.store.compressedValues";
  public static final boolean DEFAULT_STATE_STORE_COMPRESSED_VALUES = true;
  // DB state store configuration
  public static final String STATE_STORE_DB_JDBC_DRIVER_KEY = "state.store.db.jdbc.driver";
  public static final String DEFAULT_STATE_STORE_DB_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
  public static final String STATE_STORE_DB_URL_KEY = "state.store.db.url";
  public static final String STATE_STORE_DB_USER_KEY = "state.store.db.user";
  public static final String STATE_STORE_DB_PASSWORD_KEY = "state.store.db.password";
  public static final String STATE_STORE_DB_TABLE_KEY = "state.store.db.table";
  public static final String DEFAULT_STATE_STORE_DB_TABLE = "gobblin_job_state";
  public static final String MYSQL_GET_MAX_RETRIES = "mysql.get.max.retries";
  public static final int DEFAULT_MYSQL_GET_MAX_RETRIES = 3;

  public static final String DATASETURN_STATESTORE_NAME_PARSER = "state.store.datasetUrnStateStoreNameParser";

  /**
   * Job scheduler configuration properties.
   */
  // Job retriggering
  public static final String JOB_RETRIGGERING_ENABLED = "job.retriggering.enabled";
  public static final String DEFAULT_JOB_RETRIGGERING_ENABLED = "true";
  public static final String LOAD_SPEC_BATCH_SIZE = "load.spec.batch.size";
  public static final int DEFAULT_LOAD_SPEC_BATCH_SIZE = 500;
  public static final String SKIP_SCHEDULING_FLOWS_AFTER_NUM_DAYS = "skip.scheduling.flows.after.num.days";
  public static final int DEFAULT_NUM_DAYS_TO_SKIP_AFTER = 365;
  // Scheduler lease determination store configuration
  public static final String MYSQL_LEASE_ARBITER_PREFIX = "MysqlMultiActiveLeaseArbiter";
  public static final String MULTI_ACTIVE_SCHEDULER_CONSTANTS_DB_TABLE_KEY = MYSQL_LEASE_ARBITER_PREFIX + ".constantsTable";
  public static final String DEFAULT_MULTI_ACTIVE_SCHEDULER_CONSTANTS_DB_TABLE = "gobblin_multi_active_scheduler_constants_store";
  public static final String SCHEDULER_LEASE_DETERMINATION_STORE_DB_TABLE_KEY = MYSQL_LEASE_ARBITER_PREFIX + ".schedulerLeaseArbiter.store.db.table";
  public static final String DEFAULT_SCHEDULER_LEASE_DETERMINATION_STORE_DB_TABLE = "gobblin_scheduler_lease_determination_store";
  public static final String SCHEDULER_LEASE_DETERMINATION_TABLE_RETENTION_PERIOD_MILLIS_KEY = MYSQL_LEASE_ARBITER_PREFIX + ".retentionPeriodMillis";
  public static final long DEFAULT_SCHEDULER_LEASE_DETERMINATION_TABLE_RETENTION_PERIOD_MILLIS = 3 * 24 * 60 * 60 * 1000; // (3 days in ms)
  // Refers to the event we originally tried to acquire a lease which achieved `consensus` among participants through
  // the database
  public static final String SCHEDULER_PRESERVED_CONSENSUS_EVENT_TIME_MILLIS_KEY = "preservedConsensusEventTimeMillis";
  // Time the reminder event Trigger is supposed to fire from the scheduler
  public static final String SCHEDULER_EXPECTED_REMINDER_TIME_MILLIS_KEY = "expectedReminderTimeMillis";
  // Event time of flow action to orchestrate using the multi-active lease arbiter
  public static final String ORCHESTRATOR_TRIGGER_EVENT_TIME_MILLIS_KEY = "orchestratorTriggerEventTimeMillis";
  public static final String ORCHESTRATOR_TRIGGER_EVENT_TIME_NEVER_SET_VAL = "-1";
  public static final String FLOW_IS_REMINDER_EVENT_KEY = "isReminderEvent";
  public static final String SCHEDULER_EVENT_EPSILON_MILLIS_KEY = MYSQL_LEASE_ARBITER_PREFIX + ".epsilonMillis";
  public static final int DEFAULT_SCHEDULER_EVENT_EPSILON_MILLIS = 5000;
  // Note: linger should be on the order of seconds even though we measure in millis
  public static final String SCHEDULER_EVENT_LINGER_MILLIS_KEY = MYSQL_LEASE_ARBITER_PREFIX + ".lingerMillis";
  public static final int DEFAULT_SCHEDULER_EVENT_LINGER_MILLIS = 30000;
  public static final String SCHEDULER_MAX_BACKOFF_MILLIS_KEY = MYSQL_LEASE_ARBITER_PREFIX + ".maxBackoffMillis";
  public static final int DEFAULT_SCHEDULER_MAX_BACKOFF_MILLIS = 10000;

  // Job executor thread pool size
  public static final String JOB_EXECUTOR_THREAD_POOL_SIZE_KEY = "jobexecutor.threadpool.size";
  public static final int DEFAULT_JOB_EXECUTOR_THREAD_POOL_SIZE = 5;
  // Job configuration file monitor polling interval in milliseconds
  public static final String JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY = "jobconf.monitor.interval";
  public static final long DEFAULT_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL = 30000;
  public static final long DISABLED_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL = -1L;
  // Directory where all job configuration files are stored WHEN ALL confs reside in local FS.
  public static final String JOB_CONFIG_FILE_DIR_KEY = "jobconf.dir";

  // Path where all job configuration files stored
  public static final String JOB_CONFIG_FILE_GENERAL_PATH_KEY = "jobconf.fullyQualifiedPath";
  // Job configuration file extensions
  public static final String JOB_CONFIG_FILE_EXTENSIONS_KEY = "jobconf.extensions";
  public static final String DEFAULT_JOB_CONFIG_FILE_EXTENSIONS = "pull,job";
  // Whether the scheduler should wait for running jobs to complete during shutdown.
  // Note this only applies to jobs scheduled by the built-in Quartz-based job scheduler.
  public static final String SCHEDULER_WAIT_FOR_JOB_COMPLETION_KEY = "scheduler.wait.for.job.completion";
  public static final String DEFAULT_SCHEDULER_WAIT_FOR_JOB_COMPLETION = Boolean.TRUE.toString();

  public static final String TASK_TIMEOUT_SECONDS = "task.timeout.seconds";
  public static final long DEFAULT_TASK_TIMEOUT_SECONDS = 60 * 60;

  /**
   * Task executor and state tracker configuration properties.
   */
  public static final String TASK_EXECUTOR_THREADPOOL_SIZE_KEY = "taskexecutor.threadpool.size";
  public static final String TASK_STATE_TRACKER_THREAD_POOL_CORE_SIZE_KEY = "tasktracker.threadpool.coresize";
  public static final String TASK_RETRY_THREAD_POOL_CORE_SIZE_KEY = "taskretry.threadpool.coresize";
  public static final int DEFAULT_TASK_EXECUTOR_THREADPOOL_SIZE = 2;
  public static final int DEFAULT_TASK_STATE_TRACKER_THREAD_POOL_CORE_SIZE = 1;
  public static final int DEFAULT_TASK_RETRY_THREAD_POOL_CORE_SIZE = 1;

  /**
   * Common flow configuration properties.
   */
  public static final String FLOW_NAME_KEY = "flow.name";
  public static final String FLOW_GROUP_KEY = "flow.group";
  public static final String FLOW_EDGE_ID_KEY = "flow.edgeId";
  public static final String FLOW_DESCRIPTION_KEY = "flow.description";
  public static final String FLOW_EXECUTION_ID_KEY = "flow.executionId";
  public static final String FLOW_FAILURE_OPTION = "flow.failureOption";
  public static final String FLOW_APPLY_RETENTION = "flow.applyRetention";
  public static final String FLOW_APPLY_INPUT_RETENTION = "flow.applyInputRetention";
  public static final String FLOW_ALLOW_CONCURRENT_EXECUTION = "flow.allowConcurrentExecution";
  public static final String FLOW_EXPLAIN_KEY = "flow.explain";
  public static final String FLOW_UNSCHEDULE_KEY = "flow.unschedule";
  public static final String FLOW_OWNING_GROUP_KEY = "flow.owningGroup";
  public static final String FLOW_SPEC_EXECUTOR = "flow.edge.specExecutors";

  /**
   * Common topology configuration properties.
   */
  public static final String TOPOLOGY_NAME_KEY = "topology.name";
  public static final String TOPOLOGY_GROUP_KEY = "topology.group";
  public static final String TOPOLOGY_DESCRIPTION_KEY = "topology.description";

  /**
   * Common job configuration properties.
   */
  public static final String JOB_NAME_KEY = "job.name";
  public static final String JOB_GROUP_KEY = "job.group";
  public static final String JOB_TAG_KEY = "job.tag";
  public static final String JOB_DESCRIPTION_KEY = "job.description";
  public static final String JOB_CURRENT_ATTEMPTS = "job.currentAttempts";
  public static final String JOB_CURRENT_GENERATION = "job.currentGeneration";
  // Job launcher type
  public static final String JOB_LAUNCHER_TYPE_KEY = "launcher.type";
  public static final String JOB_SCHEDULE_KEY = "job.schedule";
  public static final String JOB_LISTENERS_KEY = "job.listeners";
  // Type of the job lock
  public static final String JOB_LOCK_TYPE = "job.lock.type";
  //Directory that stores task staging data and task output data.
  public static final String TASK_DATA_ROOT_DIR_KEY = "task.data.root.dir";
  public static final String SOURCE_CLASS_KEY = "source.class";
  public static final String CONVERTER_CLASSES_KEY = "converter.classes";
  public static final String RECORD_STREAM_PROCESSOR_CLASSES_KEY = "recordStreamProcessor.classes";
  public static final String FORK_OPERATOR_CLASS_KEY = "fork.operator.class";
  public static final String DEFAULT_FORK_OPERATOR_CLASS = "org.apache.gobblin.fork.IdentityForkOperator";
  public static final String JOB_COMMIT_POLICY_KEY = "job.commit.policy";
  public static final String DEFAULT_JOB_COMMIT_POLICY = "full";
  // If true, commit of different datasets will be performed in parallel
  // only turn on if publisher is thread-safe
  public static final String PARALLELIZE_DATASET_COMMIT = "job.commit.parallelize";
  public static final boolean DEFAULT_PARALLELIZE_DATASET_COMMIT = false;
  /** Only applicable if {@link #PARALLELIZE_DATASET_COMMIT} is true. */
  public static final String DATASET_COMMIT_THREADS = "job.commit.parallelCommits";
  public static final int DEFAULT_DATASET_COMMIT_THREADS = 20;

  public static final String WORK_UNIT_RETRY_POLICY_KEY = "workunit.retry.policy";
  public static final String WORK_UNIT_RETRY_ENABLED_KEY = "workunit.retry.enabled";
  public static final String WORK_UNIT_CREATION_TIME_IN_MILLIS = "workunit.creation.time.in.millis";
  public static final String WORK_UNIT_CREATION_AND_RUN_INTERVAL = "workunit.creation.and.run.interval";
  public static final String WORK_UNIT_ENABLE_TRACKING_LOGS = "workunit.enableTrackingLogs";

  public static final String JOB_DEPENDENCIES = "job.dependencies";
  public static final String JOB_FORK_ON_CONCAT = "job.forkOnConcat";
  public static final String JOB_RUN_ONCE_KEY = "job.runonce";
  public static final String JOB_DISABLED_KEY = "job.disabled";
  public static final String JOB_JAR_FILES_KEY = "job.jars";
  public static final String JOB_LOCAL_FILES_KEY = "job.local.files";
  public static final String JOB_HDFS_FILES_KEY = "job.hdfs.files";
  public static final String JOB_JAR_HDFS_FILES_KEY = "job.hdfs.jars";
  public static final String JOB_LOCK_ENABLED_KEY = "job.lock.enabled";
  public static final String JOB_MAX_FAILURES_KEY = "job.max.failures";
  public static final int DEFAULT_JOB_MAX_FAILURES = 1;
  public static final String MAX_TASK_RETRIES_KEY = "task.maxretries";
  public static final int DEFAULT_MAX_TASK_RETRIES = 5;
  public static final String TASK_RETRY_INTERVAL_IN_SEC_KEY = "task.retry.intervalinsec";
  public static final long DEFAULT_TASK_RETRY_INTERVAL_IN_SEC = 300;
  public static final String OVERWRITE_CONFIGS_IN_STATESTORE = "overwrite.configs.in.statestore";
  public static final boolean DEFAULT_OVERWRITE_CONFIGS_IN_STATESTORE = false;
  public static final String CLEANUP_STAGING_DATA_PER_TASK = "cleanup.staging.data.per.task";
  public static final boolean DEFAULT_CLEANUP_STAGING_DATA_PER_TASK = true;
  public static final String CLEANUP_STAGING_DATA_BY_INITIALIZER = "cleanup.staging.data.by.initializer";
  public static final String CLEANUP_OLD_JOBS_DATA = "cleanup.old.job.data";
  public static final boolean DEFAULT_CLEANUP_OLD_JOBS_DATA = false;
  public static final String MAXIMUM_JAR_COPY_RETRY_TIMES_KEY = JOB_JAR_FILES_KEY + ".uploading.retry.maximum";
  public static final String USER_DEFINED_STATIC_STAGING_DIR = "user.defined.static.staging.dir";
  public static final String USER_DEFINED_STAGING_DIR_FLAG = "user.defined.staging.dir.flag";

  public static final String QUEUED_TASK_TIME_MAX_SIZE = "taskexecutor.queued_task_time.history.max_size";
  public static final int DEFAULT_QUEUED_TASK_TIME_MAX_SIZE = 2048;
  public static final String QUEUED_TASK_TIME_MAX_AGE = "taskexecutor.queued_task_time.history.max_age";
  public static final long DEFAULT_QUEUED_TASK_TIME_MAX_AGE = TimeUnit.HOURS.toMillis(1);

  /**
   * Optional property to specify whether existing data in databases can be overwritten during ingestion jobs
   */
  public static final String ALLOW_JDBC_RECORD_OVERWRITE = "allow.jdbc.record.overwrite";

  /**
   * Optional property to specify a default Authenticator class for a job
   */
  public static final String DEFAULT_AUTHENTICATOR_CLASS = "job.default.authenticator.class";

  /** Optional, for user to specified which template to use, inside .job file */
  public static final String JOB_TEMPLATE_PATH = "job.template";

  /**
   * Configuration property used only for job configuration file's template
   */
  public static final String REQUIRED_ATRRIBUTES_LIST = "gobblin.template.required_attributes";

  /**
   * Configuration for emitting job events
   */
  public static final String EVENT_METADATA_GENERATOR_CLASS_KEY = "event.metadata.generator.class";
  public static final String DEFAULT_EVENT_METADATA_GENERATOR_CLASS_KEY = "noop";

  /**
   * Configuration for dynamic configuration generation
   */
  public static final String DYNAMIC_CONFIG_GENERATOR_CLASS_KEY = "dynamicConfigGenerator.class";
  public static final String DEFAULT_DYNAMIC_CONFIG_GENERATOR_CLASS_KEY = "noop";

  /**
   * Configuration properties used internally.
   */
  public static final String JOB_ID_KEY = "job.id";
  public static final String JOB_KEY_KEY = "job.key";
  public static final String TASK_ID_KEY = "task.id";
  public static final String TASK_KEY_KEY = "task.key";
  public static final String TASK_START_TIME_MILLIS_KEY = "task.startTimeMillis";
  public static final String TASK_ATTEMPT_ID_KEY = "task.AttemptId";
  public static final String JOB_CONFIG_FILE_PATH_KEY = "job.config.path";
  public static final String TASK_FAILURE_EXCEPTION_KEY = "task.failure.exception";
  public static final String TASK_ISSUES_KEY = "task.issues";
  public static final String JOB_FAILURE_EXCEPTION_KEY = "job.failure.exception";
  public static final String TASK_RETRIES_KEY = "task.retries";
  public static final String TASK_IGNORE_CLOSE_FAILURES = "task.ignoreCloseFailures";
  //A boolean config to allow skipping task interrupt on cancellation. Useful for example when thread manages
  // a Kafka consumer which when interrupted during a poll() leaves the consumer in a corrupt state that prevents
  // the consumer being closed subsequently, leading to a potential resource leak.
  public static final String TASK_INTERRUPT_ON_CANCEL = "task.interruptOnCancel";
  public static final String JOB_FAILURES_KEY = "job.failures";
  public static final String JOB_TRACKING_URL_KEY = "job.tracking.url";
  public static final String FORK_STATE_KEY = "fork.state";
  public static final String JOB_STATE_FILE_PATH_KEY = "job.state.file.path";
  public static final String JOB_STATE_DISTRIBUTED_CACHE_NAME = "job.state.distributed.cache.name";

  /**
   * Dataset-related configuration properties;
   */
  // This property is used to specify the URN of a dataset a job or WorkUnit extracts data for
  public static final String DATASET_URN_KEY = "dataset.urn";
  public static final String GLOBAL_WATERMARK_DATASET_URN = "__globalDatasetWatermark";
  public static final String DEFAULT_DATASET_URN = "";

  /**
   * Work unit related configuration properties.
   */
  public static final String WORK_UNIT_LOW_WATER_MARK_KEY = "workunit.low.water.mark";
  public static final String WORK_UNIT_HIGH_WATER_MARK_KEY = "workunit.high.water.mark";
  public static final String WORK_UNIT_SKIP_KEY = "workunit.skip";

  /**
   * Work unit runtime state related configuration properties.
   */
  public static final String WORK_UNIT_WORKING_STATE_KEY = "workunit.working.state";
  public static final String WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK = "workunit.state.runtime.high.water.mark";
  public static final String WORK_UNIT_STATE_ACTUAL_HIGH_WATER_MARK_KEY = "workunit.state.actual.high.water.mark";
  public static final String WORK_UNIT_DATE_PARTITION_KEY = "workunit.source.date.partition";
  public static final String WORK_UNIT_DATE_PARTITION_NAME = "workunit.source.date.partitionName";
  public static final String WORK_UNIT_GENERATOR_FAILURE_IS_FATAL = "workunit.generator.failure.is.fatal";
  public static final boolean DEFAULT_WORK_UNIT_FAST_FAIL_ENABLED = true;

  /**
   * Task execution properties.
   */
  public static final String TASK_SYNCHRONOUS_EXECUTION_MODEL_KEY = "task.execution.synchronousExecutionModel";
  public static final boolean DEFAULT_TASK_SYNCHRONOUS_EXECUTION_MODEL = true;

  /**
   * Watermark interval related configuration properties.
   */
  public static final String WATERMARK_INTERVAL_VALUE_KEY = "watermark.interval.value";

  /**
   * Extract related configuration properties.
   */
  public static final String EXTRACT_TABLE_TYPE_KEY = "extract.table.type";
  public static final String EXTRACT_NAMESPACE_NAME_KEY = "extract.namespace";
  public static final String EXTRACT_TABLE_NAME_KEY = "extract.table.name";
  public static final String EXTRACT_EXTRACT_ID_KEY = "extract.extract.id";
  public static final String EXTRACT_IS_FULL_KEY = "extract.is.full";
  public static final String DEFAULT_EXTRACT_IS_FULL = "false";
  public static final String EXTRACT_FULL_RUN_TIME_KEY = "extract.full.run.time";
  public static final String EXTRACT_PRIMARY_KEY_FIELDS_KEY = "extract.primary.key.fields";
  public static final String EXTRACT_DELTA_FIELDS_KEY = "extract.delta.fields";
  public static final String EXTRACT_SCHEMA = "extract.schema";
  public static final String EXTRACT_LIMIT_ENABLED_KEY = "extract.limit.enabled";
  public static final boolean DEFAULT_EXTRACT_LIMIT_ENABLED = false;
  public static final String EXTRACT_ID_TIME_ZONE = "extract.extractIdTimeZone";
  public static final String DEFAULT_EXTRACT_ID_TIME_ZONE = "UTC";
  public static final String EXTRACT_SALESFORCE_BULK_API_MIN_WAIT_TIME_IN_MILLIS_KEY =
      "extract.salesforce.bulkApi.minWaitTimeInMillis";
  public static final long DEFAULT_EXTRACT_SALESFORCE_BULK_API_MIN_WAIT_TIME_IN_MILLIS = 60 * 1000L; // 1 min
  public static final String EXTRACT_SALESFORCE_BULK_API_MAX_WAIT_TIME_IN_MILLIS_KEY =
      "extract.salesforce.bulkApi.maxWaitTimeInMillis";
  public static final long DEFAULT_EXTRACT_SALESFORCE_BULK_API_MAX_WAIT_TIME_IN_MILLIS = 10 * 60 * 1000L; // 10 min

  /**
   * Converter configuration properties.
   */
  public static final String CONVERTER_AVRO_DATE_FORMAT = "converter.avro.date.format";
  public static final String CONVERTER_AVRO_DATE_TIMEZONE = "converter.avro.date.timezone";
  public static final String CONVERTER_AVRO_TIME_FORMAT = "converter.avro.time.format";
  public static final String CONVERTER_AVRO_TIMESTAMP_FORMAT = "converter.avro.timestamp.format";
  public static final String CONVERTER_AVRO_BINARY_CHARSET = "converter.avro.binary.charset";
  public static final String CONVERTER_AVRO_MAX_CONVERSION_FAILURES = "converter.avro.max.conversion.failures";
  public static final long DEFAULT_CONVERTER_AVRO_MAX_CONVERSION_FAILURES = 0;
  public static final String CONVERTER_CSV_TO_JSON_DELIMITER = "converter.csv.to.json.delimiter";
  public static final String CONVERTER_FILTER_FIELD_NAME = "converter.filter.field";
  public static final String CONVERTER_FILTER_FIELD_VALUE = "converter.filter.value";
  public static final String CONVERTER_IS_EPOCH_TIME_IN_SECONDS = "converter.is.epoch.time.in.seconds";
  public static final String CONVERTER_AVRO_EXTRACTOR_FIELD_PATH = "converter.avro.extractor.field.path";
  public static final String CONVERTER_STRING_FILTER_PATTERN = "converter.string.filter.pattern";
  public static final String CONVERTER_STRING_SPLITTER_DELIMITER = "converter.string.splitter.delimiter";
  public static final String CONVERTER_STRING_SPLITTER_SHOULD_TRIM_RESULTS =
      "converter.string.splitter.shouldITrimResults";
  public static final boolean DEFAULT_CONVERTER_STRING_SPLITTER_SHOULD_TRIM_RESULTS = false;
  public static final String CONVERTER_CSV_TO_JSON_ENCLOSEDCHAR = "converter.csv.to.json.enclosedchar";
  public static final String DEFAULT_CONVERTER_CSV_TO_JSON_ENCLOSEDCHAR = "\0";
  public static final String CONVERTER_AVRO_FIELD_PICK_FIELDS = "converter.avro.fields";
  public static final String CONVERTER_AVRO_JDBC_ENTRY_FIELDS_PAIRS = "converter.avro.jdbc.entry_fields_pairs";
  public static final String CONVERTER_SKIP_FAILED_RECORD = "converter.skipFailedRecord";
  public static final String CONVERTER_AVRO_SCHEMA_KEY = "converter.avroSchema";
  public static final String CONVERTER_IGNORE_FIELDS = "converter.ignoreFields";

  /**
   * Fork operator configuration properties.
   */
  public static final String FORK_BRANCHES_KEY = "fork.branches";
  public static final String FORK_BRANCH_NAME_KEY = "fork.branch.name";
  public static final String FORK_BRANCH_ID_KEY = "fork.branch.id";
  public static final String DEFAULT_FORK_BRANCH_NAME = "fork_";
  public static final String FORK_RECORD_QUEUE_CAPACITY_KEY = "fork.record.queue.capacity";
  public static final int DEFAULT_FORK_RECORD_QUEUE_CAPACITY = 100;
  public static final String FORK_RECORD_QUEUE_TIMEOUT_KEY = "fork.record.queue.timeout";
  public static final long DEFAULT_FORK_RECORD_QUEUE_TIMEOUT = 1000;
  public static final String FORK_RECORD_QUEUE_TIMEOUT_UNIT_KEY = "fork.record.queue.timeout.unit";
  public static final String DEFAULT_FORK_RECORD_QUEUE_TIMEOUT_UNIT = TimeUnit.MILLISECONDS.name();
  public static final String FORK_MAX_WAIT_MININUTES = "fork.max.wait.minutes";
  public static final long DEFAULT_FORK_MAX_WAIT_MININUTES = 60;
  public static final String FORK_FINISHED_CHECK_INTERVAL = "fork.finished.check.interval";
  public static final long DEFAULT_FORK_FINISHED_CHECK_INTERVAL = 1000;
  public static final String FORK_CLOSE_WRITER_ON_COMPLETION = "fork.closeWriterOnCompletion";
  public static final boolean DEFAULT_FORK_CLOSE_WRITER_ON_COMPLETION = false;


  /**
   * Writer configuration properties.
   */
  public static final String WRITER_PREFIX = "writer";
  public static final String WRITER_DESTINATION_TYPE_KEY = WRITER_PREFIX + ".destination.type";
  public static final String WRITER_OUTPUT_FORMAT_KEY = WRITER_PREFIX + ".output.format";
  public static final String WRITER_FILE_SYSTEM_URI = WRITER_PREFIX + ".fs.uri";
  public static final String WRITER_STAGING_DIR = WRITER_PREFIX + ".staging.dir";
  public static final String WRITER_STAGING_TABLE = WRITER_PREFIX + ".staging.table";
  public static final String WRITER_TRUNCATE_STAGING_TABLE = WRITER_PREFIX + ".truncate.staging.table";
  public static final String WRITER_OUTPUT_DIR = WRITER_PREFIX + ".output.dir";
  public static final String WRITER_BUILDER_CLASS = WRITER_PREFIX + ".builder.class";
  public static final String DEFAULT_WRITER_BUILDER_CLASS = "org.apache.gobblin.writer.AvroDataWriterBuilder";
  public static final String WRITER_FILE_NAME = WRITER_PREFIX + ".file.name";
  public static final String WRITER_FILE_PATH = WRITER_PREFIX + ".file.path";
  public static final String WRITER_FILE_PATH_TYPE = WRITER_PREFIX + ".file.path.type";
  public static final String WRITER_FILE_OWNER = WRITER_PREFIX + ".file.owner";
  public static final String WRITER_FILE_GROUP = WRITER_PREFIX + ".file.group";
  public static final String WRITER_FILE_REPLICATION_FACTOR = WRITER_PREFIX + ".file.replication.factor";
  public static final String WRITER_FILE_BLOCK_SIZE = WRITER_PREFIX + ".file.block.size";
  public static final String WRITER_FILE_PERMISSIONS = WRITER_PREFIX + ".file.permissions";
  public static final String WRITER_DIR_PERMISSIONS = WRITER_PREFIX + ".dir.permissions";
  public static final String WRITER_BUFFER_SIZE = WRITER_PREFIX + ".buffer.size";
  public static final String WRITER_PRESERVE_FILE_NAME = WRITER_PREFIX + ".preserve.file.name";
  public static final String WRITER_DEFLATE_LEVEL = WRITER_PREFIX + ".deflate.level";
  public static final String WRITER_CODEC_TYPE = WRITER_PREFIX + ".codec.type";
  public static final String WRITER_EAGER_INITIALIZATION_KEY = WRITER_PREFIX + ".eager.initialization";
  public static final String WRITER_PARTITIONER_CLASS = WRITER_PREFIX + ".partitioner.class";
  public static final String WRITER_SKIP_NULL_RECORD = WRITER_PREFIX + ".skipNullRecord";
  public static final boolean DEFAULT_WRITER_EAGER_INITIALIZATION = false;
  public static final String WRITER_GROUP_NAME = WRITER_PREFIX + ".group.name";
  public static final String DEFAULT_WRITER_FILE_BASE_NAME = "part";
  public static final int DEFAULT_DEFLATE_LEVEL = 9;
  public static final int DEFAULT_BUFFER_SIZE = 4096;
  public static final String DEFAULT_WRITER_FILE_PATH_TYPE = "default";
  public static final String SIMPLE_WRITER_DELIMITER = "simple.writer.delimiter";
  public static final String SIMPLE_WRITER_PREPEND_SIZE = "simple.writer.prepend.size";
  public static final String WRITER_ADD_TASK_TIMESTAMP = WRITER_PREFIX + ".addTaskTimestamp";

  // Internal use only - used to send metadata to publisher
  public static final String WRITER_METADATA_KEY = WRITER_PREFIX + "._internal.metadata";
  public static final String WRITER_PARTITION_PATH_KEY = WRITER_PREFIX + "._internal.partition.path";

  /**
   * Writer configuration properties used internally.
   */
  public static final String WRITER_FINAL_OUTPUT_FILE_PATHS = WRITER_PREFIX + ".final.output.file.paths";
  public static final String WRITER_RECORDS_WRITTEN = WRITER_PREFIX + ".records.written";
  public static final String WRITER_BYTES_WRITTEN = WRITER_PREFIX + ".bytes.written";
  public static final String WRITER_EARLIEST_TIMESTAMP = WRITER_PREFIX + ".earliest.timestamp";
  public static final String WRITER_AVERAGE_TIMESTAMP = WRITER_PREFIX + ".average.timestamp";
  public static final String WRITER_COUNT_METRICS_FROM_FAILED_TASKS = WRITER_PREFIX + ".jobTaskSummary.countMetricsFromFailedTasks";

  /**
   * Configuration properties used by the quality checker.
   */
  public static final String QUALITY_CHECKER_PREFIX = "qualitychecker";
  public static final String TASK_LEVEL_POLICY_LIST = QUALITY_CHECKER_PREFIX + ".task.policies";
  public static final String TASK_LEVEL_POLICY_LIST_TYPE = QUALITY_CHECKER_PREFIX + ".task.policy.types";
  public static final String ROW_LEVEL_POLICY_LIST = QUALITY_CHECKER_PREFIX + ".row.policies";
  public static final String ROW_LEVEL_POLICY_LIST_TYPE = QUALITY_CHECKER_PREFIX + ".row.policy.types";
  public static final String ROW_LEVEL_ERR_FILE = QUALITY_CHECKER_PREFIX + ".row.err.file";
  public static final String QUALITY_CHECKER_TIMEZONE = QUALITY_CHECKER_PREFIX + ".timezone";
  public static final String DEFAULT_QUALITY_CHECKER_TIMEZONE = PST_TIMEZONE_NAME;
  public static final String CLEAN_ERR_DIR = QUALITY_CHECKER_PREFIX + ".clean.err.dir";
  public static final boolean DEFAULT_CLEAN_ERR_DIR = false;
  /** Set the approximate max number of records to write in err_file for each task. Note the actual number of records
   * written may be anything from 0 to about the value set + 100. */
  public static final String ROW_LEVEL_ERR_FILE_RECORDS_PER_TASK =
      QUALITY_CHECKER_PREFIX + ".row.errFile.recordsPerTask";
  public static final long DEFAULT_ROW_LEVEL_ERR_FILE_RECORDS_PER_TASK = 1000000;

  /**
   * Configuration properties used by the row count policies.
   */
  public static final String EXTRACTOR_ROWS_EXTRACTED = QUALITY_CHECKER_PREFIX + ".rows.extracted";
  public static final String EXTRACTOR_ROWS_EXPECTED = QUALITY_CHECKER_PREFIX + ".rows.expected";
  public static final String WRITER_ROWS_WRITTEN = QUALITY_CHECKER_PREFIX + ".rows.written";
  public static final String ROW_COUNT_RANGE = QUALITY_CHECKER_PREFIX + ".row.count.range";

  /**
   * Configuration properties for the task status.
   */
  public static final String TASK_STATUS_REPORT_INTERVAL_IN_MS_KEY = "task.status.reportintervalinms";
  public static final long DEFAULT_TASK_STATUS_REPORT_INTERVAL_IN_MS = 30000;

  /**
   * Configuration properties for the data publisher.
   */
  public static final String DATA_PUBLISHER_PREFIX = "data.publisher";

  /**
   * Metadata configuration
   *
   * PUBLISH_WRITER_METADATA_KEY: Whether or not to publish writer-generated metadata
   * PUBLISH_WRITER_METADATA_MERGER_NAME_KEY: Class to use to merge writer-generated metadata.
   */
  public static final String DATA_PUBLISH_WRITER_METADATA_KEY = DATA_PUBLISHER_PREFIX + ".metadata.publish.writer";
  public static final String DATA_PUBLISH_WRITER_METADATA_MERGER_NAME_KEY =
      DATA_PUBLISHER_PREFIX + ".metadata.publish.writer.merger.class";

  /**
   * Metadata configuration properties used internally
   */
  public static final String DATA_PUBLISH_WRITER_METADATA_MERGER_NAME_DEFAULT =
      "org.apache.gobblin.metadata.types.GlobalMetadataJsonMerger";
  public static final String DATA_PUBLISHER_METADATA_OUTPUT_DIR = DATA_PUBLISHER_PREFIX + ".metadata.output.dir";
  //Metadata String in the configuration file
  public static final String DATA_PUBLISHER_METADATA_STR = DATA_PUBLISHER_PREFIX + ".metadata.string";
  public static final String DATA_PUBLISHER_METADATA_OUTPUT_FILE = DATA_PUBLISHER_PREFIX + ".metadata.output_file";

  /**
   * @deprecated Use {@link #TASK_DATA_PUBLISHER_TYPE} and {@link #JOB_DATA_PUBLISHER_TYPE}.
   */
  @Deprecated
  public static final String DATA_PUBLISHER_TYPE = DATA_PUBLISHER_PREFIX + ".type";
  public static final String JOB_DATA_PUBLISHER_TYPE = DATA_PUBLISHER_PREFIX + ".job.type";
  public static final String TASK_DATA_PUBLISHER_TYPE = DATA_PUBLISHER_PREFIX + ".task.type";
  public static final String DEFAULT_DATA_PUBLISHER_TYPE = "org.apache.gobblin.publisher.BaseDataPublisher";
  public static final String DATA_PUBLISHER_FILE_SYSTEM_URI = DATA_PUBLISHER_PREFIX + ".fs.uri";
  public static final String DATA_PUBLISHER_FINAL_DIR = DATA_PUBLISHER_PREFIX + ".final.dir";
  public static final String DATA_PUBLISHER_APPEND_EXTRACT_TO_FINAL_DIR =
      DATA_PUBLISHER_PREFIX + ".appendExtractToFinalDir";
  public static final boolean DEFAULT_DATA_PUBLISHER_APPEND_EXTRACT_TO_FINAL_DIR = true;
  public static final String DATA_PUBLISHER_REPLACE_FINAL_DIR = DATA_PUBLISHER_PREFIX + ".replace.final.dir";
  public static final String DATA_PUBLISHER_FINAL_NAME = DATA_PUBLISHER_PREFIX + ".final.name";
  public static final String DATA_PUBLISHER_OVERWRITE_ENABLED = DATA_PUBLISHER_PREFIX + ".overwrite.enabled";
  // @DATA_PUBLISHER_FINAL_DIR is the final publishing root directory
  // @DATA_PUBLISHER_FINAL_DIR_GROUP is set at the leaf level (DATA_PUBLISHER_FINAL_DIR/EXTRACT/file.xxx) which is incorrect
  // Use @DATA_PUBLISHER_OUTPUT_DIR_GROUP to set group at output dir level @DATA_PUBLISHER_FINAL_DIR/EXTRACT
  @Deprecated
  public static final String DATA_PUBLISHER_FINAL_DIR_GROUP = DATA_PUBLISHER_PREFIX + ".final.dir.group";
  public static final String DATA_PUBLISHER_OUTPUT_DIR_GROUP = DATA_PUBLISHER_PREFIX + ".output.dir.group";
  public static final String DATA_PUBLISHER_PERMISSIONS = DATA_PUBLISHER_PREFIX + ".permissions";
  public static final String PUBLISH_DATA_AT_JOB_LEVEL = "publish.data.at.job.level";
  public static final boolean DEFAULT_PUBLISH_DATA_AT_JOB_LEVEL = true;
  public static final String PUBLISHER_DIRS = DATA_PUBLISHER_PREFIX + ".output.dirs";
  public static final String DATA_PUBLISHER_CAN_BE_SKIPPED = DATA_PUBLISHER_PREFIX + ".canBeSkipped";
  public static final boolean DEFAULT_DATA_PUBLISHER_CAN_BE_SKIPPED = false;
  public static final String PUBLISHER_LATEST_FILE_ARRIVAL_TIMESTAMP =
      DATA_PUBLISHER_PREFIX + ".latest.file.arrival.timestamp";

  /**
   * Dynamically configured Publisher properties used internally
   */
  //Dataset-specific final publish location
  public static final String DATA_PUBLISHER_DATASET_DIR = DATA_PUBLISHER_PREFIX + ".dataset.dir";

  /**
   * Configuration properties used by the extractor.
   */
  public static final String SOURCE_ENTITY = "source.entity";
  public static final String SCHEMA_IN_SOURCE_DIR = "schema.in.source.dir";
  public static final boolean DEFAULT_SCHEMA_IN_SOURCE_DIR = false;
  public static final String SCHEMA_FILENAME = "schema.filename";
  public static final String DEFAULT_SCHEMA_FILENAME = "metadata.json";
  // An optional configuration for extractor's specific implementation to set, which helps data writer
  // tune some parameters that are relevant to the record size.
  // See the reference GobblinOrcWriter as an example.
  public static final String AVG_RECORD_SIZE = "avg.record.size";

  // Comma-separated source entity names
  public static final String SOURCE_ENTITIES = "source.entities";
  public static final String SOURCE_TIMEZONE = "source.timezone";
  public static final String SOURCE_SCHEMA = "source.schema";
  public static final String SOURCE_MAX_NUMBER_OF_PARTITIONS = "source.max.number.of.partitions";
  public static final String SOURCE_SKIP_FIRST_RECORD = "source.skip.first.record";
  public static final String SOURCE_COLUMN_NAME_CASE = "source.column.name.case";
  public static final String SOURCE_EARLY_STOP_ENABLED = "source.earlyStop.enabled";
  public static final boolean DEFAULT_SOURCE_EARLY_STOP_ENABLED = false;

  /**
   * Configuration properties used by the QueryBasedExtractor.
   */
  public static final String SOURCE_QUERYBASED_WATERMARK_TYPE = "source.querybased.watermark.type";
  public static final String SOURCE_QUERYBASED_HOUR_COLUMN = "source.querybased.hour.column";
  public static final String SOURCE_QUERYBASED_SKIP_HIGH_WATERMARK_CALC = "source.querybased.skip.high.watermark.calc";
  public static final String SOURCE_QUERYBASED_QUERY = "source.querybased.query";
  public static final String SOURCE_QUERYBASED_EXCLUDED_COLUMNS = "source.querybased.excluded.columns";
  public static final String SOURCE_QUERYBASED_IS_HOURLY_EXTRACT = "source.querybased.hourly.extract";
  public static final String SOURCE_QUERYBASED_EXTRACT_TYPE = "source.querybased.extract.type";
  public static final String SOURCE_QUERYBASED_PARTITION_INTERVAL = "source.querybased.partition.interval";
  public static final String SOURCE_QUERYBASED_START_VALUE = "source.querybased.start.value";
  public static final String SOURCE_QUERYBASED_END_VALUE = "source.querybased.end.value";
  public static final String SOURCE_QUERYBASED_APPEND_MAX_WATERMARK_LIMIT =
      "source.querybased.append.max.watermark.limit";
  public static final String SOURCE_QUERYBASED_IS_WATERMARK_OVERRIDE = "source.querybased.is.watermark.override";
  public static final String SOURCE_QUERYBASED_LOW_WATERMARK_BACKUP_SECS =
      "source.querybased.low.watermark.backup.secs";
  public static final String SOURCE_QUERYBASED_SCHEMA = "source.querybased.schema";
  public static final String SOURCE_QUERYBASED_FETCH_SIZE = "source.querybased.fetch.size";
  public static final String SOURCE_QUERYBASED_IS_SPECIFIC_API_ACTIVE = "source.querybased.is.specific.api.active";
  public static final String SOURCE_QUERYBASED_SKIP_COUNT_CALC = "source.querybased.skip.count.calc";
  public static final String SOURCE_QUERYBASED_IS_METADATA_COLUMN_CHECK_ENABLED =
      "source.querybased.is.metadata.column.check.enabled";
  public static final String SOURCE_QUERYBASED_IS_COMPRESSION_ENABLED = "source.querybased.is.compression.enabled";
  public static final String SOURCE_QUERYBASED_JDBC_RESULTSET_FETCH_SIZE =
      "source.querybased.jdbc.resultset.fetch.size";
  public static final String SOURCE_QUERYBASED_ALLOW_REMOVE_UPPER_BOUNDS = "source.querybased.allowRemoveUpperBounds";

  public static final String SOURCE_QUERYBASED_PROMOTE_UNSIGNED_INT_TO_BIGINT =
      "source.querybased.promoteUnsignedIntToBigInt";
  public static final boolean DEFAULT_SOURCE_QUERYBASED_PROMOTE_UNSIGNED_INT_TO_BIGINT = false;

  public static final String SOURCE_QUERYBASED_RESET_EMPTY_PARTITION_WATERMARK =
      "source.querybased.resetEmptyPartitionWatermark";
  public static final boolean DEFAULT_SOURCE_QUERYBASED_RESET_EMPTY_PARTITION_WATERMARK = true;

  public static final String ENABLE_DELIMITED_IDENTIFIER = "enable.delimited.identifier";
  public static final boolean DEFAULT_ENABLE_DELIMITED_IDENTIFIER = false;

  public static final String SQL_SERVER_CONNECTION_PARAMETERS = "source.querybased.sqlserver.connectionParameters";

  /**
   * Configuration properties used by the CopySource.
   */
  public static final String COPY_SOURCE_FILESET_WU_GENERATOR_CLASS = "copy.source.fileset.wu.generator.class";
  public static final String COPY_EXPECTED_SCHEMA = "gobblin.copy.expectedSchema";

  /**
   * Configuration properties used by the FileBasedExtractor
   */
  public static final String SOURCE_FILEBASED_DATA_DIRECTORY = "source.filebased.data.directory";
  public static final String SOURCE_FILEBASED_PLATFORM = "source.filebased.platform";
  public static final String SOURCE_FILEBASED_FILES_TO_PULL = "source.filebased.files.to.pull";
  public static final String SOURCE_FILEBASED_MAX_FILES_PER_RUN = "source.filebased.maxFilesPerRun";
  public static final String SOURCE_FILEBASED_FS_SNAPSHOT = "source.filebased.fs.snapshot";
  public static final String SOURCE_FILEBASED_FS_URI = "source.filebased.fs.uri";
  public static final String SOURCE_FILEBASED_PRESERVE_FILE_NAME = "source.filebased.preserve.file.name";
  public static final String SOURCE_FILEBASED_OPTIONAL_DOWNLOADER_CLASS = "source.filebased.downloader.class";
  public static final String SOURCE_FILEBASED_ENCRYPTED_CONFIG_PATH = "source.filebased.encrypted";

  public static final String SOURCE_FILEBASED_FS_PRIOR_SNAPSHOT_REQUIRED =
      "source.filebased.fs.prior.snapshot.required";
  public static final boolean DEFAULT_SOURCE_FILEBASED_FS_PRIOR_SNAPSHOT_REQUIRED = false;

  /**
   * Configuration properties used internally by the KafkaSource.
   */
  public static final String OFFSET_TOO_EARLY_COUNT = "offset.too.early.count";
  public static final String OFFSET_TOO_LATE_COUNT = "offset.too.late.count";
  public static final String FAIL_TO_GET_OFFSET_COUNT = "fail.to.get.offset.count";

  /**
   * Configuration properties used internally by the KafkaExtractor.
   */
  public static final String ERROR_PARTITION_COUNT = "error.partition.count";
  public static final String ERROR_MESSAGE_UNDECODABLE_COUNT = "error.message.undecodable.count";

  /**
   * Configuration properties for source connection.
   */
  public static final String SOURCE_CONN_PREFIX = "source.conn.";
  public static final String SOURCE_CONN_USE_AUTHENTICATION = SOURCE_CONN_PREFIX + "use.authentication";
  public static final String SOURCE_CONN_PRIVATE_KEY = SOURCE_CONN_PREFIX + "private.key";
  public static final String SOURCE_CONN_KNOWN_HOSTS = SOURCE_CONN_PREFIX + "known.hosts";
  public static final String SOURCE_CONN_CLIENT_SECRET = SOURCE_CONN_PREFIX + "client.secret";
  public static final String SOURCE_CONN_CLIENT_ID = SOURCE_CONN_PREFIX + "client.id";
  public static final String SOURCE_CONN_DOMAIN = SOURCE_CONN_PREFIX + "domain";
  public static final String SOURCE_CONN_USERNAME = SOURCE_CONN_PREFIX + "username";
  public static final String SOURCE_CONN_PASSWORD = SOURCE_CONN_PREFIX + "password";
  public static final String SOURCE_CONN_SECURITY_TOKEN = SOURCE_CONN_PREFIX + "security.token";
  public static final String SOURCE_CONN_HOST_NAME = SOURCE_CONN_PREFIX + "host";
  public static final String SOURCE_CONN_VERSION = SOURCE_CONN_PREFIX + "version";
  public static final String SOURCE_CONN_TIMEOUT = SOURCE_CONN_PREFIX + "timeout";
  public static final String SOURCE_CONN_PROPERTIES = SOURCE_CONN_PREFIX + "properties";
  public static final String SOURCE_CONN_REST_URL = SOURCE_CONN_PREFIX + "rest.url";
  public static final String SOURCE_CONN_USE_PROXY_URL = SOURCE_CONN_PREFIX + "use.proxy.url";
  public static final String SOURCE_CONN_USE_PROXY_PORT = SOURCE_CONN_PREFIX + "use.proxy.port";
  public static final String SOURCE_CONN_DRIVER = SOURCE_CONN_PREFIX + "driver";
  public static final String SOURCE_CONN_PORT = SOURCE_CONN_PREFIX + "port";
  public static final int SOURCE_CONN_DEFAULT_PORT = 22;
  public static final String SOURCE_CONN_SID = SOURCE_CONN_PREFIX + "sid";
  public static final String SOURCE_CONN_REFRESH_TOKEN = SOURCE_CONN_PREFIX + "refresh.token";
  public static final String SOURCE_CONN_DECRYPT_CLIENT_SECRET = SOURCE_CONN_PREFIX + "decrypt.client.id.secret";

  /**
   * Source default configurations.
   */
  public static final long DEFAULT_WATERMARK_VALUE = -1;
  public static final int DEFAULT_MAX_NUMBER_OF_PARTITIONS = 20;
  public static final int DEFAULT_SOURCE_FETCH_SIZE = 1000;
  public static final String DEFAULT_WATERMARK_TYPE = "timestamp";
  public static final String DEFAULT_LOW_WATERMARK_BACKUP_SECONDS = "1000";
  public static final int DEFAULT_CONN_TIMEOUT = 500000;
  public static final String ESCAPE_CHARS_IN_COLUMN_NAME = "$,&";
  public static final String ESCAPE_CHARS_IN_TABLE_NAME = "$,&";
  public static final String DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL = "'$WATERMARK'";
  public static final String DEFAULT_SOURCE_QUERYBASED_IS_METADATA_COLUMN_CHECK_ENABLED = "true";
  public static final String DEFAULT_COLUMN_NAME_CASE = "NOCHANGE";
  public static final int DEFAULT_SOURCE_QUERYBASED_JDBC_RESULTSET_FETCH_SIZE = 1000;
  public static final String FILEBASED_REPORT_STATUS_ON_COUNT = "filebased.report.status.on.count";
  public static final int DEFAULT_FILEBASED_REPORT_STATUS_ON_COUNT = 10000;
  public static final String DEFAULT_SOURCE_TIMEZONE = PST_TIMEZONE_NAME;

  /**
   * Configuration properties used by the Hadoop MR job launcher.
   */
  public static final String MR_JOB_ROOT_DIR_KEY = "mr.job.root.dir";
  /** Specifies a static location in HDFS to upload jars to. Useful for sharing jars across different Gobblin runs.*/
  @Deprecated // Deprecated; use MR_JARS_BASE_DIR instead
  public static final String MR_JARS_DIR = "mr.jars.dir";
  // dir pointed by MR_JARS_BASE_DIR has month partitioned dirs to store jar files and are cleaned up on a regular basis
  // retention feature is not available for dir pointed by MR_JARS_DIR
  public static final String MR_JARS_BASE_DIR = "mr.jars.base.dir";
  public static final String MR_JOB_MAX_MAPPERS_KEY = "mr.job.max.mappers";
  public static final String MR_JOB_MAPPER_FAILURE_IS_FATAL_KEY = "mr.job.map.failure.is.fatal";
  public static final String MR_PERSIST_WORK_UNITS_THEN_CANCEL_KEY = "mr.persist.workunits.then.cancel";
  public static final String MR_TARGET_MAPPER_SIZE = "mr.target.mapper.size";
  public static final String MR_REPORT_METRICS_AS_COUNTERS_KEY = "mr.report.metrics.as.counters";
  public static final boolean DEFAULT_MR_REPORT_METRICS_AS_COUNTERS = false;
  public static final int DEFAULT_MR_JOB_MAX_MAPPERS = 100;
  public static final boolean DEFAULT_MR_JOB_MAPPER_FAILURE_IS_FATAL = false;
  public static final boolean DEFAULT_MR_PERSIST_WORK_UNITS_THEN_CANCEL = false;
  public static final boolean DEFAULT_ENABLE_MR_SPECULATIVE_EXECUTION = false;

  /**
   * Configuration properties used by the distributed job launcher.
   */
  public static final String TASK_STATE_COLLECTOR_INTERVAL_SECONDS = "task.state.collector.interval.secs";
  public static final int DEFAULT_TASK_STATE_COLLECTOR_INTERVAL_SECONDS = 60;
  public static final String TASK_STATE_COLLECTOR_HANDLER_CLASS = "task.state.collector.handler.class";
  public static final String REPORT_JOB_PROGRESS = "report.job.progress";
  public static final boolean DEFAULT_REPORT_JOB_PROGRESS = false;
  public static final double DEFAULT_PROGRESS_REPORTING_THRESHOLD = 0.05;

  /**
   * Set to true so that job still proceed if TaskStateCollectorService failed.
   */
  public static final String JOB_PROCEED_ON_TASK_STATE_COLLECOTR_SERVICE_FAILURE =
      "job.proceed.onTaskStateCollectorServiceFailure";

  /**
   * Configuration properties for email settings.
   */
  public static final String ALERT_EMAIL_ENABLED_KEY = "email.alert.enabled";
  public static final String NOTIFICATION_EMAIL_ENABLED_KEY = "email.notification.enabled";
  public static final String EMAIL_HOST_KEY = "email.host";
  public static final String DEFAULT_EMAIL_HOST = "localhost";
  public static final String EMAIL_SMTP_PORT_KEY = "email.smtp.port";
  public static final String EMAIL_USER_KEY = "email.user";
  public static final String EMAIL_PASSWORD_KEY = "email.password";
  public static final String EMAIL_FROM_KEY = "email.from";
  public static final String EMAIL_TOS_KEY = "email.tos";

  /**
   * Common metrics configuration properties.
   */
  public static final String METRICS_CONFIGURATIONS_PREFIX = "metrics.";
  public static final String METRICS_ENABLED_KEY = METRICS_CONFIGURATIONS_PREFIX + "enabled";
  public static final String DEFAULT_METRICS_ENABLED = Boolean.toString(true);
  public static final String METRICS_REPORT_INTERVAL_KEY = METRICS_CONFIGURATIONS_PREFIX + "report.interval";
  public static final String DEFAULT_METRICS_REPORT_INTERVAL = Long.toString(TimeUnit.SECONDS.toMillis(30));
  public static final String METRIC_CONTEXT_NAME_KEY = "metrics.context.name";
  public static final String METRIC_TIMER_WINDOW_SIZE_IN_MINUTES =
      METRICS_CONFIGURATIONS_PREFIX + "timer.window.size.in.minutes";
  public static final int DEFAULT_METRIC_TIMER_WINDOW_SIZE_IN_MINUTES = 15;
  public static final String METRICS_REPORTING_CONFIGURATIONS_PREFIX = "metrics.reporting";
  public static final String METRICS_REPORTING_EVENTS_CONFIGURATIONS_PREFIX =
      METRICS_REPORTING_CONFIGURATIONS_PREFIX + ".events";

  //Configuration keys to trigger job/task failures on metric reporter instantiation failures. Useful
  //when monitoring of Gobblin pipelines critically depend on events and metrics emitted by the metrics
  //reporting service running in each container.
  public static final String GOBBLIN_TASK_METRIC_REPORTING_FAILURE_FATAL = "gobblin.task.isMetricReportingFailureFatal";
  public static final boolean DEFAULT_GOBBLIN_TASK_METRIC_REPORTING_FAILURE_FATAL = false;
  public static final String GOBBLIN_TASK_EVENT_REPORTING_FAILURE_FATAL = "gobblin.task.isEventReportingFailureFatal";
  public static final boolean DEFAULT_GOBBLIN_TASK_EVENT_REPORTING_FAILURE_FATAL = false;
  public static final String GOBBLIN_JOB_METRIC_REPORTING_FAILURE_FATAL = "gobblin.job.isMetricReportingFailureFatal";
  public static final boolean DEFAULT_GOBBLIN_JOB_METRIC_REPORTING_FAILURE_FATAL = false;
  public static final String GOBBLIN_JOB_EVENT_REPORTING_FAILURE_FATAL = "gobblin.job.isEventReportingFailureFatal";
  public static final boolean DEFAULT_GOBBLIN_JOB_EVENT_REPORTING_FAILURE_FATAL = false;

  // File-based reporting
  public static final String METRICS_REPORTING_FILE_ENABLED_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.file.enabled";
  public static final String DEFAULT_METRICS_REPORTING_FILE_ENABLED = Boolean.toString(false);
  public static final String METRICS_LOG_DIR_KEY = METRICS_CONFIGURATIONS_PREFIX + "log.dir";
  public static final String METRICS_FILE_SUFFIX = METRICS_CONFIGURATIONS_PREFIX + "reporting.file.suffix";
  public static final String DEFAULT_METRICS_FILE_SUFFIX = "";
  public static final String FAILURE_REPORTING_FILE_ENABLED_KEY = "failure.reporting.file.enabled";
  public static final String DEFAULT_FAILURE_REPORTING_FILE_ENABLED = Boolean.toString(true);
  public static final String FAILURE_LOG_DIR_KEY = "failure.log.dir";

  // JMX-based reporting
  public static final String METRICS_REPORTING_JMX_ENABLED_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.jmx.enabled";
  public static final String DEFAULT_METRICS_REPORTING_JMX_ENABLED = Boolean.toString(false);

  // Kafka-based reporting
  public static final String METRICS_REPORTING_KAFKA_ENABLED_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.enabled";
  public static final String DEFAULT_METRICS_REPORTING_KAFKA_ENABLED = Boolean.toString(false);
  public static final String METRICS_REPORTING_KAFKA_METRICS_ENABLED_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.metrics.enabled";
  public static final String METRICS_REPORTING_KAFKA_EVENTS_ENABLED_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.events.enabled";
  public static final String DEFAULT_METRICS_REPORTING_KAFKA_REPORTER_CLASS =
      "org.apache.gobblin.metrics.kafka.KafkaMetricReporterFactory";
  public static final String DEFAULT_EVENTS_REPORTING_KAFKA_REPORTER_CLASS =
      "org.apache.gobblin.metrics.kafka.KafkaEventReporterFactory";

  public static final String METRICS_REPORTING_KAFKA_FORMAT = METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.format";
  public static final String METRICS_REPORTING_EVENTS_KAFKA_FORMAT =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.events.kafka.format";
  public static final String METRICS_REPORTING_KAFKAPUSHERKEYS =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.kafkaPusherKeys";
  public static final String METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.events.kafkaPusherKeys";
  public static final String DEFAULT_METRICS_REPORTING_KAFKA_FORMAT = "json";
  public static final String METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.avro.use.schema.registry";
  public static final String METRICS_REPORTING_EVENTS_KAFKA_AVRO_SCHEMA_ID =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.events.kafka.avro.schemaId";
  public static final String METRICS_REPORTING_METRICS_KAFKA_AVRO_SCHEMA_ID =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.metrics.kafka.avro.schemaId";

  public static final String DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY = Boolean.toString(false);
  public static final String METRICS_KAFKA_BROKERS = METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.brokers";
  // Topic used for both event and metric reporting.
  // Can be overriden by METRICS_KAFKA_TOPIC_METRICS and METRICS_KAFKA_TOPIC_EVENTS.
  public static final String METRICS_KAFKA_TOPIC = METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.topic.common";
  // Topic used only for metric reporting.
  public static final String METRICS_KAFKA_TOPIC_METRICS =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.topic.metrics";
  // Topic used only for event reporting.
  public static final String METRICS_KAFKA_TOPIC_EVENTS =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.topic.events";
  // Key related configurations for raw metric and event key value reporters
  public static final int DEFAULT_REPORTER_KEY_SIZE = 100;

  public static final String METRICS_REPORTING_PUSHERKEYS = METRICS_CONFIGURATIONS_PREFIX + "reporting.pusherKeys";
  public static final String METRICS_REPORTING_EVENTS_PUSHERKEYS =
      METRICS_REPORTING_EVENTS_CONFIGURATIONS_PREFIX + ".pusherKeys";

  //Graphite-based reporting
  public static final String METRICS_REPORTING_GRAPHITE_METRICS_ENABLED_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.graphite.metrics.enabled";
  public static final String DEFAULT_METRICS_REPORTING_GRAPHITE_METRICS_ENABLED = Boolean.toString(false);
  public static final String METRICS_REPORTING_GRAPHITE_EVENTS_ENABLED_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.graphite.events.enabled";
  public static final String DEFAULT_METRICS_REPORTING_GRAPHITE_EVENTS_ENABLED = Boolean.toString(false);
  public static final String METRICS_REPORTING_GRAPHITE_HOSTNAME =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.graphite.hostname";
  public static final String METRICS_REPORTING_GRAPHITE_PORT =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.graphite.port";
  public static final String DEFAULT_METRICS_REPORTING_GRAPHITE_PORT = "2003";
  public static final String METRICS_REPORTING_GRAPHITE_EVENTS_PORT =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.graphite.events.port";
  public static final String METRICS_REPORTING_GRAPHITE_EVENTS_VALUE_AS_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.graphite.events.value.as.key";
  public static final String DEFAULT_METRICS_REPORTING_GRAPHITE_EVENTS_VALUE_AS_KEY = Boolean.toString(false);
  public static final String METRICS_REPORTING_GRAPHITE_SENDING_TYPE =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.graphite.sending.type";
  public static final String METRICS_REPORTING_GRAPHITE_PREFIX =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.graphite.prefix";
  public static final String DEFAULT_METRICS_REPORTING_GRAPHITE_PREFIX = "";

  public static final String DEFAULT_METRICS_REPORTING_GRAPHITE_SENDING_TYPE = "TCP";

  //InfluxDB-based reporting
  public static final String METRICS_REPORTING_INFLUXDB_METRICS_ENABLED_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.influxdb.metrics.enabled";
  public static final String DEFAULT_METRICS_REPORTING_INFLUXDB_METRICS_ENABLED = Boolean.toString(false);
  public static final String METRICS_REPORTING_INFLUXDB_EVENTS_ENABLED_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.influxdb.events.enabled";
  public static final String DEFAULT_METRICS_REPORTING_INFLUXDB_EVENTS_ENABLED = Boolean.toString(false);
  public static final String METRICS_REPORTING_INFLUXDB_URL = METRICS_CONFIGURATIONS_PREFIX + "reporting.influxdb.url";
  public static final String METRICS_REPORTING_INFLUXDB_DATABASE =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.influxdb.database";
  public static final String METRICS_REPORTING_INFLUXDB_EVENTS_DATABASE =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.influxdb.events.database";
  public static final String METRICS_REPORTING_INFLUXDB_USER =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.influxdb.user";
  public static final String METRICS_REPORTING_INFLUXDB_PASSWORD =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.influxdb.password";
  public static final String METRICS_REPORTING_INFLUXDB_SENDING_TYPE =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.influxdb.sending.type";
  public static final String DEFAULT_METRICS_REPORTING_INFLUXDB_SENDING_TYPE = "TCP";

  //Custom-reporting
  public static final String METRICS_CUSTOM_BUILDERS = METRICS_CONFIGURATIONS_PREFIX + "reporting.custom.builders";

  /**
   * Rest server configuration properties.
   */
  public static final String REST_SERVER_HOST_KEY = "rest.server.host";
  public static final String DEFAULT_REST_SERVER_HOST = "localhost";
  public static final String REST_SERVER_PORT_KEY = "rest.server.port";
  public static final String DEFAULT_REST_SERVER_PORT = "8080";
  public static final String REST_SERVER_ADVERTISED_URI_KEY = "rest.server.advertised.uri";

  /*
   * Admin server configuration properties.
   */
  public static final String ADMIN_SERVER_ENABLED_KEY = "admin.server.enabled";
  /** The name of the class with the admin interface. The class must implement the
   * AdminWebServerFactory interface .*/
  public static final String ADMIN_SERVER_FACTORY_CLASS_KEY = "admin.server.factory.type";
  public static final String ADMIN_SERVER_HOST_KEY = "admin.server.host";
  public static final String DEFAULT_ADMIN_SERVER_HOST = "localhost";
  public static final String ADMIN_SERVER_PORT_KEY = "admin.server.port";
  public static final String DEFAULT_ADMIN_SERVER_PORT = "8000";
  public static final String ADMIN_SERVER_HIDE_JOBS_WITHOUT_TASKS_BY_DEFAULT_KEY =
      "admin.server.hide_jobs_without_tasks_by_default.enabled";
  public static final String DEFAULT_ADMIN_SERVER_HIDE_JOBS_WITHOUT_TASKS_BY_DEFAULT = "false";
  public static final String ADMIN_SERVER_REFRESH_INTERVAL_KEY = "admin.server.refresh_interval";
  public static final long DEFAULT_ADMIN_SERVER_REFRESH_INTERVAL = 30000;

  public static final String DEFAULT_ADMIN_SERVER_FACTORY_CLASS =
      "org.apache.gobblin.admin.DefaultAdminWebServerFactory";

  /**
   * Kafka job configurations.
   */
  public static final String KAFKA_BROKERS = "kafka.brokers";
  public static final String KAFKA_BROKERS_TO_SIMPLE_NAME_MAP_KEY = "kafka.brokersToSimpleNameMap";
  public static final String KAFKA_SOURCE_WORK_UNITS_CREATION_THREADS = "kafka.source.work.units.creation.threads";
  public static final int KAFKA_SOURCE_WORK_UNITS_CREATION_DEFAULT_THREAD_COUNT = 30;
  public static final String KAFKA_SOURCE_SHARE_CONSUMER_CLIENT = "kafka.source.shareConsumerClient";
  public static final boolean DEFAULT_KAFKA_SOURCE_SHARE_CONSUMER_CLIENT = false;
  public static final String KAFKA_SOURCE_AVG_FETCH_TIME_CAP = "kakfa.source.avgFetchTimeCap";
  public static final int DEFAULT_KAFKA_SOURCE_AVG_FETCH_TIME_CAP = 100;
  public static final String SHARED_KAFKA_CONFIG_PREFIX = "gobblin.kafka.sharedConfig";

  /**
   * Kafka schema registry HTTP client configuration
   */
  public static final String KAFKA_SCHEMA_REGISTRY_HTTPCLIENT_SO_TIMEOUT =
      "kafka.schema.registry.httpclient.so.timeout";
  public static final String KAFKA_SCHEMA_REGISTRY_HTTPCLIENT_CONN_TIMEOUT =
      "kafka.schema.registry.httpclient.conn.timeout";
  public static final String KAFKA_SCHEMA_REGISTRY_HTTPCLIENT_METHOD_RETRY_COUNT =
      "kafka.schema.registry.httpclient.methodRetryCount";
  public static final String KAFKA_SCHEMA_REGISTRY_HTTPCLIENT_REQUEST_RETRY_ENABLED =
      "kafka.schema.registry.httpclient.requestRetryEnabled";
  public static final String KAFKA_SCHEMA_REGISTRY_HTTPCLIENT_METHOD_RETRY_HANDLER_CLASS =
      "kafka.schema.registry.httpclient.methodRetryHandlerClass";

  /**
   * Kafka schema registry retry configurations
   */
  public static final String KAFKA_SCHEMA_REGISTRY_RETRY_TIMES = "kafka.schema.registry.retry.times";
  public static final String KAFKA_SCHEMA_REGISTRY_RETRY_INTERVAL_IN_MILLIS =
      "kafka.schema.registry.retry.interval.inMillis";

  /**
   * Job execution info server and history store configuration properties.
   */
  // If job execution info server is enabled
  public static final String JOB_EXECINFO_SERVER_ENABLED_KEY = "job.execinfo.server.enabled";
  public static final String JOB_HISTORY_STORE_ENABLED_KEY = "job.history.store.enabled";
  public static final String JOB_HISTORY_STORE_URL_KEY = "job.history.store.url";
  public static final String JOB_HISTORY_STORE_JDBC_DRIVER_KEY = "job.history.store.jdbc.driver";
  public static final String DEFAULT_JOB_HISTORY_STORE_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
  public static final String JOB_HISTORY_STORE_USER_KEY = "job.history.store.user";
  public static final String DEFAULT_JOB_HISTORY_STORE_USER = "gobblin";
  public static final String JOB_HISTORY_STORE_PASSWORD_KEY = "job.history.store.password";
  public static final String DEFAULT_JOB_HISTORY_STORE_PASSWORD = "gobblin";

  /**
   * Password encryption and decryption properties.
   */
  public static final String ENCRYPT_KEY_FS_URI = "encrypt.key.fs.uri";
  public static final String ENCRYPT_KEY_LOC = "encrypt.key.loc";
  public static final String ENCRYPT_USE_STRONG_ENCRYPTOR = "encrypt.use.strong.encryptor";
  public static final boolean DEFAULT_ENCRYPT_USE_STRONG_ENCRYPTOR = false;
  public static final String NUMBER_OF_ENCRYPT_KEYS = "num.encrypt.keys";
  public static final int DEFAULT_NUMBER_OF_MASTER_PASSWORDS = 2;

  /**
   * Proxy Filesystem operation properties.
   */
  public static final String SHOULD_FS_PROXY_AS_USER = "should.fs.proxy.as.user";
  public static final boolean DEFAULT_SHOULD_FS_PROXY_AS_USER = false;
  public static final String FS_PROXY_AS_USER_NAME = "fs.proxy.as.user.name";
  public static final String FS_PROXY_AS_USER_TOKEN_FILE = "fs.proxy.as.user.token.file";
  public static final String SUPER_USER_NAME_TO_PROXY_AS_OTHERS = "super.user.name.to.proxy.as.others";
  public static final String SUPER_USER_KEY_TAB_LOCATION = "super.user.key.tab.location";
  public static final String TOKEN_AUTH = "TOKEN";
  public static final String KERBEROS_AUTH = "KERBEROS";
  public static final String FS_PROXY_AUTH_METHOD = "fs.proxy.auth.method";
  public static final String DEFAULT_FS_PROXY_AUTH_METHOD = TOKEN_AUTH;
  public static final String KERBEROS_REALM = "kerberos.realm";

  /**
   * Azkaban properties.
   */
  public static final String AZKABAN_EXECUTION_TIME_RANGE = "azkaban.execution.time.range";
  public static final String AZKABAN_EXECUTION_DAYS_LIST = "azkaban.execution.days.list";
  public static final String AZKABAN_PROJECT_NAME = "azkaban.flow.projectname";
  public static final String AZKABAN_FLOW_ID = "azkaban.flow.flowid";
  public static final String AZKABAN_JOB_ID = "azkaban.job.id";
  public static final String AZKABAN_EXEC_ID = "azkaban.flow.execid";
  public static final String AZKABAN_URL = "azkaban.link.execution.url";
  public static final String AZKABAN_FLOW_URL = "azkaban.link.workflow.url";
  public static final String AZKABAN_JOB_URL = "azkaban.link.job.url";
  public static final String AZKABAN_JOB_EXEC_URL = "azkaban.link.jobexec.url";
  public static final String AZKABAN_WEBSERVERHOST = "azkaban.webserverhost";
  public static final String AZKABAN_SERVER_NAME = "azkaban.server.name";

  /**
   * Hive registration properties
   */
  public static final String HIVE_REGISTRATION_POLICY = "hive.registration.policy";
  public static final String HIVE_REG_PUBLISHER_CLASS = "hive.reg.publisher.class";
  public static final String DEFAULT_HIVE_REG_PUBLISHER_CLASS =
      "org.apache.gobblin.publisher.HiveRegistrationPublisher";

  /**
   * Config store properties
   */
  public static final String CONFIG_MANAGEMENT_STORE_URI = "gobblin.config.management.store.uri";
  public static final String CONFIG_MANAGEMENT_STORE_ENABLED = "gobblin.config.management.store.enabled";
  public static final String DEFAULT_CONFIG_MANAGEMENT_STORE_ENABLED = "false";

  /**
   * Other configuration properties.
   */
  public static final String GOBBLIN_RUNTIME_DELIVERY_SEMANTICS = "gobblin.runtime.delivery.semantics";
  public static final Charset DEFAULT_CHARSET_ENCODING = Charsets.UTF_8;
  public static final String TEST_HARNESS_LAUNCHER_IMPL = "gobblin.testharness.launcher.impl";
  public static final int PERMISSION_PARSING_RADIX = 8;
  // describes a comma separated list of non transient errors that may come in a gobblin job
  // e.g. "invalid_grant,CredentialStoreException"
  public static final String GOBBLIN_NON_TRANSIENT_ERRORS = "gobblin.errorMessages.nonTransientErrors";

  /**
   * Configuration properties related to Flows
   */
  public static final String FLOW_RUN_IMMEDIATELY = "flow.runImmediately";
  public static final String GOBBLIN_FLOW_SLA_TIME = "gobblin.flow.sla.time";
  public static final String GOBBLIN_FLOW_SLA_TIME_UNIT = "gobblin.flow.sla.timeunit";
  public static final String DEFAULT_GOBBLIN_FLOW_SLA_TIME_UNIT = "MINUTES";
  public static final String GOBBLIN_JOB_START_SLA_TIME = "gobblin.job.start.sla.time";
  public static final String GOBBLIN_JOB_START_SLA_TIME_UNIT = "gobblin.job.start.sla.timeunit";
  public static final long FALLBACK_GOBBLIN_JOB_START_SLA_TIME = 10L;
  public static final String FALLBACK_GOBBLIN_JOB_START_SLA_TIME_UNIT = "MINUTES";
  public static final String DATASET_SUBPATHS_KEY = "gobblin.flow.dataset.subPaths";
  public static final String DATASET_BASE_INPUT_PATH_KEY = "gobblin.flow.dataset.baseInputPath";
  public static final String DATASET_BASE_OUTPUT_PATH_KEY = "gobblin.flow.dataset.baseOutputPath";
  public static final String DATASET_COMBINE_KEY = "gobblin.flow.dataset.combine";
  public static final String WHITELISTED_EDGE_IDS = "gobblin.flow.whitelistedEdgeIds";
  public static final String GOBBLIN_OUTPUT_JOB_LEVEL_METRICS = "gobblin.job.outputJobLevelMetrics";

  /**
   * Configuration properties related to flowGraphs
   */

  public static final String FLOWGRAPH_JAVA_PROPS_EXTENSIONS = "flowGraph.javaPropsExtensions";
  public static final String FLOWGRAPH_HOCON_FILE_EXTENSIONS = "flowGraph.hoconFileExtensions";
  public static final String DEFAULT_PROPERTIES_EXTENSIONS = "properties";
  public static final String DEFAULT_CONF_EXTENSIONS = "conf";
  public static final String FLOWGRAPH_POLLING_INTERVAL = "flowGraph.pollingInterval";
  public static final String FLOWGRAPH_BASE_DIR = "flowGraph.configBaseDirectory";
  public static final String FLOWGRAPH_ABSOLUTE_DIR = "flowGraph.absoluteDirectory";


  /***
   * Configuration properties related to TopologySpec Store
   */
  public static final String TOPOLOGYSPEC_STORE_CLASS_KEY = "topologySpec.store.class";
  public static final String TOPOLOGYSPEC_SERDE_CLASS_KEY = "topologySpec.serde.class";
  public static final String TOPOLOGYSPEC_STORE_DIR_KEY = "topologySpec.store.dir";

  /***
   * Configuration properties related to Spec Executor Instance
   */
  public static final String SPECEXECUTOR_INSTANCE_URI_KEY = "specExecInstance.uri";
  public static final String SPECEXECUTOR_INSTANCE_CAPABILITIES_KEY = "specExecInstance.capabilities";
  public static final String SPECEXECUTOR_CONFIGS_PREFIX_KEY = "specExecutor.additional.configs.key";

  /***
   * Configuration properties related to Spec Producer
   */
  public static final String SPEC_PRODUCER_SERIALIZED_FUTURE = "specProducer.serialized.future";

  /***
   * Configuration properties related to Compaction Suite
   */
  public static final String COMPACTION_PREFIX = "compaction.";
  public static final String COMPACTION_SUITE_FACTORY = COMPACTION_PREFIX + "suite.factory";
  public static final String DEFAULT_COMPACTION_SUITE_FACTORY = "CompactionSuiteBaseFactory";

  public static final String COMPACTION_PRIORITIZATION_PREFIX = COMPACTION_PREFIX + "prioritization.";
  public static final String COMPACTION_PRIORITIZER_ALIAS = COMPACTION_PRIORITIZATION_PREFIX + "prioritizerAlias";
  public static final String COMPACTION_ESTIMATOR = COMPACTION_PRIORITIZATION_PREFIX + "estimator";

  /***
   * Configuration properties related to Re-compaction
   */
  public static String RECOMPACTION_WRITE_TO_NEW_FOLDER = "recompaction.write.to.new.folder";


  /**
   * Configuration related to ConfigStore based copy/retention
   */
  public static final String CONFIG_BASED_PREFIX = "gobblin.configBased";

  /**
   * Configuration related to the Git based monitoring service
   */
  public static final String GIT_MONITOR_REPO_URI = "repositoryUri";
  public static final String GIT_MONITOR_REPO_DIR = "repositoryDirectory";
  public static final String GIT_MONITOR_CONFIG_BASE_DIR = "configBaseDirectory";
  public static final String GIT_MONITOR_POLLING_INTERVAL = "pollingInterval";
  public static final String GIT_MONITOR_BRANCH_NAME = "branchName";
  //Configuration keys for authentication using HTTPS
  public static final String GIT_MONITOR_USERNAME = "username";
  public static final String GIT_MONITOR_PASSWORD = "password";
  //Configuration keys for authentication using SSH with Public Key
  public static final String GIT_MONITOR_SSH_WITH_PUBLIC_KEY_ENABLED = "isSshWithPublicKeyEnabled";
  public static final String GIT_MONITOR_SSH_PRIVATE_KEY_PATH = "privateKeyPath";
  public static final String GIT_MONITOR_SSH_PRIVATE_KEY_BASE64_ENCODED = "privateKeyBase64";
  public static final String GIT_MONITOR_SSH_PASSPHRASE = "passphrase";
  public static final String GIT_MONITOR_SSH_STRICT_HOST_KEY_CHECKING_ENABLED = "isStrictHostKeyCheckingEnabled";
  public static final String GIT_MONITOR_SSH_KNOWN_HOSTS = "knownHosts";
  public static final String GIT_MONITOR_SSH_KNOWN_HOSTS_FILE = "knownHostsFile";
  public static final String GIT_MONITOR_JSCH_LOGGER_ENABLED = "isJschLoggerEnabled";

  /**
   * Configuration related to avro schema check strategy
   */
  public static final String AVRO_SCHEMA_CHECK_STRATEGY = "avro.schema.check.strategy";
  public static final String AVRO_SCHEMA_CHECK_STRATEGY_DEFAULT =
      "org.apache.gobblin.util.schema_check.AvroSchemaCheckDefaultStrategy";

  /**
   * Configuration and constant vale for GobblinMetadataChangeEvent
   */
  public static final String GOBBLIN_METADATA_CHANGE_EVENT_ENABLED = "GobblinMetadataChangeEvent.enabled";
  public static final String LIST_DELIMITER_KEY = ",";
  public static final String RANGE_DELIMITER_KEY = "-";

  /**
   * Configuration for emitting task events
   */
  public static final String TASK_EVENT_METADATA_GENERATOR_CLASS_KEY = "gobblin.task.event.metadata.generator.class";
  public static final String DEFAULT_TASK_EVENT_METADATA_GENERATOR_CLASS_KEY = "nooptask";

  /**
   * Configuration for sharded directory files
   */
  public static final String USE_DATASET_LOCAL_WORK_DIR = "gobblin.useDatasetLocalWorkDir";
  public static final String DESTINATION_DATASET_HANDLER_CLASS = "gobblin.destination.datasetHandlerClass";
  public static final String DATASET_DESTINATION_PATH = "gobblin.dataset.destination.path";
  public static final String TMP_DIR = ".temp";
  public static final String TRASH_DIR = ".trash";
  public static final String STAGING_DIR_DEFAULT_SUFFIX = "/" + TMP_DIR + "/taskStaging";
  public static final String OUTPUT_DIR_DEFAULT_SUFFIX = "/" + TMP_DIR + "/taskOutput";
  public static final String ROW_LEVEL_ERR_FILE_DEFAULT_SUFFIX = "/err";


  /**
   * Troubleshooter configuration
   */

  /**
   * Disables all troubleshooter functions
   * */
  public static final String TROUBLESHOOTER_DISABLED = "gobblin.troubleshooter.disabled";

  /**
   * Disables reporting troubleshooter issues as GobblinTrackingEvents
   * */
  public static final String TROUBLESHOOTER_DISABLE_EVENT_REPORTING = "gobblin.troubleshooter.disableEventReporting";

  /**
   * The maximum number of issues that In-memory troubleshooter repository will keep.
   *
   * This setting can control memory usage of the troubleshooter.
   * */
  public static final String TROUBLESHOOTER_IN_MEMORY_ISSUE_REPOSITORY_MAX_SIZE = "gobblin.troubleshooter.inMemoryIssueRepository.maxSize";
  public static final int DEFAULT_TROUBLESHOOTER_IN_MEMORY_ISSUE_REPOSITORY_MAX_SIZE = 100;

  public static final String JOB_METRICS_REPORTER_CLASS_KEY = "gobblin.job.metrics.reporter.class";
  public static final String DEFAULT_JOB_METRICS_REPORTER_CLASS = "org.apache.gobblin.runtime.metrics.DefaultGobblinJobMetricReporter";

}
