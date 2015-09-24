/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.configuration;

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

  // Directory where all job configuration files are stored
  public static final String JOB_CONFIG_FILE_DIR_KEY = "jobconf.dir";

  // Job configuration file extensions
  public static final String JOB_CONFIG_FILE_EXTENSIONS_KEY = "jobconf.extensions";
  // Default job configuration file extensions
  public static final String DEFAULT_JOB_CONFIG_FILE_EXTENSIONS = "pull,job";

  // Root directory where task state files are stored
  public static final String STATE_STORE_ROOT_DIR_KEY = "state.store.dir";

  // File system URI for file-system-based task store
  public static final String STATE_STORE_FS_URI_KEY = "state.store.fs.uri";

  // Directory where job lock files are stored
  public static final String JOB_LOCK_DIR_KEY = "job.lock.dir";

  // Job launcher type
  public static final String JOB_LAUNCHER_TYPE_KEY = "launcher.type";

  // If job execution info server is enabled
  public static final String JOB_EXECINFO_SERVER_ENABLED_KEY = "job.execinfo.server.enabled";

  // Job executor thread pool size
  public static final String JOB_EXECUTOR_THREAD_POOL_SIZE_KEY = "jobexecutor.threadpool.size";
  public static final int DEFAULT_JOB_EXECUTOR_THREAD_POOL_SIZE = 5;

  // Job configuration file monitor polling interval in milliseconds
  public static final String JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY = "jobconf.monitor.interval";
  public static final long DEFAULT_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL = 300000;

  // Comma-separated list of framework jars to include
  public static final String FRAMEWORK_JAR_FILES_KEY = "framework.jars";

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
   * Common job configuration properties.
   */
  public static final String JOB_NAME_KEY = "job.name";
  public static final String JOB_GROUP_KEY = "job.group";
  public static final String JOB_DESCRIPTION_KEY = "job.description";
  public static final String JOB_SCHEDULE_KEY = "job.schedule";
  public static final String SOURCE_CLASS_KEY = "source.class";
  public static final String CONVERTER_CLASSES_KEY = "converter.classes";
  public static final String FORK_OPERATOR_CLASS_KEY = "fork.operator.class";
  public static final String DEFAULT_FORK_OPERATOR_CLASS = "gobblin.fork.IdentityForkOperator";
  public static final String JOB_COMMIT_POLICY_KEY = "job.commit.policy";
  public static final String DEFAULT_JOB_COMMIT_POLICY = "full";
  public static final String WORK_UNIT_RETRY_POLICY_KEY = "workunit.retry.policy";
  public static final String WORK_UNIT_RETRY_ENABLED_KEY = "workunit.retry.enabled";
  public static final String JOB_RUN_ONCE_KEY = "job.runonce";
  public static final String JOB_DISABLED_KEY = "job.disabled";
  public static final String JOB_JAR_FILES_KEY = "job.jars";
  public static final String JOB_LOCAL_FILES_KEY = "job.local.files";
  public static final String JOB_HDFS_FILES_KEY = "job.hdfs.files";
  public static final String JOB_LOCK_ENABLED_KEY = "job.lock.enabled";
  public static final String JOB_MAX_FAILURES_KEY = "job.max.failures";
  public static final int DEFAULT_JOB_MAX_FAILURES = 1;
  public static final String MAX_TASK_RETRIES_KEY = "task.maxretries";
  public static final int DEFAULT_MAX_TASK_RETRIES = 5;
  public static final String TASK_RETRY_INTERVAL_IN_SEC_KEY = "task.retry.intervalinsec";
  public static final long DEFAULT_TASK_RETRY_INTERVAL_IN_SEC = 300;
  public static final String OVERWRITE_CONFIGS_IN_STATESTORE = "overwrite.configs.in.statestore";
  public static final boolean DEFAULT_OVERWRITE_CONFIGS_IN_STATESTORE = false;

  /**
   * Configuration properties used internally.
   */
  public static final String JOB_ID_KEY = "job.id";
  public static final String TASK_ID_KEY = "task.id";
  public static final String JOB_CONFIG_FILE_PATH_KEY = "job.config.path";
  public static final String TASK_FAILURE_EXCEPTION_KEY = "task.failure.exception";
  public static final String TASK_RETRIES_KEY = "task.retries";
  public static final String JOB_FAILURES_KEY = "job.failures";
  public static final String JOB_TRACKING_URL_KEY = "job.tracking.url";
  public static final String FORK_STATE_KEY = "fork.state";
  public static final String JOB_STATE_FILE_PATH_KEY = "job.state.file.path";

  /**
   * Dataset-related configuration properties;
   */
  // This property is used to specify the URN of a dataset a job or WorkUnit extracts data for
  public static final String DATASET_URN_KEY = "dataset.urn";
  public static final String DEFAULT_DATASET_URN = "";

  /**
   * Work unit related configuration properties.
   */
  public static final String WORK_UNIT_LOW_WATER_MARK_KEY = "workunit.low.water.mark";
  public static final String WORK_UNIT_HIGH_WATER_MARK_KEY = "workunit.high.water.mark";

  /**
   * Work unit runtime state related configuration properties.
   */
  public static final String WORK_UNIT_WORKING_STATE_KEY = "workunit.working.state";
  public static final String WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK = "workunit.state.runtime.high.water.mark";
  public static final String WORK_UNIT_STATE_ACTUAL_HIGH_WATER_MARK_KEY = "workunit.state.actual.high.water.mark";

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
  public static final String EXTRACT_FULL_RUN_TIME_KEY = "extract.full.run.time";
  public static final String EXTRACT_PRIMARY_KEY_FIELDS_KEY = "extract.primary.key.fields";
  public static final String EXTRACT_DELTA_FIELDS_KEY = "extract.delta.fields";
  public static final String EXTRACT_SCHEMA = "extract.schema";
  public static final String EXTRACT_LIMIT_ENABLED_KEY = "extract.limit.enabled";
  public static final boolean DEFAULT_EXTRACT_LIMIT_ENABLED = false;

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
  public static final String CONVERTER_CSV_TO_JSON_ENCLOSEDCHAR = "converter.csv.to.json.enclosedchar";
  public static final String DEFAULT_CONVERTER_CSV_TO_JSON_ENCLOSEDCHAR = "\0";

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

  /**
   * Writer configuration properties.
   */
  public static final String WRITER_PREFIX = "writer";
  public static final String WRITER_DESTINATION_TYPE_KEY = WRITER_PREFIX + ".destination.type";
  public static final String WRITER_OUTPUT_FORMAT_KEY = WRITER_PREFIX + ".output.format";
  public static final String WRITER_FILE_SYSTEM_URI = WRITER_PREFIX + ".fs.uri";
  public static final String WRITER_STAGING_DIR = WRITER_PREFIX + ".staging.dir";
  public static final String WRITER_OUTPUT_DIR = WRITER_PREFIX + ".output.dir";
  // WRITER_FINAL_OUTPUT_PATH is used for internal purposes only to pass the absolute writer path to the publisher
  public static final String WRITER_FINAL_OUTPUT_FILE_PATHS = WRITER_PREFIX + ".final.output.file.paths";
  public static final String WRITER_BUILDER_CLASS = WRITER_PREFIX + ".builder.class";
  public static final String DEFAULT_WRITER_BUILDER_CLASS = "gobblin.writer.AvroDataWriterBuilder";
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
  public static final boolean DEFAULT_EAGER_WRITER_INITIALIZATION = false;

  // Deprecated. Use WRITER_PARTITION_COLUMNS
  @Deprecated
  public static final String WRITER_PARTITION_COLUMN_NAME = WRITER_PREFIX + ".partition.column.name";
  public static final String WRITER_PARTITION_COLUMNS = WRITER_PREFIX + ".partition.columns";
  public static final String WRITER_PARTITION_LEVEL = WRITER_PREFIX + ".partition.level";
  public static final String WRITER_PARTITION_PATTERN = WRITER_PREFIX + ".partition.pattern";
  public static final String WRITER_PARTITION_TIMEZONE = WRITER_PREFIX + ".partition.timezone";
  public static final String WRITER_GROUP_NAME = WRITER_PREFIX + ".group.name";
  public static final String DEFAULT_WRITER_FILE_BASE_NAME = "part";
  public static final int DEFAULT_DEFLATE_LEVEL = 9;
  public static final String DEFAULT_BUFFER_SIZE = "4096";
  public static final String DEFAULT_WRITER_PARTITION_LEVEL = "daily";
  public static final String DEFAULT_WRITER_PARTITION_PATTERN = "yyyy/MM/dd";
  public static final String DEFAULT_WRITER_PARTITION_TIMEZONE = "America/Los_Angeles";
  public static final String DEFAULT_WRITER_FILE_PATH_TYPE = "default";

  public static final String SIMPLE_WRITER_DELIMITER = "simple.writer.delimiter";
  public static final String SIMPLE_WRITER_PREPEND_SIZE = "simple.writer.prepend.size";

  /**
   * Writer configuration properties used internally.
   */
  public static final String WRITER_RECORDS_WRITTEN = WRITER_PREFIX + ".records.written";
  public static final String WRITER_BYTES_WRITTEN = WRITER_PREFIX + ".bytes.written";
  public static final String WRITER_EARLIEST_TIMESTAMP = WRITER_PREFIX + ".earliest.timestamp";
  public static final String WRITER_AVERAGE_TIMESTAMP = WRITER_PREFIX + ".average.timestamp";

  /**
   * Configuration properties used by the quality checker.
   */
  public static final String QUALITY_CHECKER_PREFIX = "qualitychecker";
  public static final String TASK_LEVEL_POLICY_LIST = QUALITY_CHECKER_PREFIX + ".task.policies";
  public static final String TASK_LEVEL_POLICY_LIST_TYPE = QUALITY_CHECKER_PREFIX + ".task.policy.types";
  public static final String ROW_LEVEL_POLICY_LIST = QUALITY_CHECKER_PREFIX + ".row.policies";
  public static final String ROW_LEVEL_POLICY_LIST_TYPE = QUALITY_CHECKER_PREFIX + ".row.policy.types";
  public static final String ROW_LEVEL_ERR_FILE = QUALITY_CHECKER_PREFIX + ".row.err.file";

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
  public static final String DATA_PUBLISHER_TYPE = DATA_PUBLISHER_PREFIX + ".type";
  public static final String DEFAULT_DATA_PUBLISHER_TYPE = "gobblin.publisher.BaseDataPublisher";
  public static final String DATA_PUBLISHER_FINAL_DIR = DATA_PUBLISHER_PREFIX + ".final.dir";
  public static final String DATA_PUBLISHER_REPLACE_FINAL_DIR = DATA_PUBLISHER_PREFIX + ".replace.final.dir";
  public static final String DATA_PUBLISHER_FINAL_NAME = DATA_PUBLISHER_PREFIX + ".final.name";
  // This property is used to specify the owner group of the data publisher final output directory
  public static final String DATA_PUBLISHER_FINAL_DIR_GROUP = DATA_PUBLISHER_PREFIX + ".final.dir.group";
  public static final String DATA_PUBLISHER_PERMISSIONS = DATA_PUBLISHER_PREFIX + ".permissions";
  public static final String PUBLISH_DATA_AT_JOB_LEVEL = "publish.data.at.job.level";
  public static final boolean DEFAULT_PUBLISH_DATA_AT_JOB_LEVEL = true;

  /**
   * Configuration properties used by the extractor.
   */
  public static final String SOURCE_ENTITY = "source.entity";
  public static final String SOURCE_TIMEZONE = "source.timezone";
  public static final String SOURCE_SCHEMA = "source.schema";
  public static final String SOURCE_MAX_NUMBER_OF_PARTITIONS = "source.max.number.of.partitions";
  public static final String SOURCE_SKIP_FIRST_RECORD = "source.skip.first.record";
  public static final String SOURCE_COLUMN_NAME_CASE = "source.column.name.case";

  /**
   * Configuration properties used by the QueryBasedExtractor.
   */
  public static final String SOURCE_QUERYBASED_WATERMARK_TYPE = "source.querybased.watermark.type";
  public static final String SOURCE_QUERYBASED_HOUR_COLUMN = "source.querybased.hour.column";
  public static final String SOURCE_QUERYBASED_SKIP_HIGH_WATERMARK_CALC = "source.querybased.skip.high.watermark.calc";
  public static final String SOURCE_QUERYBASED_QUERY = "source.querybased.query";
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

  /**
   * Configuration properties used by the FileBasedExtractor
   */
  public static final String SOURCE_FILEBASED_DATA_DIRECTORY = "source.filebased.data.directory";
  public static final String SOURCE_FILEBASED_FILES_TO_PULL = "source.filebased.files.to.pull";
  public static final String SOURCE_FILEBASED_FS_SNAPSHOT = "source.filebased.fs.snapshot";
  public static final String SOURCE_FILEBASED_FS_URI = "source.filebased.fs.uri";
  public static final String SOURCE_FILEBASED_PRESERVE_FILE_NAME = "source.filebased.preserve.file.name";

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
  public static final String ERROR_MESSAGE_INVALID_SCHEMA_ID_COUNT = "error.message.invalid.schema.id.count";
  public static final String ERROR_MESSAGE_UNDECODABLE_COUNT = "error.message.undecodable.count";

  /**
   * Configuration properties for source connection.
   */
  public static final String SOURCE_CONN_USE_AUTHENTICATION = "source.conn.use.authentication";
  public static final String SOURCE_CONN_PRIVATE_KEY = "source.conn.private.key";
  public static final String SOURCE_CONN_KNOWN_HOSTS = "source.conn.known.hosts";
  public static final String SOURCE_CONN_CLIENT_SECRET = "source.conn.client.secret";
  public static final String SOURCE_CONN_CLIENT_ID = "source.conn.client.id";
  public static final String SOURCE_CONN_DOMAIN = "source.conn.domain";
  public static final String SOURCE_CONN_USERNAME = "source.conn.username";
  public static final String SOURCE_CONN_PASSWORD = "source.conn.password";
  public static final String SOURCE_CONN_SECURITY_TOKEN = "source.conn.security.token";
  public static final String SOURCE_CONN_HOST_NAME = "source.conn.host";
  public static final String SOURCE_CONN_VERSION = "source.conn.version";
  public static final String SOURCE_CONN_TIMEOUT = "source.conn.timeout";
  public static final String SOURCE_CONN_REST_URL = "source.conn.rest.url";
  public static final String SOURCE_CONN_USE_PROXY_URL = "source.conn.use.proxy.url";
  public static final String SOURCE_CONN_USE_PROXY_PORT = "source.conn.use.proxy.port";
  public static final String SOURCE_CONN_DRIVER = "source.conn.driver";
  public static final String SOURCE_CONN_PORT = "source.conn.port";
  public static final int SOURCE_CONN_DEFAULT_PORT = 22;

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
  public static final String DEFAULT_SOURCE_TIMEZONE = "America/Los_Angeles";

  /**
   * Configuration properties used by the Hadoop MR job launcher.
   */
  public static final String MR_JOB_ROOT_DIR_KEY = "mr.job.root.dir";
  public static final String MR_JOB_MAX_MAPPERS_KEY = "mr.job.max.mappers";
  public static final String MR_REPORT_METRICS_AS_COUNTERS_KEY = "mr.report.metrics.as.counters";
  public static final boolean DEFAULT_MR_REPORT_METRICS_AS_COUNTERS = false;
  public static final int DEFAULT_MR_JOB_MAX_MAPPERS = 100;

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
   * Configuration properties for compaction.
   */
  public static final String COMPACTION_PREFIX = "compaction.";
  public static final String COMPACTION_THREAD_POOL_SIZE = COMPACTION_PREFIX + "thread.pool.size";
  public static final int DEFAULT_COMPACTION_THREAD_POOL_SIZE = 20;
  public static final String COMPACTION_INPUT_DIR = COMPACTION_PREFIX + "input.dir";
  public static final String COMPACTION_INPUT_SUBDIR = COMPACTION_PREFIX + "input.subdir";
  public static final String DEFAULT_COMPACTION_INPUT_SUBDIR = "hourly";
  public static final String COMPACTION_JOB_INPUT_DIR = COMPACTION_PREFIX + "job.input.dir";
  public static final String COMPACTION_DEST_DIR = COMPACTION_PREFIX + "dest.dir";
  public static final String COMPACTION_DEST_SUBDIR = COMPACTION_PREFIX + "dest.subdir";
  public static final String DEFAULT_COMPACTION_DEST_SUBDIR = "daily";
  public static final String COMPACTION_JOB_DEST_DIR = COMPACTION_PREFIX + "job.dest.dir";
  public static final String COMPACTION_JOB_DEST_PARTITION = COMPACTION_PREFIX + "job.dest.partition";
  public static final String COMPACTION_TMP_DIR = COMPACTION_PREFIX + "tmp.dir";
  public static final String DEFAULT_COMPACTION_TMP_DIR = "/tmp";
  public static final String COMPACTION_JOB_TMP_DIR = COMPACTION_PREFIX + "job.tmp.dir";
  public static final String COMPACTION_BLACKLIST = COMPACTION_PREFIX + "blacklist";
  public static final String COMPACTION_WHITELIST = COMPACTION_PREFIX + "whitelist";
  public static final String COMPACTION_HIGH_PRIORITY_TOPICS = COMPACTION_PREFIX + "high.priority.topics";
  public static final String COMPACTION_NORMAL_PRIORITY_TOPICS = COMPACTION_PREFIX + "normal.priority.topics";
  public static final String COMPACTION_JOBPROPS_CREATOR_CLASS = COMPACTION_PREFIX + "jobprops.creator.class";
  public static final String DEFAULT_COMPACTION_JOBPROPS_CREATOR_CLASS =
      "gobblin.compaction.mapreduce.MRCompactorTimeBasedJobPropCreator";
  public static final String COMPACTION_TIMEBASED_FOLDER_PATTERN = COMPACTION_PREFIX + "timebased.folder.pattern";
  public static final String DEFAULT_COMPACTION_TIMEBASED_FOLDER_PATTERN = "YYYY/MM/dd";
  public static final String COMPACTION_TIMEZONE = COMPACTION_PREFIX + "timezone";
  public static final String DEFAULT_COMPACTION_TIMEZONE = "America/Los_Angeles";
  public static final String COMPACTION_TIMEBASED_MAX_TIME_AGO = COMPACTION_PREFIX + "timebased.max.time.ago";
  public static final String DEFAULT_COMPACTION_TIMEBASED_MAX_TIME_AGO = "3d";
  public static final String COMPACTION_TIMEBASED_MIN_TIME_AGO = COMPACTION_PREFIX + "timebased.min.time.ago";
  public static final String DEFAULT_COMPACTION_TIMEBASED_MIN_TIME_AGO = "1d";
  public static final String COMPACTION_TOPIC = COMPACTION_PREFIX + "topic";
  public static final String COMPACTION_OUTPUT_DIR_PERMISSION = COMPACTION_PREFIX + "output.dir.permission";
  public static final String COMPACTION_TARGET_OUTPUT_FILE_SIZE = COMPACTION_PREFIX + "target.output.file.size";
  public static final long DEFAULT_COMPACTION_TARGET_OUTPUT_FILE_SIZE = 268435456;
  public static final String COMPACTION_MAX_NUM_REDUCERS = COMPACTION_PREFIX + "max.num.reducers";
  public static final int DEFAULT_COMPACTION_MAX_NUM_REDUCERS = 900;
  public static final String COMPACTION_MAPRED_MAX_SPLIT_SIZE = COMPACTION_PREFIX + "mapred.max.split.size";
  public static final long DEFAULT_COMPACTION_MAPRED_MAX_SPLIT_SIZE = 268435456;
  public static final String COMPACTION_MAPRED_MIN_SPLIT_SIZE = COMPACTION_PREFIX + "mapred.min.split.size";
  public static final long DEFAULT_COMPACTION_MAPRED_MIN_SPLIT_SIZE = 268435456;
  public static final String COMPACTION_AVRO_KEY_SCHEMA_LOC = COMPACTION_PREFIX + "avro.key.schema.loc";
  public static final String COMPACTION_DEDUP_KEY = COMPACTION_PREFIX + "dedup.key";
  public static final String COMPACTION_JOB_RUNNER_CLASS = COMPACTION_PREFIX + "job.runner.class";
  public static final String DEFAULT_COMPACTION_JOB_RUNNER_CLASS =
      "gobblin.compaction.mapreduce.avro.MRCompactorAvroKeyDedupJobRunner";
  public static final String COMPACTION_COMPACTOR_CLASS = COMPACTION_PREFIX + "compactor.class";
  public static final String DEFAULT_COMPACTION_COMPACTOR_CLASS = "gobblin.compaction.mapreduce.MRCompactor";
  public static final String COMPACTION_FILE_SYSTEM_URI = COMPACTION_PREFIX + "file.system.uri";
  public static final String COMPACTION_MR_JOB_TIMEOUT_MINUTES = COMPACTION_PREFIX + "mr.job.timeout.minutes";
  public static final int DEFAULT_COMPACTION_MR_JOB_TIMEOUT_MINUTES = Integer.MAX_VALUE;
  public static final String COMPACTION_RECOMPACT_FOR_LATE_DATA = COMPACTION_PREFIX + "recompact.for.late.data";
  public static final boolean DEFAULT_COMPACTION_RECOMPACT_FOR_LATE_DATA = false;
  public static final String COMPACTION_DEDUPLICATE = COMPACTION_PREFIX + "deduplicate";
  public static final boolean DEFAULT_COMPACTION_DEDUPLICATE = true;
  public static final String COMPACTION_JOB_LATE_DATA_MOVEMENT_TASK = COMPACTION_PREFIX + "job.late.data.movement.task";
  public static final String COMPACTION_JOB_LATE_DATA_FILES = COMPACTION_PREFIX + "job.late.data.files";
  public static final String COMPACTION_COMPLETE_FILE_NAME = "_COMPACTION_COMPLETE";
  public static final String COMPACTION_LATE_FILES_DIRECTORY = "late";
  public static final String COMPACTION_ENABLE_SUCCESS_FILE = "mapreduce.fileoutputcommitter.marksuccessfuljobs";
  public static final String COMPACTION_OVERWRITE_OUTPUT_DIR = COMPACTION_PREFIX + "overwrite.output.dir";
  public static final boolean DEFAULT_COMPACTION_OVERWRITE_OUTPUT_DIR = false;
  public static final String COMPACTION_JARS = COMPACTION_PREFIX + "jars";
  public static final String COMPACTION_TRACKING_EVENTS_NAMESPACE = COMPACTION_PREFIX + "tracking.events";

  /**
   * Common metrics configuration properties.
   */
  public static final String METRICS_CONFIGURATIONS_PREFIX = "metrics.";
  public static final String METRICS_LOG_DIR_KEY = METRICS_CONFIGURATIONS_PREFIX + "log.dir";
  public static final String METRICS_ENABLED_KEY = METRICS_CONFIGURATIONS_PREFIX + "enabled";
  public static final String DEFAULT_METRICS_ENABLED = Boolean.toString(true);
  public static final String METRICS_FILE_SUFFIX = METRICS_CONFIGURATIONS_PREFIX + "reporting.file.suffix";
  public static final String DEFAULT_METRICS_FILE_SUFFIX = "";
  public static final String METRICS_REPORTING_FILE_ENABLED_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.file.enabled";
  public static final String DEFAULT_METRICS_REPORTING_FILE_ENABLED = Boolean.toString(false);
  public static final String METRICS_REPORTING_JMX_ENABLED_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.jmx.enabled";
  public static final String DEFAULT_METRICS_REPORTING_JMX_ENABLED = Boolean.toString(false);
  public static final String METRICS_REPORTING_KAFKA_ENABLED_KEY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.enabled";
  public static final String DEFAULT_METRICS_REPORTING_KAFKA_ENABLED = Boolean.toString(false);
  public static final String METRICS_REPORTING_KAFKA_FORMAT = METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.format";
  public static final String DEFAULT_METRICS_REPORTING_KAFKA_FORMAT = "json";
  public static final String METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.avro.use.schema.registry";
  public static final String DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY = Boolean.toString(false);
  public static final String METRICS_KAFKA_BROKERS = METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.brokers";
  // Topic used for both event and metric reporting.
  // Can be overriden by METRICS_KAFKA_TOPIC_METRICS and METRICS_KAFKA_TOPIC_EVENTS.
  public static final String METRICS_KAFKA_TOPIC = METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.topic";
  // Topic used only for metric reporting.
  public static final String METRICS_KAFKA_TOPIC_METRICS =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.topic.metrics";
  // Topic used only for event reporting.
  public static final String METRICS_KAFKA_TOPIC_EVENTS =
      METRICS_CONFIGURATIONS_PREFIX + "reporting.kafka.topic.events";
  public static final String METRICS_CUSTOM_BUILDERS = METRICS_CONFIGURATIONS_PREFIX + "reporting.custom.builders";
  public static final String METRICS_REPORT_INTERVAL_KEY = METRICS_CONFIGURATIONS_PREFIX + "report.interval";
  public static final String DEFAULT_METRICS_REPORT_INTERVAL = Long.toString(TimeUnit.SECONDS.toMillis(30));

  /**
   * Rest server configuration properties.
   */
  public static final String REST_SERVER_HOST_KEY = "rest.server.host";
  public static final String DEFAULT_REST_SERVER_HOST = "localhost";
  public static final String REST_SERVER_PORT_KEY = "rest.server.port";
  public static final String DEFAULT_REST_SERVER_PORT = "8080";

  /**
   * Kafka job configurations.
   */
  public static final String KAFKA_BROKERS = "kafka.brokers";

  /**
   * MySQL job history store configuration properties.
   */
  public static final String JOB_HISTORY_STORE_ENABLED_KEY = "job.history.store.enabled";
  public static final String JOB_HISTORY_STORE_URL_KEY = "job.history.store.url";
  public static final String JOB_HISTORY_STORE_JDBC_DRIVER_KEY = "job.history.store.jdbc.driver";
  public static final String DEFAULT_JOB_HISTORY_STORE_JDBC_DRIVER = "com.mysql.jdbc.Driver";
  public static final String JOB_HISTORY_STORE_USER_KEY = "job.history.store.user";
  public static final String DEFAULT_JOB_HISTORY_STORE_USER = "gobblin";
  public static final String JOB_HISTORY_STORE_PASSWORD_KEY = "job.history.store.password";
  public static final String DEFAULT_JOB_HISTORY_STORE_PASSWORD = "gobblin";

  /**
   * Password encryption and decryption properties.
   */
  public static final String ENCRYPT_KEY_LOC = "encrypt.key.loc";
  public static final String ENCRYPT_USE_STRONG_ENCRYPTOR = "encrypt.use.strong.encryptor";
  public static final boolean DEFAULT_ENCRYPT_USE_STRONG_ENCRYPTOR = false;

  /**
   * Proxy Filesystem operation properties.
   */
  public static final String SHOULD_FS_PROXY_AS_USER = "should.fs.proxy.as.user";
  public static final boolean DEFAULT_SHOULD_FS_PROXY_AS_USER = false;
  public static final String FS_PROXY_AS_USER_NAME = "fs.proxy.as.user.name";
  public static final String FS_PROXY_AS_USER_TOKEN_FILE = "fs.proxy.as.user.token.file";
  public static final String SUPER_USER_NAME_TO_PROXY_AS_OTHERS = "super.user.name.to.proxy.as.others";
  public static final String SUPER_USER_KEY_TAB_LOCATION = "super.user.key.tab.location";

  /**
   * Azkaban properties.
   */
  public static final String AZKABAN_EXECUTION_TIME_RANGE = "azkaban.execution.time.range";
  public static final String AZKABAN_EXECUTION_DAYS_LIST = "azkaban.execution.days.list";

  /**
   * Other configuration properties.
   */
  public static final Charset DEFAULT_CHARSET_ENCODING = Charsets.UTF_8;
  public static final String TEST_HARNESS_LAUNCHER_IMPL = "gobblin.testharness.launcher.impl";
  public static final int PERMISSION_PARSING_RADIX = 8;
}
