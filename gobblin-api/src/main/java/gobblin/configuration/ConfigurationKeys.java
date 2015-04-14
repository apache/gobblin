/* (c) 2014 LinkedIn Corp. All rights reserved.
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

import java.util.concurrent.TimeUnit;


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
  public static final String TASK_STATE_TRACKER_THREAD_POOL_MAX_SIZE_KEY = "tasktracker.threadpool.maxsize";
  public static final String TASK_RETRY_THREAD_POOL_CORE_SIZE_KEY = "taskretry.threadpool.coresize";
  public static final String TASK_RETRY_THREAD_POOL_MAX_SIZE_KEY = "taskretry.threadpool.maxsize";
  public static final int DEFAULT_TASK_EXECUTOR_THREADPOOL_SIZE = 2;
  public static final int DEFAULT_TASK_STATE_TRACKER_THREAD_POOL_CORE_SIZE = 1;
  public static final int DEFAULT_TASK_STATE_TRACKER_THREAD_POOL_MAX_SIZE = 2;
  public static final int DEFAULT_TASK_RETRY_THREAD_POOL_CORE_SIZE = 1;
  public static final int DEFAULT_TASK_RETRY_THREAD_POOL_MAX_SIZE = 2;

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
  public static final boolean DEFAULT_OVERWRITE_CONFIGS_IN_STATESTORE = Boolean.FALSE;

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
  public static final String EXTRACT_PULL_LIMIT = "extract.pull.limit";

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
  public static final String FORK_BRANCH_RECORD_QUEUE_CAPACITY_KEY = "fork.record.queue.capacity";
  public static final int DEFAULT_FORK_BRANCH_RECORD_QUEUE_CAPACITY = 1000;
  public static final String FORK_BRANCH_RECORD_QUEUE_TIMEOUT_KEY = "fork.record.queue.timeout";
  public static final long DEFAULT_FORK_BRANCH_RECORD_QUEUE_TIMEOUT = 1000;
  public static final String FORK_BRANCH_RECORD_QUEUE_TIMEOUT_UNIT_KEY = "fork.record.queue.timeout.unit";
  public static final String DEFAULT_FORK_BRANCH_RECORD_QUEUE_TIMEOUT_UNIT = TimeUnit.MILLISECONDS.name();

  /**
   * Writer configuration properties.
   */
  public static final String WRITER_PREFIX = "writer";
  public static final String WRITER_DESTINATION_TYPE_KEY = WRITER_PREFIX + ".destination.type";
  public static final String WRITER_OUTPUT_FORMAT_KEY = WRITER_PREFIX + ".output.format";
  public static final String WRITER_FILE_SYSTEM_URI = WRITER_PREFIX + ".fs.uri";
  public static final String WRITER_STAGING_DIR = WRITER_PREFIX + ".staging.dir";
  public static final String WRITER_OUTPUT_DIR = WRITER_PREFIX + ".output.dir";
  public static final String WRITER_BUILDER_CLASS = WRITER_PREFIX + ".builder.class";
  public static final String DEFAULT_WRITER_BUILDER_CLASS = "gobblin.writer.AvroDataWriterBuilder";
  public static final String WRITER_FILE_NAME = WRITER_PREFIX + ".file.name";
  public static final String WRITER_FILE_PATH = WRITER_PREFIX + ".file.path";
  public static final String WRITER_BUFFER_SIZE = WRITER_PREFIX + ".buffer.size";
  public static final String WRITER_PRESERVE_FILE_NAME = WRITER_PREFIX + ".preserve.file.name";
  public static final String WRITER_DEFLATE_LEVEL = WRITER_PREFIX + ".deflate.level";
  public static final String WRITER_CODEC_TYPE = WRITER_PREFIX + ".codec.type";
  public static final String DEFAULT_DEFLATE_LEVEL = "9";
  public static final String DEFAULT_BUFFER_SIZE = "4096";

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
  public static final String MR_INCLUDE_TASK_COUNTERS_KEY = "mr.include.task.counters";
  public static final boolean DEFAULT_MR_INCLUDE_TASK_COUNTERS = Boolean.FALSE;

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
  public static final String METRICS_LOG_DIR_KEY = "metrics.log.dir";
  public static final String METRICS_ENABLED_KEY = "metrics.enabled";
  public static final String DEFAULT_METRICS_ENABLED = Boolean.toString(false);
  public static final String METRICS_REPORTING_FILE_ENABLED_KEY = "metrics.reporting.file.enabled";
  public static final String DEFAULT_METRICS_REPORTING_FILE_ENABLED = Boolean.toString(true);
  public static final String METRICS_REPORTING_JMX_ENABLED_KEY = "metrics.reporting.jmx.enabled";
  public static final String DEFAULT_METRICS_REPORTING_JMX_ENABLED = Boolean.toString(false);
  public static final String METRICS_REPORT_INTERVAL_KEY = "metrics.report.interval";
  public static final String DEFAULT_METRICS_REPORT_INTERVAL = "30000";

  /**
   * FluxDB metrics store configuration properties.
   */
  public static final String FLUXDB_URL_KEY = "fluxdb.url";
  public static final String FLUXDB_USER_NAME_KEY = "fluxdb.user.name";
  public static final String DEFAULT_FLUXDB_USER_NAME = "root";
  public static final String FLUXDB_USER_PASSWORD_KEY = "fluxdb.user.password";
  public static final String DEFAULT_FLUXDB_USER_PASSWORD = "root";

  /**
   * Rest server configuration properties.
   */
  public static final String REST_SERVER_HOST_KEY = "rest.server.host";
  public static final String DEFAULT_REST_SERVER_HOST = "localhost";
  public static final String REST_SERVER_PORT_KEY = "rest.server.port";
  public static final String DEFAULT_REST_SERVER_PORT = "8080";

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
   * Other configuration properties.
   */
  public static final String DEFAULT_CHARSET_ENCODING = "UTF-8";
}
