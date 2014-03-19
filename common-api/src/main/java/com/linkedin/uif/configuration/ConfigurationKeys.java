package com.linkedin.uif.configuration;

/**
 * A central place for all UIF configuration property keys.
 */
public class ConfigurationKeys {

    // Directory where all job configuration files are stored
    public static final String JOB_CONFIG_FILE_DIR_KEY = "jobconf.dir";

    // Root directory where task state files are stored
    public static final String STATE_STORE_ROOT_DIR_KEY = "state.store.dir";

    // File system URI for file-system-based task store
    public static final String STATE_STORE_FS_URI_KEY = "state.store.fs.uri";

    // Directory where metrics csv files are stored
    public static final String METRICS_DIR_KEY = "metrics.dir";

    /**
     * Common job configuraion properties
     */
    public static final String JOB_NAME_KEY = "job.name";
    public static final String JOB_GROUP_KEY = "job.group";
    public static final String JOB_DESCRIPTION_KEY = "job.description";
    public static final String JOB_SCHEDULE_KEY = "job.schedule";
    public static final String SOURCE_CLASS_KEY = "source.class";
    public static final String SOURCE_WRAPPER_CLASS_KEY = "source.wrapper.class";
    public static final String CONVERTER_CLASSES_KEY = "converter.classes";
    public static final String SOURCE_SCHEMA_TYPE_KEY = "source.schema.type";
    public static final String JOB_COMMIT_POLICY_KEY = "job.commit.policy";
    public static final String DEFAULT_JOB_COMMIT_POLICY = "full";
    public static final String JOB_RUN_ONCE_KEY = "job.runonce";

    /**
     * Work unit related configuration properties
     */
    public static final String WORK_UNIT_LOW_WATER_MARK_KEY = "workunit.low.water.mark";
    public static final String WORK_UNIT_HIGH_WATER_MARK_KEY = "workunit.high.water.mark";

    /**
     * Work unit runtime state related configuration properties
     */
    public static final String WORK_UNIT_WORKING_STATE_KEY = "workunit.working.state";
    public static final String WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK = "workunit.state.runtime.high.water.mark";

    /**
     * Extract related configuration properties
     */
    public static final String EXTRACT_TABLE_TYPE_KEY = "extract.table.type";
    public static final String EXTRACT_NAMESPACE_NAME_KEY = "extract.namespace";
    public static final String EXTRACT_TABLE_NAME_KEY = "extract.table.name";
    public static final String EXTRACT_EXTRACT_ID_KEY = "extract.extract.id";
    public static final String EXTRACT_IS_FULL_KEY = "extract.is.full";
    public static final String EXTRACT_FULL_RUN_TIME_KEY = "extract.full.run.time";
    public static final String EXTRACT_RECORD_COUNT_KEY = "extract.record.count";
    public static final String EXTRACT_RECORD_COUNT_ESTIMATED_KEY = "extract.record.count.estimated";
    public static final String EXTRACT_VALIDATION_RECORD_COUNT_KEY = "extract.validation.record.count";
    public static final String EXTRACT_VALIDATION_RECORD_COUNT_HWM_KEY = "extract.validation.record.count.high.water.mark";
    public static final String EXTRACT_IS_SHARDED_KEY = "extract.is.sharded";
    public static final String EXTRACT_SHARDED_REAL_NAMESPACE_KEY = "extract.sharded.real.namespace";
    public static final String EXTRACT_IS_SECURED_KEY = "extract.is.secured";
    public static final String EXTRACT_SECURITY_PERMISSION_GROUP_KEY = "extract.security.permission.group";
    public static final String EXTRACT_PRIMARY_KEY_FIELDS_KEY = "extract.primary.key.fields";
    public static final String EXTRACT_DELTA_FIELDS_KEY = "extract.delta.fields";
    public static final String EXTRACT_SCHEMA = "extract.schema";

    /**
     * Converter configuration properties
     */
    public static final String CONVERTER_AVRO_DATE_FORMAT = "converter.avro.date.format";
    public static final String CONVERTER_AVRO_DATE_TIMEZONE = "converter.avro.date.timezone";
    public static final String CONVERTER_AVRO_TIME_FORMAT = "converter.avro.time.format";
    public static final String CONVERTER_AVRO_TIMESTAMP_FORMAT = "converter.avro.timestamp.format";
    public static final String CONVERTER_AVRO_BINARY_CHARSET = "converter.avro.binary.charset";

    /**
     * Writer configuration properties
     */
    public static final String WRITER_PREFIX = "writer";
    public static final String WRITER_DESTINATION_TYPE_KEY =
            WRITER_PREFIX + ".destination.type";
    public static final String WRITER_OUTPUT_FORMAT_KEY = WRITER_PREFIX + ".output.format";
    public static final String WRITER_FILE_EXTENSION = WRITER_PREFIX + ".file.extension";

    /**
     * HDFS writer configuration properties
     */
    public static final String WRITER_FILE_SYSTEM_URI =
            WRITER_PREFIX + ".fs.uri";
    public static final String WRITER_STAGING_DIR =
            WRITER_PREFIX + ".staging.dir";
    public static final String WRITER_OUTPUT_DIR =
            WRITER_PREFIX + ".output.dir";
    public static final String WRITER_FILE_NAME =
            WRITER_PREFIX + ".file.name";
    public static final String WRITER_BUFFER_SIZE =
            WRITER_PREFIX + ".buffer.size";
    public static final String DEFAULT_STAGING_DIR = "";
    public static final String DEFAULT_OUTPUT_DIR = "";
    public static final String DEFAULT_BUFFER_SIZE = "4096";
    
    /**
     * Configuration properties used internally
     */
    public static final String TASK_EXECUTOR_THREADPOOL_SIZE_KEY =
            "taskexecutor.threadpool.size";
    public static final String TASK_STATE_TRACKER_THREAD_POOL_CORE_SIZE_KEY =
            "tasktracker.threadpool.coresize";
    public static final String TASK_STATE_TRACKER_THREAD_POOL_MAX_SIZE_KEY =
            "tasktracker.threadpool.maxsize";
    public static final String TASK_RETRY_THREAD_POOL_CORE_SIZE_KEY =
            "taskretry.threadpool.coresize";
    public static final String TASK_RETRY_THREAD_POOL_MAX_SIZE_KEY =
            "taskretry.threadpool.maxsize";
    public static final String DEFAULT_TASK_SCHEDULER_THREADPOOL_SIZE = "10";
    public static final String DEFAULT_TASK_STATE_TRACKER_THREAD_POOL_CORE_SIZE = "10";
    public static final String DEFAULT_TASK_STATE_TRACKER_THREAD_POOL_MAX_SIZE = "10";
    public static final String DEFAULT_TASK_RETRY_THREAD_POOL_CORE_SIZE =
            "2";
    public static final String DEFAULT_TASK_RETRY_THREAD_POOL_MAX_SIZE =
            "2";
    public static final String MAX_TASK_RETRIES_KEY = "task.maxretries";
    public static final String DEFAULT_MAX_TASK_RETRIES = "5";
    public static final String TASK_RETRY_INTERVAL_IN_SEC_KEY =
            "task.retry.intervalinsec";
    public static final String DEFAULT_TASK_RETRY_INTERVAL_IN_SEC = "300";
    public static final String JOB_ID_KEY = "job.id";
    public static final String TASK_ID_KEY = "task.id";
    public static final String JOB_CONFIG_FILE_PATH_KEY = "job.config.path";
    public static final String SOURCE_WRAPPERS = "source.wrappers";
    public static final String DEFAULT_SOURCE_WRAPPER = "default";
    public static final String METRICS_ENABLED_KEY = "metrics.enabled";
    public static final String DEFAULT_METRICS_ENABLED = "true";
    public static final String METRICS_REPORT_INTERVAL_KEY = "metrics.report.interval";
    public static final String DEFAULT_METRICS_REPORT_INTERVAL = "60000";

    /**
     * Configuration properties used by the quality checker
     */
    public static final String QUALITY_CHECKER_PREFIX = "qualitychecker";
    public static final String POLICY_LIST = QUALITY_CHECKER_PREFIX + ".policies";
    public static final String POLICY_LIST_TYPE = QUALITY_CHECKER_PREFIX + ".policy.types";
    
    /**
     * Configuration properties used by the row count policies
     */
    public static final String EXTRACTOR_ROWS_READ = QUALITY_CHECKER_PREFIX + ".rows.read";
    public static final String WRITER_ROWS_WRITTEN = QUALITY_CHECKER_PREFIX + ".rows.written";
    public static final String ROW_COUNT_RANGE = QUALITY_CHECKER_PREFIX + ".row.count.range";
    
    /**
     * Configuration properties for the task status
     */
    public static final String TASK_STATUS_REPORT_INTERVAL_IN_MS_KEY =
            "task.status.reportintervalinms";
    public static final long DEFAULT_TASK_STATUS_REPORT_INTERVAL_IN_MS = 30000;
    
    /**
     * Configuration properties for the metadata client
     */
    public static final String METADATA_CLIENT = "metadataclient";
    
    /**
     * Configurations properties for the schema retriever
     */
    public static final String SCHEMA_RETRIEVER_PREFIX = "schema.retriever";
    public static final String SCHEMA_RETRIEVER_TYPE = SCHEMA_RETRIEVER_PREFIX + ".type";
    
    /**
     * Configuration properties for the data publisher
     */
    public static final String DATA_PUBLISHER_PREFIX = "data.publisher";
    public static final String DATA_PUBLISHER_TYPE = DATA_PUBLISHER_PREFIX + ".type";
    public static final String DATA_PUBLISHER_TMP_DIR = DATA_PUBLISHER_PREFIX + ".tmp.dir";
    public static final String DATA_PUBLISHER_FINAL_DIR = DATA_PUBLISHER_PREFIX + ".final.dir";
    public static final String DATA_PUBLISHER_REPLACE_FINAL_DIR =  DATA_PUBLISHER_PREFIX + ".replace.final.dir";
    
    /** 
    * Configuration properties used by the extractor
    */
    public static final String SOURCE_SCHEMA = "source.schema";
    public static final String SOURCE_ENTITY = "source.entity";
    public static final String SOURCE_WATERMARK_TYPE = "source.watermark.type";
    public static final String SOURCE_HOUR_COLUMN = "source.hour.column";
    public static final String SOURCE_SKIP_HIGH_WATERMARK_CALC = "source.skip.high.watermark.calc";
    public static final String SOURCE_QUERY = "source.query";
    public static final String SOURCE_IS_HOURLY_EXTRACT = "source.hourly.extract";
    public static final String SOURCE_EXTRACT_TYPE = "source.extract.type";
    public static final String SOURCE_TIMEZONE = "source.timezone";
    public static final String SOURCE_PARTITION_INTERVAL = "source.partition.interval";
    public static final String SOURCE_START_VALUE = "source.start.value";
    public static final String SOURCE_END_VALUE = "source.end.value";
    public static final String SOURCE_APPEND_MAX_WATERMARK_LIMIT = "source.append.max.watermark.limit";
    public static final String SOURCE_IS_WATERMARK_OVERRIDE = "source.is.watermark.override";
    public static final String SOURCE_MAX_NUMBER_OF_PARTITIONS = "source.max.number.of.partitions";
    public static final String SOURCE_LOW_WATERMARK_BACKUP_SECS = "source.low.watermark.backup.secs";
    
    
    public static final String SOURCE_CLIENT_ID = "source.client.id";
    public static final String SOURCE_CLIENT_SECRET = "source.client.secret";
    public static final String SOURCE_USERNAME = "source.username";
    public static final String SOURCE_PASSWORD = "source.password";
    public static final String SOURCE_SECURITY_TOKEN = "source.security.token";
    public static final String SOURCE_HOST_NAME = "source.host";
    public static final String SOURCE_VERSION = "source.version";
    public static final String SOURCE_TIMEOUT = "source.timeout";
    
    public static final long DEFAULT_WATERMARK_VALUE = -1;
    public static final int DEFAULT_MAX_NUMBER_OF_PARTITIONS = 20;
}
