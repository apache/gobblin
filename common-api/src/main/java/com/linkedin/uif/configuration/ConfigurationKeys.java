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

    /**
     * Common job configuraion properties
     */
    public static final String JOB_NAME_KEY = "job.name";
    public static final String JOB_GROUP_KEY = "job.group";
    public static final String JOB_DESCRIPTION_KEY = "job.description";
    public static final String JOB_SCHEDULE_KEY = "job.schedule";
    public static final String SOURCE_CLASS_KEY = "source.class";
    public static final String CONVERTER_CLASSES_KEY = "converter.classes";
    public static final String SOURCE_SCHEMA_TYPE_KEY = "source.schema.type";

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
    public static final String WRITER_DESTINATION_CONFIG_KEY_PREFIX = "writer";
    public static final String WRITER_DESTINATION_TYPE_KEY =
            WRITER_DESTINATION_CONFIG_KEY_PREFIX + ".destination.type";
    public static final String WRITER_OUTPUT_FORMAT_KEY = ".output.format";

    /**
     * HDFS writer configuration properties
     */
    public static final String FILE_SYSTEM_URI_KEY =
            WRITER_DESTINATION_CONFIG_KEY_PREFIX + ".fs.uri";
    public static final String STAGING_DIR_KEY =
            WRITER_DESTINATION_CONFIG_KEY_PREFIX + ".staging.dir";
    public static final String OUTPUT_DIR_KEY =
            WRITER_DESTINATION_CONFIG_KEY_PREFIX + ".output.dir";
    public static final String FILE_NAME_KEY =
            WRITER_DESTINATION_CONFIG_KEY_PREFIX + ".file.name";
    public static final String BUFFER_SIZE_KEY =
            WRITER_DESTINATION_CONFIG_KEY_PREFIX + ".buffer.size";
    public static final String DEFAULT_STAGING_DIR = "";
    public static final String DEFAULT_OUTPUT_DIR = "";
    public static final String DEFAULT_BUFFER_SIZE = "4096";

    /**
     * Job publisher properties
     */
    public static final String JOB_PUBLISHER_PREFIX = "publisher.job";
    
    /**
     * HDFS task publisher properties
     */
    public static final String JOB_FINAL_DIR_HDFS = JOB_PUBLISHER_PREFIX + ".final.dir";
    
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
    
    /**
     * Configuration properties used by the quality checker
     */
    public static final String QUALITY_CHECKER_PREFIX = "qualitychecker";
    public static final String POLICY_LIST = ".policies";
    public static final String POLICY_LIST_TYPE = ".policy.types";
    public static final String TASK_DATA_PUBLISHER_TYPE = ".taskpublisher.type";
    public static final String METADATA_CLIENT = ".metadatacollector";
    public static final String EXTRACTOR_ROWS_READ = ".rows.read";
    public static final String WRITER_ROWS_WRITTEN = ".rows.written";
    public static final String ROW_COUNT_RANGE = ".row.count.range";
    public static final String TASK_STATUS_REPORT_INTERVAL_IN_MS_KEY =
            "task.status.reportintervalinms";
    public static final long DEFAULT_TASK_STATUS_REPORT_INTERVAL_IN_MS = 30000;
}
