package com.linkedin.uif.configuration;

/**
 * A central place for all UIF configuration property keys.
 */
public class ConfigurationKeys {

    // Directory where all job configuration files are stored
    public static final String JOB_CONFIG_FILE_DIR_KEY = "jobconf.dir";

    // Root directory where task state files are stored
    public static final String TASK_STATE_STORE_ROOT_DIR_KEY = "taskstate.store.dir";

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
    public static final String WORK_UNIT_NAMESPACE_KEY = "workunit.namespace";
    public static final String WORK_UNIT_TABLE_KEY = "workunit.table";

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
    public static final String TASK_RETRY_INTERVAL_KEY = "task.retry.interval";
    public static final String DEFAULT_TASK_RETRY_INTERVAL = "300";
    public static final String JOB_ID_KEY = "job.id";
    public static final String TASK_ID_KEY = "task.id";
    public static final String TASK_STATUS_REPORT_INTERVAL_KEY =
            "task.status.reportinterval";
    public static final long DEFAULT_TASK_STATUS_REPORT_INTERVAL = 30000;
}
