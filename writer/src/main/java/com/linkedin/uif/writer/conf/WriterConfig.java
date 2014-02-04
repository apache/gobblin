package com.linkedin.uif.writer.conf;

/**
 *
 */
public class WriterConfig {

    /**
     * HDFS writer configuration properties
     */
    public static final String FILE_SYSTEM_URI_KEY = "uif.writer.fs.uri";
    public static final String STAGING_DIR_KEY = "uif.writer.staging.dir";
    public static final String OUTPUT_DIR_KEY = "uif.writer.output.dir";
    public static final String FILE_NAME_KEY = "uif.writer.file.name";
    public static final String BUFFER_SIZE_KEY = "uif.writer.buffer.size";
    public static final String DEFAULT_STAGING_DIR = "";
    public static final String DEFAULT_OUTPUT_DIR = "";
    public static final String DEFAULT_BUFFER_SIZE = "4096";

    /**
     * Kafka writer configuration properties
     */
}
