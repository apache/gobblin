package com.linkedin.uif.configuration;

/**
 * A central place for all UIF configuration property keys.
 */
public class ConfigurationKeys {

    /**
     * Converter configuration properties
     */
    public static final String DATA_CONVERTER_CLASS_KEY = "uif.data.converter.class";
    public static final String SCHEMA_CONVERTER_CLASS_KEY = "uif.schema.converter.class";

    public static final String SOURCE_SCHEMA_TYPE_KEY = "uif.source.schema.type";

    /**
     * Writer destination configuration properties
     */
    public static final String WRITER_DESTINATION_CONFIG_KEY_PREFIX = "uif.writer";
    public static final String WRITER_DESTINATION_TYPE_KEY =
            WRITER_DESTINATION_CONFIG_KEY_PREFIX + ".destination.type";

    /**
     * HDFS writer configuration properties
     */
    public static final String FILE_SYSTEM_URI_KEY = WRITER_DESTINATION_CONFIG_KEY_PREFIX + ".fs.uri";
    public static final String STAGING_DIR_KEY = WRITER_DESTINATION_CONFIG_KEY_PREFIX + ".staging.dir";
    public static final String OUTPUT_DIR_KEY = WRITER_DESTINATION_CONFIG_KEY_PREFIX + ".output.dir";
    public static final String FILE_NAME_KEY = WRITER_DESTINATION_CONFIG_KEY_PREFIX + ".file.name";
    public static final String BUFFER_SIZE_KEY = WRITER_DESTINATION_CONFIG_KEY_PREFIX + ".buffer.size";
    public static final String DEFAULT_STAGING_DIR = "";
    public static final String DEFAULT_OUTPUT_DIR = "";
    public static final String DEFAULT_BUFFER_SIZE = "4096";
}
