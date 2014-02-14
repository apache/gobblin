package com.linkedin.uif.writer;

/**
 * Test constants.
 *
 * @author ynli
 */
public class TestConstants {

    // Test Avro schema
    public static final String AVRO_SCHEMA =
            "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n" +
            "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n" +
            " ]\n" +
            "}";

    // Test Avro data in json format
    public static final String[] JSON_RECORDS = {
        "{\"fields\": {\"name\": \"Alyssa\", \"favorite_number\": 256, \"favorite_color\": \"yellow\"}}",
        "{\"fields\": {\"name\": \"Ben\", \"favorite_number\": 7, \"favorite_color\": \"red\"}}",
        "{\"fields\": {\"name\": \"Charlie\", \"favorite_number\": 68, \"favorite_color\": \"blue\"}}"
    };

    public static final String TEST_FS_URI = "file://localhost/";

    public static final String TEST_STAGING_DIR = "test-staging";

    public static final String TEST_OUTPUT_DIR = "test-output";

    public static final String TEST_FILE_NAME = "test.avro";

    public static final String TEST_WRITER_ID = "writer-1";
}
