package com.linkedin.uif.writer;

/**
 * Test constants.
 */
public class TestConstants {

    // Test Avro schema
    public static final String AVRO_SCHEMA =
            "{\"namespace\": \"example.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"User\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"name\", \"type\": \"string\"},\n" +
            "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
            "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
            " ]\n" +
            "}";

    // Test Avro data in json format
    public static final String[] JSON_RECORDS = {
        "{\"fields\": {\"name\": \"Alyssa\", \"favorite_number\": 256, \"favorite_color\": null}}",
        "{\"fields\": {\"name\": \"Ben\", \"favorite_number\": 7, \"favorite_color\": \"red\"}}",
        "{\"fields\": {\"name\": \"Charlie\", \"favorite_number\": null, \"favorite_color\": \"blue\"}}"
    };

    public static final String TEST_FS_URI = "file://localhost/";

    public static final String TEST_STAGING_DIR = "test-staging";

    public static final String TEST_OUTPUT_DIR = "test-output";

    public static final String TEST_FILE_NAME = "test.avro";
}
