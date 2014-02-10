package com.linkedin.uif.writer;

import java.io.IOException;
import java.util.Properties;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.converter.SchemaConverter;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.writer.schema.SchemaType;

/**
 * Unit tests for {@link com.linkedin.uif.writer.HdfsDataWriter}.
 */
@Test(groups = {"com.linkedin.uif.writer"})
public class HdfsDataWriterTest {

    private DataWriter<String> writer;

    @BeforeClass
    public void setUp() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ConfigurationKeys.BUFFER_SIZE_KEY,
                ConfigurationKeys.DEFAULT_BUFFER_SIZE);
        properties.setProperty(ConfigurationKeys.FILE_SYSTEM_URI_KEY, TestConstants.TEST_FS_URI);
        properties.setProperty(ConfigurationKeys.STAGING_DIR_KEY, TestConstants.TEST_STAGING_DIR);
        properties.setProperty(ConfigurationKeys.OUTPUT_DIR_KEY, TestConstants.TEST_OUTPUT_DIR);
        properties.setProperty(ConfigurationKeys.FILE_NAME_KEY, TestConstants.TEST_FILE_NAME);

        SchemaConverter<String> schemaConverter = new TestSchemaConverter();

        this.writer = DataWriterBuilder.<String, String>newBuilder()
                .writeTo(Destination.of(Destination.DestinationType.HDFS, properties))
                .writerId("writer-1")
                .useDataConverter(new TestDataConverter(
                        schemaConverter.convert(TestConstants.AVRO_SCHEMA)))
                .useSchemaConverter(new TestSchemaConverter())
                .dataSchema(TestConstants.AVRO_SCHEMA, SchemaType.AVRO)
                .build();
    }

    @Test
    public void testWrite() throws IOException {
        for (String record : TestConstants.JSON_RECORDS) {
            this.writer.write(record);
        }
        Assert.assertEquals(this.writer.recordsWritten(), 3);

        this.writer.close();
        this.writer.commit();
    }

    @AfterClass
    public void tearDown() throws IOException {
        this.writer.close();
    }
}
