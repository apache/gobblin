package com.linkedin.uif.writer.hdfs;

import java.io.IOException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.writer.DataWriter;
import com.linkedin.uif.writer.DataWriterBuilder;
import com.linkedin.uif.writer.Destination;
import com.linkedin.uif.writer.TestConstants;
import com.linkedin.uif.writer.conf.WriterConfig;
import com.linkedin.uif.writer.schema.SchemaType;

/**
 * Unit tests for {@link HdfsDataWriter}.
 */
@Test(groups = {"com.linkedin.uif.writer.hdfs"})
public class HdfsDataWriterTest {

    private DataWriter writer;

    @BeforeClass
    public void setUp() throws IOException {
        Properties properties = new Properties();
        properties.setProperty(WriterConfig.BUFFER_SIZE_KEY,
                WriterConfig.DEFAULT_BUFFER_SIZE);
        properties.setProperty(WriterConfig.FILE_SYSTEM_URI_KEY, TestConstants.TEST_FS_URI);
        properties.setProperty(WriterConfig.STAGING_DIR_KEY, TestConstants.TEST_STAGING_DIR);
        properties.setProperty(WriterConfig.OUTPUT_DIR_KEY, TestConstants.TEST_OUTPUT_DIR);
        properties.setProperty(WriterConfig.FILE_NAME_KEY, TestConstants.TEST_FILE_NAME);

        this.writer = DataWriterBuilder.newBuilder()
                .writeTo(Destination.of(Destination.DestinationType.HDFS, properties))
                .writerId("writer-1")
                .dataName("test")
                .dataStartTime("20140131")
                .dataEndTime("20140201")
                .dataSchema(TestConstants.AVRO_SCHEMA, SchemaType.AVRO)
                .totalCount(3)
                .build();
    }

    @Test
    public void testWrite() throws IOException {
        for (String record : TestConstants.JSON_RECORDS) {

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
