package com.linkedin.uif.writer;

import java.io.IOException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.uif.writer.conf.WriterConfig;
import com.linkedin.uif.writer.hdfs.HdfsDataWriter;
import com.linkedin.uif.writer.schema.SchemaType;

/**
 * Unit tests for {@link DataWriterBuilder}.
 */
@Test(groups = {"com.linkedin.uif.writer"})
public class DataWriterBuilderTest {

    @Test
    public void testBuild() throws IOException {
        Properties properties = new Properties();
        properties.setProperty(WriterConfig.BUFFER_SIZE_KEY,
                WriterConfig.DEFAULT_BUFFER_SIZE);
        properties.setProperty(WriterConfig.FILE_SYSTEM_URI_KEY, TestConstants.TEST_FS_URI);
        properties.setProperty(WriterConfig.STAGING_DIR_KEY, TestConstants.TEST_STAGING_DIR);
        properties.setProperty(WriterConfig.OUTPUT_DIR_KEY, TestConstants.TEST_OUTPUT_DIR);
        properties.setProperty(WriterConfig.FILE_NAME_KEY, TestConstants.TEST_FILE_NAME);

        DataWriter writer = DataWriterBuilder.newBuilder()
                .writeTo(Destination.of(Destination.DestinationType.HDFS, properties))
                .writerId("writer-1")
                .dataName("test")
                .dataStartTime("20140131")
                .dataEndTime("20140201")
                .dataSchema(TestConstants.AVRO_SCHEMA, SchemaType.AVRO)
                .totalCount(1)
                .build();

        Assert.assertTrue(writer instanceof HdfsDataWriter);

        writer.close();
    }
}
