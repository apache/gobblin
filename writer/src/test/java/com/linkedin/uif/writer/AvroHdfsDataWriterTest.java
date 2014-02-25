package com.linkedin.uif.writer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import org.apache.hadoop.fs.FileUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.writer.converter.SchemaConverter;
import com.linkedin.uif.writer.schema.SchemaType;

/**
 * Unit tests for {@link AvroHdfsDataWriter}.
 *
 * @author ynli
 */
@Test(groups = {"com.linkedin.uif.writer"})
public class AvroHdfsDataWriterTest {

    private Schema schema;
    private DataWriter<String, GenericRecord> writer;

    @BeforeClass
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        // Making the staging and/or output dirs if necessary
        File stagingDir = new File(TestConstants.TEST_STAGING_DIR);
        File outputDir = new File(TestConstants.TEST_OUTPUT_DIR);
        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        }
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        Properties properties = new Properties();
        properties.setProperty(ConfigurationKeys.BUFFER_SIZE_KEY,
                ConfigurationKeys.DEFAULT_BUFFER_SIZE);
        properties.setProperty(ConfigurationKeys.FILE_SYSTEM_URI_KEY,
                TestConstants.TEST_FS_URI);
        properties.setProperty(ConfigurationKeys.STAGING_DIR_KEY,
                TestConstants.TEST_STAGING_DIR);
        properties.setProperty(ConfigurationKeys.OUTPUT_DIR_KEY,
                TestConstants.TEST_OUTPUT_DIR);
        properties.setProperty(ConfigurationKeys.FILE_NAME_KEY,
                TestConstants.TEST_FILE_NAME);

        SchemaConverter<String, Schema> schemaConverter = new TestSchemaConverter();
        this.schema = schemaConverter.convert(TestConstants.AVRO_SCHEMA);

        // Build a writer to write test records
        this.writer = new DataWriterBuilderFactory().newDataWriterBuilder(
                WriterOutputFormat.AVRO)
                .writeTo(Destination.of(Destination.DestinationType.HDFS, properties))
                .writeInFormat(WriterOutputFormat.AVRO)
                .withWriterId(TestConstants.TEST_WRITER_ID)
                .useDataConverter(new TestDataConverter(
                        schemaConverter.convert(TestConstants.AVRO_SCHEMA)))
                .useSchemaConverter(new TestSchemaConverter())
                .withSourceSchema(TestConstants.AVRO_SCHEMA, SchemaType.AVRO)
                .build();
    }

    @Test
    public void testWrite() throws IOException {
        // Write all test records
        for (String record : TestConstants.JSON_RECORDS) {
            this.writer.write(record);
        }

        Assert.assertEquals(this.writer.recordsWritten(), 3);

        this.writer.close();
        this.writer.commit();

        File outputFile = new File(TestConstants.TEST_OUTPUT_DIR,
                TestConstants.TEST_FILE_NAME + "." + TestConstants.TEST_WRITER_ID);
        DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(
                outputFile, new GenericDatumReader<GenericRecord>(this.schema));

        // Read the records back and assert they are identical to the ones written
        GenericRecord user1 = reader.next();
        // Strings are in UTF8, so we have to call toString() here and below
        Assert.assertEquals(user1.get("name").toString(), "Alyssa");
        Assert.assertEquals(user1.get("favorite_number"), 256);
        Assert.assertEquals(user1.get("favorite_color").toString(), "yellow");

        GenericRecord user2 = reader.next();
        Assert.assertEquals(user2.get("name").toString(), "Ben");
        Assert.assertEquals(user2.get("favorite_number"), 7);
        Assert.assertEquals(user2.get("favorite_color").toString(), "red");

        GenericRecord user3 = reader.next();
        Assert.assertEquals(user3.get("name").toString(), "Charlie");
        Assert.assertEquals(user3.get("favorite_number"), 68);
        Assert.assertEquals(user3.get("favorite_color").toString(), "blue");

        reader.close();
    }

    @AfterClass
    public void tearDown() throws IOException {
        // Clean up the staging and/or output directories if necessary
        File stagingDir = new File(TestConstants.TEST_STAGING_DIR);
        File outputDir = new File(TestConstants.TEST_OUTPUT_DIR);
        if (stagingDir.exists()) {
            FileUtil.fullyDelete(stagingDir);
        }
        if (outputDir.exists()) {
            FileUtil.fullyDelete(outputDir);
        }
    }
}
