package com.linkedin.ufi.test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.source.extractor.DataRecordException;
import com.linkedin.uif.source.extractor.Extractor;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.test.TestExtractor;
import com.linkedin.uif.writer.DataWriter;
import com.linkedin.uif.writer.DataWriterBuilderFactory;
import com.linkedin.uif.writer.Destination;
import com.linkedin.uif.writer.TestDataConverter;
import com.linkedin.uif.writer.TestSchemaConverter;
import com.linkedin.uif.writer.WriterOutputFormat;
import com.linkedin.uif.writer.converter.SchemaConverter;

public class ExtractorToWriterTest
{

    private static final String SOURCE_FILE_KEY = "source.file";
    private static final String SOURCE_FILES = "test/resource/source/test.avro.0";
    
    private Schema schema;
    private DataWriter<String, GenericRecord> writer;
    private Extractor<String, String> extractor;
    private FileSystem fs;
    
    @SuppressWarnings("unchecked")
    @BeforeClass
    public void setUp() throws SchemaConversionException, IOException, URISyntaxException, DataRecordException {
        WorkUnit workUnit = new WorkUnit(null, null);
        workUnit.setProp(SOURCE_FILE_KEY, SOURCE_FILES);
        WorkUnitState workUnitState = new WorkUnitState(workUnit);
        
        this.extractor = new TestExtractor(workUnitState);
        String schema = extractor.getSchema();
        
        File stagingDir = new File(TestConstants.TEST_STAGING_DIR);
        File outputDir = new File(TestConstants.TEST_OUTPUT_DIR);
        
        if (!stagingDir.exists()) {
            stagingDir.mkdirs();
        }
        
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        this.fs = FileSystem.get(new URI(TestConstants.TEST_FS_URI), new Configuration());
        
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
        this.schema = schemaConverter.convert(schema);

        // Build a writer to write test records
        this.writer = new DataWriterBuilderFactory().newDataWriterBuilder(
                WriterOutputFormat.AVRO)
                .writeTo(Destination.of(Destination.DestinationType.HDFS, properties))
                .writeInFormat(WriterOutputFormat.AVRO)
                .withWriterId(TestConstants.TEST_WRITER_ID)
                .useDataConverter(new TestDataConverter(
                        schemaConverter.convert(schema)))
                .useSchemaConverter(new TestSchemaConverter())
                .withSourceSchema(schema)
                .build();
        
        String record;
        while ((record = this.extractor.readRecord()) != null) {
            this.writer.write(record);
        }
        
        this.writer.close();
        this.writer.commit();
    }
    
    @Test
    public void checkFileEquality() throws IOException {
        Assert.assertEquals(fs.getFileChecksum(new Path(SOURCE_FILES)), fs.getFileChecksum(new Path(TestConstants.TEST_OUTPUT_DIR)));
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
