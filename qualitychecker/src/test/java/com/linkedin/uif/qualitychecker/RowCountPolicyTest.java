package com.linkedin.uif.qualitychecker;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.test.TestExtractor;
import com.linkedin.uif.writer.DataWriter;
import com.linkedin.uif.writer.DataWriterBuilderFactory;
import com.linkedin.uif.writer.Destination;
import com.linkedin.uif.writer.TestDataConverter;
import com.linkedin.uif.writer.TestSchemaConverter;
import com.linkedin.uif.writer.WriterOutputFormat;
import com.linkedin.uif.writer.converter.SchemaConverter;
import com.linkedin.uif.writer.schema.SchemaType;

@Test(groups = {"com.linkedin.uif.qualitychecker"})
public class RowCountPolicyTest
{
    
    private static final String SOURCE_FILE_KEY = "source.file";
    private static final String SOURCE_FILES = "test/resource/source/test.avro.0";
    
    private Schema schema;
    private DataWriter writer;
    
    @BeforeClass
    public void setUp() {
        
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testPolicy() throws Exception {
        WorkUnit workUnit = new WorkUnit(null, null);
        workUnit.setProp(SOURCE_FILE_KEY, SOURCE_FILES);
        WorkUnitState workUnitState = new WorkUnitState(workUnit);
        
        TestExtractor extractor = new TestExtractor(workUnitState);
        String schema = extractor.getSchema();
        
        File stagingDir = new File(TestConstants.TEST_STAGING_DIR);
        File outputDir = new File(TestConstants.TEST_OUTPUT_DIR);
        
        for (File f : stagingDir.listFiles()) { f.delete(); }
        stagingDir.delete();
        stagingDir.mkdirs();
        
        for (File f : outputDir.listFiles()) { f.delete(); }
        outputDir.delete();
        outputDir.mkdirs();

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
                .withSourceSchema(schema, SchemaType.AVRO)
                .build();
        
        Object record;
        while ((record = extractor.readRecord()) != null) {
            writer.write(record);
        }
        
        writer.close();
        writer.commit();
        
        State state = new State();
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST, "com.linkedin.uif.qualitychecker.RowCountPolicy");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.POLICY_LIST_TYPE, "MANDATORY");
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.EXTRACTOR_ROWS_READ, extractor.getExpectedRecordCount());
        state.setProp(ConfigurationKeys.QUALITY_CHECKER_PREFIX + ConfigurationKeys.WRITER_ROWS_WRITTEN, writer.recordsWritten());
        System.out.println("Extractor: " + extractor.getExpectedRecordCount());
        System.out.println("Writer: " + writer.recordsWritten());
        PolicyChecker checker = new PolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state, null).build();
        PolicyCheckResults results = checker.executePolicies();
        for (Map.Entry<QualityCheckResult, Policy.Type> entry : results.getPolicyResults().entrySet()) {
            Assert.assertEquals(entry.getKey(), QualityCheckResult.PASSED);
        }
    }
}
