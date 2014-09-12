package com.linkedin.uif.runtime;

import java.io.StringReader;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.test.TestSource;
import com.linkedin.uif.writer.Destination;
import com.linkedin.uif.writer.WriterOutputFormat;
import com.linkedin.uif.writer.converter.DataConverter;
import com.linkedin.uif.writer.converter.SchemaConverter;

/**
 * Unit tests for {@link TaskContext}.
 *
 * @author ynli
 */
@Test(groups = {"com.linkedin.uif.runtime"})
public class TaskContextTest {

    private static final String TEST_JOB_CONFIG =
            "job.name=UIFTest1\n" +
            "job.group=Test\n" +
            "job.description=Test UIF job 1\n" +
            "job.schedule=0 0/1 * * * ?\n" +
            "source.class=com.linkedin.uif.test.TestSource\n" +
            "converter.classes=com.linkedin.uif.test.TestConverter\n" +
            "workunit.namespace=test\n" +
            "workunit.table=test\n" +
            "writer.destination.type=HDFS\n" +
            "writer.output.format=AVRO\n" +
            "writer.fs.uri=file://localhost/\n" +
            "writer.staging.dir=test/staging\n" +
            "writer.output.dir=test/output\n" +
            "writer.file.name=test.avro";

    private static final String TEST_SCHEMA =
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
    private static final String TEST_DATA_RECORD =
            "{" +
                    "\"name\": \"Alyssa\", " +
                    "\"favorite_number\": 256, " +
                    "\"favorite_color\": \"yellow\"" +
            "}";

    private TaskContext taskContext;

    @BeforeClass
    public void setUp() throws Exception {
        WorkUnit workUnit = new WorkUnit(null, null);
        Properties properties = new Properties();
        properties.load(new StringReader(TEST_JOB_CONFIG));
        workUnit.addAll(properties);
        this.taskContext = new TaskContext(new WorkUnitState(workUnit));
    }

    @Test
    public void testConverters() throws Exception {
        // Test schema converter
        SchemaConverter schemaConverter = this.taskContext.getSchemaConverter();
        Schema schema = (Schema) schemaConverter.convert(TEST_SCHEMA);

        Assert.assertEquals(schema.getNamespace(), "example.avro");
        Assert.assertEquals(schema.getType(), Schema.Type.RECORD);
        Assert.assertEquals(schema.getName(), "User");
        Assert.assertEquals(schema.getFields().size(), 3);

        Schema.Field nameField = schema.getField("name");
        Assert.assertEquals(nameField.name(), "name");
        Assert.assertEquals(nameField.schema().getType(), Schema.Type.STRING);

        Schema.Field favNumberField = schema.getField("favorite_number");
        Assert.assertEquals(favNumberField.name(), "favorite_number");
        Assert.assertEquals(favNumberField.schema().getType(), Schema.Type.INT);

        Schema.Field favColorField = schema.getField("favorite_color");
        Assert.assertEquals(favColorField.name(), "favorite_color");
        Assert.assertEquals(favColorField.schema().getType(), Schema.Type.STRING);

        // Test data converter
        DataConverter dataConverter = this.taskContext.getDataConverter(TEST_SCHEMA);
        GenericRecord record = (GenericRecord) dataConverter.convert(TEST_DATA_RECORD);

        Assert.assertEquals(record.get("name"), "Alyssa");
        Assert.assertEquals(record.get("favorite_number"), 256d);
        Assert.assertEquals(record.get("favorite_color"), "yellow");
    }

    @Test
    public void testOtherMethods() {
        Assert.assertTrue(this.taskContext.getSource() instanceof TestSource);
        Assert.assertEquals(this.taskContext.getStatusReportingInterval(),
                ConfigurationKeys.DEFAULT_TASK_STATUS_REPORT_INTERVAL_IN_MS);
        Assert.assertEquals(
                this.taskContext.getDestinationType(),
                Destination.DestinationType.HDFS);
        Assert.assertEquals(
                this.taskContext.getWriterOutputFormat(),
                WriterOutputFormat.AVRO);
        Assert.assertTrue(this.taskContext.getConverters().isEmpty());
    }
}
