package com.linkedin.uif.runtime;

import java.io.StringReader;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.test.TestSource;
import com.linkedin.uif.writer.Destination;
import com.linkedin.uif.writer.WriterOutputFormat;

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
            "workunit.namespace=test\n" +
            "workunit.table=test\n" +
            "writer.destination.type=HDFS\n" +
            "writer.output.format=AVRO\n" +
            "writer.fs.uri=file://localhost/\n" +
            "writer.staging.dir=test/staging\n" +
            "writer.output.dir=test/output\n" +
            "writer.file.name=test.avro";

    private TaskContext taskContext;

    @BeforeClass
    public void setUp() throws Exception {
        WorkUnit workUnit = new WorkUnit();
        Properties properties = new Properties();
        properties.load(new StringReader(TEST_JOB_CONFIG));
        workUnit.addAll(properties);
        this.taskContext = new TaskContext(new WorkUnitState(workUnit));
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
