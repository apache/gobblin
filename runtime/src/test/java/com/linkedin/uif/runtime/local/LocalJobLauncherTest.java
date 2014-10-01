package com.linkedin.uif.runtime.local;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.metastore.FsStateStore;
import com.linkedin.uif.runtime.JobLauncherTestBase;
import com.linkedin.uif.runtime.JobState;
import com.linkedin.uif.writer.Destination;
import com.linkedin.uif.writer.WriterOutputFormat;

/**
 * Unit test for {@link LocalJobLauncher}.
 */
@Test(groups = {"ignore", "com.linkedin.uif.runtime.local"})
public class LocalJobLauncherTest extends JobLauncherTestBase {

    @BeforeClass
    public void startUp() throws Exception {
        this.properties = new Properties();
        this.properties.load(new FileReader("test/resource/uif.test.properties"));
        this.properties.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "false");
        this.jobStateStore = new FsStateStore(
                this.properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY),
                this.properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY),
                JobState.class);
    }

    @Test
    public void testLaunchJob() throws Exception {
        runTest(loadJobProps());
    }

    @Test
    public void testLaunchJobWithPullLimit() throws Exception {
        Properties jobProps = loadJobProps();
        jobProps.setProperty(ConfigurationKeys.EXTRACT_PULL_LIMIT, "10");
        runTestWithPullLimit(jobProps);
    }

    @Test
    public void testLaunchJobWithMultiWorkUnit() throws Exception {
        Properties jobProps = loadJobProps();
        jobProps.setProperty("use.multiworkunit", Boolean.toString(true));
        runTest(jobProps);
    }

    @Test
    public void testCancelJob() throws Exception {
        runTestWithCancellation(loadJobProps());
    }

    @Test
    public void testLaunchJobWithFork() throws Exception {
        Properties jobProps = loadJobProps();
        jobProps.setProperty(ConfigurationKeys.FORK_BRANCHES_KEY, "2");
        jobProps.setProperty(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY + ".0",
                WriterOutputFormat.AVRO.name());
        jobProps.setProperty(ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY + ".1",
                WriterOutputFormat.AVRO.name());
        jobProps.setProperty(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY + ".0",
                Destination.DestinationType.HDFS.name());
        jobProps.setProperty(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY + ".1",
                Destination.DestinationType.HDFS.name());
        runTestWithFork(jobProps);
    }

    private Properties loadJobProps() throws IOException {
        Properties jobProps = new Properties();
        jobProps.load(new FileReader("test/resource/job-conf/UIFTest1.pull"));
        jobProps.putAll(this.properties);
        jobProps.setProperty(SOURCE_FILE_LIST_KEY,
                "test/resource/source/test.avro.0," +
                "test/resource/source/test.avro.1," +
                "test/resource/source/test.avro.2," +
                "test/resource/source/test.avro.3");

        return jobProps;
    }
}
