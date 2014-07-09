package com.linkedin.uif.runtime.local;

import java.io.FileReader;
import java.util.Properties;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.metastore.FsStateStore;
import com.linkedin.uif.runtime.JobLauncherTestBase;
import com.linkedin.uif.runtime.JobState;

/**
 * Unit test for {@link LocalJobLauncher}.
 */
//@Test(groups = {"com.linkedin.uif.runtime.local"})
public class LocalJobLauncherTest extends JobLauncherTestBase {

    //@BeforeClass
    public void startUp() throws Exception {
        this.properties = new Properties();
        this.properties.load(new FileReader("test/resource/uif.test.properties"));
        this.properties.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "false");
        this.jobStateStore = new FsStateStore(
                this.properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY),
                this.properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY),
                JobState.class);
    }

    //@Test(groups = {"ignore"})
    public void testLaunchJob() throws Exception {
        Properties jobProps = new Properties();
        jobProps.load(new FileReader("test/resource/job-conf/UIFTest1.pull"));
        jobProps.putAll(this.properties);
        jobProps.setProperty(SOURCE_FILE_LIST_KEY,
                "test/resource/source/test.avro.0," +
                "test/resource/source/test.avro.1," +
                "test/resource/source/test.avro.2," +
                "test/resource/source/test.avro.3");

        runTest(jobProps);
    }

    //@Test(groups = {"ignore"})
    public void testLaunchJobWithPullLimit() throws Exception {
        Properties jobProps = new Properties();
        jobProps.load(new FileReader("test/resource/job-conf/UIFTest1.pull"));
        jobProps.putAll(this.properties);
        jobProps.setProperty(ConfigurationKeys.EXTRACT_PULL_LIMIT, "10");
        jobProps.setProperty(SOURCE_FILE_LIST_KEY,
                "test/resource/source/test.avro.0," +
                "test/resource/source/test.avro.1," +
                "test/resource/source/test.avro.2," +
                "test/resource/source/test.avro.3");

        runTestWithPullLimit(jobProps);
    }
}
