package com.linkedin.uif.scheduler;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ServiceManager;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.runtime.TaskExecutor;
import com.linkedin.uif.runtime.TaskStateTracker;
import com.linkedin.uif.runtime.WorkUnitManager;
import com.linkedin.uif.runtime.local.LocalJobManager;
import com.linkedin.uif.runtime.local.LocalTaskStateTracker;

/**
 * Unit tests for the job configuration file monitor in {@link LocalJobManager}.
 *
 * @author ynli
 */
@Test(groups = {"com.linkedin.uif.scheduler"})
public class JobConfigFileMonitorTest {

    private static final String JOB_CONFIG_FILE_DIR = "test/resource/job-conf";

    private ServiceManager serviceManager;
    private LocalJobManager jobManager;
    private File newJobConfigFile;

    @BeforeClass
    public void setUp() throws Exception {
        Properties properties = new Properties();
        properties.load(new FileReader("test/resource/uif.test.properties"));
        properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, JOB_CONFIG_FILE_DIR);
        properties.setProperty(ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY, "1000");
        properties.setProperty(ConfigurationKeys.METRICS_ENABLED_KEY, "false");

        TaskExecutor taskExecutor = new TaskExecutor(properties);
        TaskStateTracker taskStateTracker = new LocalTaskStateTracker(
                properties, taskExecutor);
        WorkUnitManager workUnitManager = new WorkUnitManager(
                taskExecutor, taskStateTracker);
        this.jobManager = new LocalJobManager(workUnitManager, properties);
        ((LocalTaskStateTracker) taskStateTracker).setJobManager(this.jobManager);

        this.serviceManager = new ServiceManager(Lists.newArrayList(
                // The order matters due to dependencies between services
                taskExecutor,
                taskStateTracker,
                workUnitManager,
                this.jobManager
        ));

        this.serviceManager.startAsync();
    }

    @Test
    public void testAddNewJobConfigFile() throws Exception {
        Thread.sleep(2000);

        Assert.assertEquals(this.jobManager.getScheduledJobs().size(), 3);

        // Create a new job configuration file by making a copy of an existing
        // one and giving a different job name
        Properties jobProps = new Properties();
        jobProps.load(new FileReader(new File(JOB_CONFIG_FILE_DIR, "UIFTest1.pull")));
        jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "UIF-test-new");
        this.newJobConfigFile = new File(JOB_CONFIG_FILE_DIR, "UIF-test-new.pull");
        jobProps.store(new FileWriter(this.newJobConfigFile), null);

        Thread.sleep(2000);

        Set<String> jobNames = Sets.newHashSet(this.jobManager.getScheduledJobs());
        Assert.assertEquals(jobNames.size(), 4);
        Assert.assertTrue(jobNames.contains("UIFTest1"));
        Assert.assertTrue(jobNames.contains("UIFTest2"));
        Assert.assertTrue(jobNames.contains("UIFTest3"));
        // The new job should be in the set of scheduled jobs
        Assert.assertTrue(jobNames.contains("UIF-test-new"));
    }

    @Test(dependsOnMethods = {"testAddNewJobConfigFile"})
    public void testChangeJobConfigFile() throws Exception {
        Assert.assertEquals(this.jobManager.getScheduledJobs().size(), 4);

        // Make a change to the new job configuration file
        Properties jobProps = new Properties();
        jobProps.load(new FileReader(this.newJobConfigFile));
        jobProps.setProperty(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "partial");
        jobProps.store(new FileWriter(this.newJobConfigFile), null);

        Thread.sleep(2000);

        Set<String> jobNames = Sets.newHashSet(this.jobManager.getScheduledJobs());
        Assert.assertEquals(jobNames.size(), 4);
        Assert.assertTrue(jobNames.contains("UIFTest1"));
        Assert.assertTrue(jobNames.contains("UIFTest2"));
        Assert.assertTrue(jobNames.contains("UIFTest3"));
        // The newly added job should still be in the set of scheduled jobs
        Assert.assertTrue(jobNames.contains("UIF-test-new"));
    }

    @Test(dependsOnMethods = {"testChangeJobConfigFile"})
    public void testUnscheduleJob() throws Exception {
        Assert.assertEquals(this.jobManager.getScheduledJobs().size(), 4);

        // Disable the new job by setting job.disabled=true
        Properties jobProps = new Properties();
        jobProps.load(new FileReader(this.newJobConfigFile));
        jobProps.setProperty(ConfigurationKeys.JOB_DISABLED_KEY, "true");
        jobProps.store(new FileWriter(this.newJobConfigFile), null);

        Thread.sleep(2000);

        Set<String> jobNames = Sets.newHashSet(this.jobManager.getScheduledJobs());
        Assert.assertEquals(jobNames.size(), 3);
        Assert.assertTrue(jobNames.contains("UIFTest1"));
        Assert.assertTrue(jobNames.contains("UIFTest2"));
        Assert.assertTrue(jobNames.contains("UIFTest3"));
    }

    @AfterClass
    public void tearDown() throws TimeoutException {
        this.newJobConfigFile.delete();
        this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
    }
}
