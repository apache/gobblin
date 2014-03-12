package com.linkedin.uif.test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ServiceManager;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.scheduler.local.LocalJobManager;
import com.linkedin.uif.scheduler.local.LocalTaskStateTracker;
import com.linkedin.uif.scheduler.JobListener;
import com.linkedin.uif.scheduler.JobState;
import com.linkedin.uif.scheduler.TaskExecutor;
import com.linkedin.uif.scheduler.TaskState;
import com.linkedin.uif.scheduler.TaskStateTracker;
import com.linkedin.uif.scheduler.WorkUnitManager;
import com.linkedin.uif.source.workunit.Extract;

/**
 * The basic integration test.
 *
 * @author ynli
 */
@Test(groups = {"com.linkedin.uif.test"})
public class BasicIntegrationTest {

    private static final String SOURCE_FILE_LIST_KEY = "source.files";
    private static final String SOURCE_FILE_KEY = "source.file";

    private ServiceManager serviceManager;
    private LocalJobManager jobManager;
    private Properties properties;

    @BeforeClass
    public void startUp() throws Exception {
        properties = new Properties();
        properties.load(new FileReader("test/resource/uif.test.properties"));
        
        TaskExecutor taskExecutor = new TaskExecutor(properties);
        TaskStateTracker taskStateTracker = new LocalTaskStateTracker(
                properties, taskExecutor);
        WorkUnitManager workUnitManager = new WorkUnitManager(
                taskExecutor, taskStateTracker);
        this.jobManager = new LocalJobManager(workUnitManager, properties);
        ((LocalTaskStateTracker) taskStateTracker).setJobManager(jobManager);

        this.serviceManager = new ServiceManager(Lists.newArrayList(
                // The order matters due to dependencies between services
                taskExecutor,
                taskStateTracker,
                workUnitManager,
                jobManager
        ));

        this.serviceManager.startAsync();

    }

    @Test
    public void runTest1() throws Exception {
        Properties jobProps = new Properties();
        jobProps.load(new FileReader("test/resource/job-conf/UIFTest1.pull"));
        jobProps.putAll(this.properties);
        jobProps.setProperty(SOURCE_FILE_LIST_KEY,
                "test/resource/source/test.avro.2,test/resource/source/test.avro.3");
        jobProps.setProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "true");

        CountDownLatch latch = new CountDownLatch(1);
        this.jobManager.scheduleJob(jobProps, new TestJobListener(latch));
        latch.await();
    }

    @Test
    public void runTest2() throws Exception {
        Properties jobProps = new Properties();
        jobProps.load(new FileReader("test/resource/job-conf/UIFTest2.pull"));
        jobProps.putAll(this.properties);
        jobProps.setProperty(SOURCE_FILE_LIST_KEY,
                "test/resource/source/test.avro.2,test/resource/source/test.avro.3");
        jobProps.setProperty(ConfigurationKeys.JOB_RUN_ONCE_KEY, "true");

        CountDownLatch latch = new CountDownLatch(1);
        this.jobManager.scheduleJob(jobProps, new TestJobListener(latch));
        latch.await();
    }

    @AfterClass
    public void tearDown() throws TimeoutException {
        this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
    }

    private static class TestJobListener implements JobListener {

        private final CountDownLatch latch;

        public TestJobListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void jobCompleted(JobState jobState) {
            Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
            Assert.assertEquals(jobState.getCompletedTasks(), 2);

            for (TaskState taskState : jobState.getTaskStates()) {
                File sourceFile = new File(taskState.getProp(SOURCE_FILE_KEY));

                Extract e = taskState.getExtract();
                File targetFile = new File(jobState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR)
                                           + "/" + e.getNamespace().replaceAll("\\.", "/") + "/" + 
                                           e.getTable() + "/" + e.getExtractId() + "_" + 
                                           (e.getIsFull() ? "FULL" : "APPEND"),
                                           jobState.getProp(ConfigurationKeys.WRITER_FILE_NAME)
                                           + "." + taskState.getId());
                
                Assert.assertEquals(taskState.getWorkingState(),
                        WorkUnitState.WorkingState.COMMITTED);
                try {
                    Assert.assertEquals(sourceFile.length(), targetFile.length());
                    Assert.assertFalse(Files.equal(sourceFile, targetFile));
                } catch (IOException ioe) {
                    Assert.fail();
                }
            }

            latch.countDown();
        }
    }
}
