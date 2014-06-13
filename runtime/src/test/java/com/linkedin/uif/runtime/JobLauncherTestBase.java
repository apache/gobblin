package com.linkedin.uif.runtime;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;

import com.google.common.io.Files;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.metastore.StateStore;
import com.linkedin.uif.source.workunit.Extract;

/**
 * Base class for {@link JobLauncher} unit tests.
 */
public abstract class JobLauncherTestBase {

    protected static final String SOURCE_FILE_LIST_KEY = "source.files";
    private static final String SOURCE_FILE_KEY = "source.file";

    protected Properties properties;
    protected StateStore jobStateStore;

    @SuppressWarnings("unchecked")
    protected void runTest(Properties jobProps) throws Exception {
        JobLauncher jobLauncher = JobLauncherFactory.newJobLauncher(this.properties);
        jobLauncher.launchJob(jobProps);
        String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
        String jobId = jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY);
        List<JobState> jobStateList = (List<JobState>) this.jobStateStore.getAll(
                jobName, jobId + ".jst");
        JobState jobState = jobStateList.get(0);

        Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
        Assert.assertEquals(jobState.getCompletedTasks(), 4);
        Assert.assertEquals(jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY), 0);

        for (TaskState taskState : jobState.getTaskStates()) {
            File sourceFile = new File(taskState.getProp(SOURCE_FILE_KEY));

            Extract e = taskState.getExtract();
            String targetDir = String.format(
                    "%s/%s/%s/%s_%s",
                    jobState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
                    e.getNamespace().replaceAll("\\.", "/"),
                    e.getTable(),
                    e.getExtractId(),
                    (e.getIsFull() ? "FULL" : "APPEND"));
            File targetFile = new File(targetDir, String.format("%s.%s.avro",
                    jobState.getProp(ConfigurationKeys.WRITER_FILE_NAME),
                    taskState.getId())
            );

            Assert.assertEquals(taskState.getWorkingState(),
                    WorkUnitState.WorkingState.COMMITTED);
            try {
                Assert.assertEquals(sourceFile.length(), targetFile.length());
                Assert.assertFalse(Files.equal(sourceFile, targetFile));
            } catch (Exception ex) {
                Assert.fail();
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected void runTestWithPullLimit(Properties jobProps) throws Exception {
        JobLauncher jobLauncher = JobLauncherFactory.newJobLauncher(this.properties);
        jobLauncher.launchJob(jobProps);
        String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
        String jobId = jobProps.getProperty(ConfigurationKeys.JOB_ID_KEY);
        List<JobState> jobStateList = (List<JobState>) this.jobStateStore.getAll(
                jobName, jobId + ".jst");
        JobState jobState = jobStateList.get(0);

        Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
        Assert.assertEquals(jobState.getCompletedTasks(), 4);
        Assert.assertEquals(jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY), 0);

        for (TaskState taskState : jobState.getTaskStates()) {
            Assert.assertEquals(taskState.getWorkingState(),
                    WorkUnitState.WorkingState.COMMITTED);
            Assert.assertEquals(
                    taskState.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED),
                    taskState.getPropAsLong(ConfigurationKeys.EXTRACT_PULL_LIMIT));
            Assert.assertEquals(
                    taskState.getPropAsLong(ConfigurationKeys.WRITER_ROWS_WRITTEN),
                    taskState.getPropAsLong(ConfigurationKeys.EXTRACT_PULL_LIMIT));
        }
    }
}
