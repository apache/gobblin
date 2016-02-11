/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.metastore.StateStore;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;
import gobblin.test.TestExtractor;
import gobblin.test.TestSource;
import gobblin.util.JobLauncherUtils;
import gobblin.util.limiter.DefaultLimiterFactory;


/**
 * Base class for {@link JobLauncher} unit tests.
 *
 * @author Yinan Li
 */
public class JobLauncherTestHelper {

  public static final String SOURCE_FILE_LIST_KEY = "source.files";

  private final StateStore<JobState.DatasetState> datasetStateStore;
  private final Properties launcherProps;

  public JobLauncherTestHelper(Properties launcherProps, StateStore<JobState.DatasetState> datasetStateStore) {
    this.launcherProps = launcherProps;
    this.datasetStateStore = datasetStateStore;
  }

  @SuppressWarnings("unchecked")
  public void runTest(Properties jobProps) throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName);
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);

    Closer closer = Closer.create();
    try {
      JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps));
      jobLauncher.launchJob(null);
    } finally {
      closer.close();
    }

    List<JobState.DatasetState> datasetStateList = this.datasetStateStore.getAll(jobName, jobId + ".jst");
    JobState jobState = datasetStateList.get(0);

    Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(jobState.getCompletedTasks(), 4);
    Assert.assertEquals(jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY), 0);
    for (TaskState taskState : jobState.getTaskStates()) {
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN),
          TestExtractor.TOTAL_RECORDS);
    }
  }

  @SuppressWarnings("unchecked")
  public void runTestWithPullLimit(Properties jobProps)
      throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName);
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);

    Closer closer = Closer.create();
    try {
      JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps));
      jobLauncher.launchJob(null);
    } finally {
      closer.close();
    }

    List<JobState.DatasetState> datasetStateList = this.datasetStateStore.getAll(jobName, jobId + ".jst");
    JobState jobState = datasetStateList.get(0);

    Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(jobState.getCompletedTasks(), 4);
    Assert.assertEquals(jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY), 0);

    for (TaskState taskState : jobState.getTaskStates()) {
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_EXTRACTED),
          taskState.getPropAsLong(DefaultLimiterFactory.EXTRACT_LIMIT_COUNT_LIMIT_KEY));
      Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_ROWS_WRITTEN),
          taskState.getPropAsLong(DefaultLimiterFactory.EXTRACT_LIMIT_COUNT_LIMIT_KEY));
    }
  }

  @SuppressWarnings("unchecked")
  public void runTestWithCancellation(final Properties jobProps) throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName);
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);

    Closer closer = Closer.create();
    try {
      final JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps));

      final AtomicBoolean isCancelled = new AtomicBoolean(false);
      // This thread will cancel the job after some time
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            Thread.sleep(500);
            jobLauncher.cancelJob(null);
            isCancelled.set(true);
          } catch (Exception je) {
            // Ignored
          }
        }
      });
      thread.start();

      jobLauncher.launchJob(null);
      Assert.assertTrue(isCancelled.get());
    } finally {
      closer.close();
    }

    List<JobState.DatasetState> datasetStateList = this.datasetStateStore.getAll(jobName, jobId + ".jst");
    Assert.assertTrue(datasetStateList.isEmpty());
  }

  @SuppressWarnings("unchecked")
  public void runTestWithFork(Properties jobProps)
      throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName);
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);

    Closer closer = Closer.create();
    try {
      JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps));
      jobLauncher.launchJob(null);
    } finally {
      closer.close();
    }

    List<JobState.DatasetState> datasetStateList = this.datasetStateStore.getAll(jobName, jobId + ".jst");
    JobState jobState = datasetStateList.get(0);

    Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(jobState.getCompletedTasks(), 4);
    Assert.assertEquals(jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY), 0);

    FileSystem lfs = FileSystem.getLocal(new Configuration());
    for (TaskState taskState : jobState.getTaskStates()) {
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      Path path =
          new Path(taskState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR), new Path(taskState.getExtract()
              .getOutputFilePath(), "fork_0"));
      Assert.assertTrue(lfs.exists(path));
      Assert.assertEquals(lfs.listStatus(path).length, 2);
      Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN + ".0"),
          TestExtractor.TOTAL_RECORDS);

      path = new Path(taskState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
          new Path(taskState.getExtract().getOutputFilePath(), "fork_1"));
      Assert.assertTrue(lfs.exists(path));
      Assert.assertEquals(lfs.listStatus(path).length, 2);
      Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN + ".1"),
          TestExtractor.TOTAL_RECORDS);
    }
  }

  public void runTestWithMultipleDatasets(Properties jobProps) throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName);
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);
    jobProps.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, MultiDatasetTestSource.class.getName());

    Closer closer = Closer.create();
    try {
      JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps));
      jobLauncher.launchJob(null);
    } finally {
      closer.close();
    }

    for (int i = 0; i < 4; i++) {
      List<JobState.DatasetState> datasetStateList =
          this.datasetStateStore.getAll(jobName, "Dataset" + i + "-current.jst");
      JobState jobState = datasetStateList.get(0);

      Assert.assertEquals(jobState.getProp(ConfigurationKeys.DATASET_URN_KEY), "Dataset" + i);
      Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
      Assert.assertEquals(jobState.getCompletedTasks(), 1);
      Assert.assertEquals(jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY), 0);
      for (TaskState taskState : jobState.getTaskStates()) {
        Assert.assertEquals(taskState.getProp(ConfigurationKeys.DATASET_URN_KEY), "Dataset" + i);
        Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
        Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN),
            TestExtractor.TOTAL_RECORDS);
      }
    }
  }

  public void runTestWithCommitSuccessfulTasksPolicy(Properties jobProps) throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName);
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);
    jobProps.setProperty(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL, Boolean.FALSE.toString());
    jobProps.setProperty(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "successful");
    jobProps.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, TestSourceWithFaultyExtractor.class.getName());
    jobProps.setProperty(ConfigurationKeys.MAX_TASK_RETRIES_KEY, "0");

    Closer closer = Closer.create();
    try {
      JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps));
      jobLauncher.launchJob(null);
    } finally {
      closer.close();
    }

    List<JobState.DatasetState> datasetStateList = this.datasetStateStore.getAll(jobName, jobId + ".jst");
    JobState jobState = datasetStateList.get(0);

    Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(jobState.getCompletedTasks(), 4);
    for (TaskState taskState : jobState.getTaskStates()) {
      if (taskState.getTaskId().endsWith("0")) {
        Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.FAILED);
      } else {
        Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
        Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN),
            TestExtractor.TOTAL_RECORDS);
      }
    }
  }

  public void runTestWithMultipleDatasetsAndFaultyExtractor(Properties jobProps, boolean usePartialCommitPolicy)
      throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName);
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);
    jobProps.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, MultiDatasetTestSourceWithFaultyExtractor.class.getName());
    jobProps.setProperty(ConfigurationKeys.MAX_TASK_RETRIES_KEY, "0");
    if (usePartialCommitPolicy) {
      jobProps.setProperty(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "partial");
    }

    Closer closer = Closer.create();
    try {
      JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps));
      jobLauncher.launchJob(null);
    } catch (JobException je) {
      // JobException is expected
    } finally {
      closer.close();
    }

    if (usePartialCommitPolicy) {
      List<JobState.DatasetState> datasetStateList = this.datasetStateStore.getAll(jobName, "Dataset0-current.jst");
      JobState.DatasetState datasetState = datasetStateList.get(0);
      Assert.assertEquals(datasetState.getState(), JobState.RunningState.COMMITTED);
      Assert.assertEquals(datasetState.getTaskCount(), 1);
      TaskState taskState = datasetState.getTaskStates().get(0);
      // BaseDataPublisher will change the state to COMMITTED
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
    } else {
      // Task 0 should have failed
      Assert.assertTrue(this.datasetStateStore.getAll(jobName, "Dataset0-current.jst").isEmpty());
    }

    for (int i = 1; i < 4; i++) {
      List<JobState.DatasetState> datasetStateList =
          this.datasetStateStore.getAll(jobName, "Dataset" + i + "-current.jst");
      JobState.DatasetState datasetState = datasetStateList.get(0);

      Assert.assertEquals(datasetState.getProp(ConfigurationKeys.DATASET_URN_KEY), "Dataset" + i);
      Assert.assertEquals(datasetState.getState(), JobState.RunningState.COMMITTED);
      Assert.assertEquals(datasetState.getCompletedTasks(), 1);
      for (TaskState taskState : datasetState.getTaskStates()) {
        Assert.assertEquals(taskState.getProp(ConfigurationKeys.DATASET_URN_KEY), "Dataset" + i);
        Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      }
    }
  }

  public void deleteStateStore(String storeName) throws IOException {
    this.datasetStateStore.delete(storeName);
  }

  public static class MultiDatasetTestSource extends TestSource {

    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
      List<WorkUnit> workUnits = super.getWorkunits(state);
      for (int i = 0; i < workUnits.size(); i++) {
        workUnits.get(i).setProp(ConfigurationKeys.DATASET_URN_KEY, "Dataset" + i);
      }
      return workUnits;
    }
  }

  public static class MultiDatasetTestSourceWithFaultyExtractor extends MultiDatasetTestSource {

    @Override
    public Extractor<String, String> getExtractor(WorkUnitState workUnitState) {
      Extractor<String, String> extractor = super.getExtractor(workUnitState);
      if (workUnitState.getProp(ConfigurationKeys.DATASET_URN_KEY).endsWith("0")) {
        return new FaultyExtractor(workUnitState);
      }
      return extractor;
    }
  }

  public static class FaultyExtractor extends TestExtractor {

    public FaultyExtractor(WorkUnitState workUnitState) {
      super(workUnitState);
    }

    @Override
    public String readRecord(@Deprecated String reuse) throws IOException {
      throw new IOException();
    }
  }

  public static class TestSourceWithFaultyExtractor extends TestSource {

    @Override
    public Extractor<String, String> getExtractor(WorkUnitState workUnitState) {
      Extractor<String, String> extractor = super.getExtractor(workUnitState);
      if (((TaskState) workUnitState).getTaskId().endsWith("0")) {
        return new FaultyExtractor(workUnitState);
      }
      return extractor;
    }
  }
}
