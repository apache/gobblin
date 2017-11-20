/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.runtime;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;

import com.google.common.io.Closer;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.JobState.DatasetState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.test.TestExtractor;
import org.apache.gobblin.test.TestSource;
import org.apache.gobblin.util.ClusterNameTags;
import org.apache.gobblin.util.JobLauncherUtils;


/**
 * Base class for {@link JobLauncher} unit tests.
 *
 * @author Yinan Li
 */
public class JobLauncherTestHelper {

  public static final String SOURCE_FILE_LIST_KEY = "source.files";
  public static final String DYNAMIC_KEY1 = "DynamicKey1";
  public static final String DYNAMIC_VALUE1 = "DynamicValue1";

  private final StateStore<JobState.DatasetState> datasetStateStore;
  private final Properties launcherProps;

  public JobLauncherTestHelper(Properties launcherProps, StateStore<JobState.DatasetState> datasetStateStore) {
    this.launcherProps = launcherProps;
    this.datasetStateStore = datasetStateStore;
  }

  public void runTest(Properties jobProps) throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName);
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);

    JobContext jobContext = null;
    Closer closer = Closer.create();
    try {
      JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps));
      jobLauncher.launchJob(null);
      jobContext = ((AbstractJobLauncher) jobLauncher).getJobContext();
    } finally {
      closer.close();
    }

    Assert.assertTrue(jobContext.getJobMetricsOptional().isPresent());
    String jobMetricContextTags = jobContext.getJobMetricsOptional().get().getMetricContext().getTags().toString();
    Assert.assertTrue(jobMetricContextTags.contains(ClusterNameTags.CLUSTER_IDENTIFIER_TAG_NAME),
        ClusterNameTags.CLUSTER_IDENTIFIER_TAG_NAME + " tag missing in job metric context tags.");

    List<JobState.DatasetState> datasetStateList = this.datasetStateStore.getAll(jobName, sanitizeJobNameForDatasetStore(jobId) + ".jst");
    DatasetState datasetState = datasetStateList.get(0);

    Assert.assertEquals(datasetState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(datasetState.getCompletedTasks(), 4);
    Assert.assertEquals(datasetState.getJobFailures(), 0);

    for (TaskState taskState : datasetState.getTaskStates()) {
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN),
          TestExtractor.TOTAL_RECORDS);
    }
  }

  public void runTestWithPullLimit(Properties jobProps, long limit) throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName).toString();
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);

    Closer closer = Closer.create();
    try {
      JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps));
      jobLauncher.launchJob(null);
    } finally {
      closer.close();
    }

    List<JobState.DatasetState> datasetStateList = this.datasetStateStore.getAll(jobName, sanitizeJobNameForDatasetStore(jobId) + ".jst");
    DatasetState datasetState = datasetStateList.get(0);

    Assert.assertEquals(datasetState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(datasetState.getCompletedTasks(), 4);
    Assert.assertEquals(datasetState.getJobFailures(), 0);

    for (TaskState taskState : datasetState.getTaskStates()) {
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_EXTRACTED), limit);
      Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_ROWS_WRITTEN), limit);
    }
  }

  public void runTestWithCancellation(final Properties jobProps) throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName).toString();
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

    List<JobState.DatasetState> datasetStateList = this.datasetStateStore.getAll(jobName, sanitizeJobNameForDatasetStore(jobId) + ".jst");
    Assert.assertTrue(datasetStateList.isEmpty());
  }

  public void runTestWithFork(Properties jobProps) throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName).toString();
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);

    try (JobLauncher jobLauncher = JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps)) {
      jobLauncher.launchJob(null);
    }

    List<JobState.DatasetState> datasetStateList = this.datasetStateStore.getAll(jobName, sanitizeJobNameForDatasetStore(jobId) + ".jst");
    DatasetState datasetState = datasetStateList.get(0);

    Assert.assertEquals(datasetState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(datasetState.getCompletedTasks(), 4);
    Assert.assertEquals(datasetState.getJobFailures(), 0);

    FileSystem lfs = FileSystem.getLocal(new Configuration());
    for (TaskState taskState : datasetState.getTaskStates()) {
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      Path path = new Path(this.launcherProps.getProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
          new Path(taskState.getExtract().getOutputFilePath(), "fork_0"));
      Assert.assertTrue(lfs.exists(path));
      Assert.assertEquals(lfs.listStatus(path).length, 2);
      Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN + ".0"),
          TestExtractor.TOTAL_RECORDS);

      path = new Path(this.launcherProps.getProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
          new Path(taskState.getExtract().getOutputFilePath(), "fork_1"));
      Assert.assertTrue(lfs.exists(path));
      Assert.assertEquals(lfs.listStatus(path).length, 2);
      Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN + ".1"),
          TestExtractor.TOTAL_RECORDS);
    }
  }

  public void runTestWithMultipleDatasets(Properties jobProps) throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName).toString();
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
      DatasetState datasetState = datasetStateList.get(0);

      Assert.assertEquals(datasetState.getDatasetUrn(), "Dataset" + i);
      Assert.assertEquals(datasetState.getState(), JobState.RunningState.COMMITTED);
      Assert.assertEquals(datasetState.getCompletedTasks(), 1);
      Assert.assertEquals(datasetState.getJobFailures(), 0);
      for (TaskState taskState : datasetState.getTaskStates()) {
        Assert.assertEquals(taskState.getProp(ConfigurationKeys.DATASET_URN_KEY), "Dataset" + i);
        Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
        Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN),
            TestExtractor.TOTAL_RECORDS);
      }
    }
  }

  /**
   * Test when a test with the matching suffix is skipped.
   * @param jobProps job properties
   * @param skippedTaskSuffix the suffix for the task that is skipped
   */
  public void runTestWithSkippedTask(Properties jobProps, String skippedTaskSuffix) throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName).toString();
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, jobId);
    jobProps.setProperty(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL, Boolean.FALSE.toString());
    jobProps.setProperty(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "successful");
    jobProps.setProperty(ConfigurationKeys.MAX_TASK_RETRIES_KEY, "0");

    Closer closer = Closer.create();
    try {
      JobLauncher jobLauncher = closer.register(JobLauncherFactory.newJobLauncher(this.launcherProps, jobProps));
      jobLauncher.launchJob(null);
    } finally {
      closer.close();
    }

    List<JobState.DatasetState> datasetStateList =
        this.datasetStateStore.getAll(jobName, sanitizeJobNameForDatasetStore(jobId) + ".jst");
    JobState jobState = datasetStateList.get(0);

    Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
    // one task is skipped out of 4
    Assert.assertEquals(jobState.getCompletedTasks(), 3);
    for (TaskState taskState : jobState.getTaskStates()) {
      if (taskState.getTaskId().endsWith(skippedTaskSuffix)) {
        Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.PENDING);
      } else {
        Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
        Assert.assertEquals(taskState.getPropAsLong(ConfigurationKeys.WRITER_RECORDS_WRITTEN),
            TestExtractor.TOTAL_RECORDS);
      }
    }
  }

  public void runTestWithCommitSuccessfulTasksPolicy(Properties jobProps) throws Exception {
    String jobName = jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    String jobId = JobLauncherUtils.newJobId(jobName).toString();
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

    List<JobState.DatasetState> datasetStateList = this.datasetStateStore.getAll(jobName, sanitizeJobNameForDatasetStore(jobId) + ".jst");
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
    String jobId = JobLauncherUtils.newJobId(jobName).toString();
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

      Assert.assertEquals(datasetState.getDatasetUrn(), "Dataset" + i);
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
      throw new IOException("Injected failure");
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

  private String sanitizeJobNameForDatasetStore(String jobId) {
    return jobId.replaceAll("[-/]", "_");
  }
}
