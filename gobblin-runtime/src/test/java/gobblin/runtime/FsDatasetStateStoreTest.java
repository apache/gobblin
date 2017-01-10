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

package gobblin.runtime;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.metastore.FsStateStore;
import gobblin.metastore.StateStore;


/**
 * Unit tests for {@link FsDatasetStateStore}.
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.runtime" })
public class FsDatasetStateStoreTest {

  private static final String TEST_JOB_NAME = "TestJob";
  private static final String TEST_JOB_ID = "TestJob1";
  private static final String TEST_TASK_ID_PREFIX = "TestTask-";
  private static final String TEST_DATASET_URN = "TestDataset";

  private StateStore<JobState> fsJobStateStore;
  private FsDatasetStateStore fsDatasetStateStore;
  private long startTime = System.currentTimeMillis();

  @BeforeClass
  public void setUp() throws IOException {
    this.fsJobStateStore = new FsStateStore<>(ConfigurationKeys.LOCAL_FS_URI,
        FsDatasetStateStoreTest.class.getSimpleName(), JobState.class);
    this.fsDatasetStateStore =
        new FsDatasetStateStore(ConfigurationKeys.LOCAL_FS_URI, FsDatasetStateStoreTest.class.getSimpleName());

    // clear data that may have been left behind by a prior test run
    this.fsDatasetStateStore.delete(TEST_JOB_NAME);
  }

  @Test
  public void testPersistJobState() throws IOException {
    JobState jobState = new JobState(TEST_JOB_NAME, TEST_JOB_ID);
    jobState.setId(TEST_JOB_ID);
    jobState.setProp("foo", "bar");
    jobState.setState(JobState.RunningState.COMMITTED);
    jobState.setStartTime(this.startTime);
    jobState.setEndTime(this.startTime + 1000);
    jobState.setDuration(1000);

    for (int i = 0; i < 3; i++) {
      TaskState taskState = new TaskState();
      taskState.setJobId(TEST_JOB_ID);
      taskState.setTaskId(TEST_TASK_ID_PREFIX + i);
      taskState.setId(TEST_TASK_ID_PREFIX + i);
      taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      jobState.addTaskState(taskState);
    }

    this.fsJobStateStore.put(TEST_JOB_NAME,
        FsDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + FsDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        jobState);
  }

  @Test(dependsOnMethods = "testPersistJobState")
  public void testGetJobState() throws IOException {
    JobState jobState = this.fsDatasetStateStore.get(TEST_JOB_NAME,
        FsDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + FsDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        TEST_JOB_ID);

    Assert.assertEquals(jobState.getJobName(), TEST_JOB_NAME);
    Assert.assertEquals(jobState.getJobId(), TEST_JOB_ID);
    Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(jobState.getStartTime(), this.startTime);
    Assert.assertEquals(jobState.getEndTime(), this.startTime + 1000);
    Assert.assertEquals(jobState.getDuration(), 1000);

    Assert.assertEquals(jobState.getCompletedTasks(), 3);
    for (int i = 0; i < jobState.getCompletedTasks(); i++) {
      TaskState taskState = jobState.getTaskStates().get(i);
      Assert.assertEquals(taskState.getJobId(), TEST_JOB_ID);
      Assert.assertEquals(taskState.getTaskId(), TEST_TASK_ID_PREFIX + i);
      Assert.assertEquals(taskState.getId(), TEST_TASK_ID_PREFIX + i);
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
    }
  }

  @Test(dependsOnMethods = "testGetJobState")
  public void testPersistDatasetState() throws IOException {
    JobState.DatasetState datasetState = new JobState.DatasetState(TEST_JOB_NAME, TEST_JOB_ID);

    datasetState.setDatasetUrn(TEST_DATASET_URN);
    datasetState.setState(JobState.RunningState.COMMITTED);
    datasetState.setId(TEST_DATASET_URN);
    datasetState.setStartTime(this.startTime);
    datasetState.setEndTime(this.startTime + 1000);
    datasetState.setDuration(1000);

    for (int i = 0; i < 3; i++) {
      TaskState taskState = new TaskState();
      taskState.setJobId(TEST_JOB_ID);
      taskState.setTaskId(TEST_TASK_ID_PREFIX + i);
      taskState.setId(TEST_TASK_ID_PREFIX + i);
      taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      datasetState.addTaskState(taskState);
    }

    this.fsDatasetStateStore.persistDatasetState(TEST_DATASET_URN, datasetState);
  }

  @Test(dependsOnMethods = "testPersistDatasetState")
  public void testGetDatasetState() throws IOException {
    JobState.DatasetState datasetState =
        this.fsDatasetStateStore.getLatestDatasetState(TEST_JOB_NAME, TEST_DATASET_URN);

    Assert.assertEquals(datasetState.getDatasetUrn(), TEST_DATASET_URN);
    Assert.assertEquals(datasetState.getJobName(), TEST_JOB_NAME);
    Assert.assertEquals(datasetState.getJobId(), TEST_JOB_ID);
    Assert.assertEquals(datasetState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(datasetState.getStartTime(), this.startTime);
    Assert.assertEquals(datasetState.getEndTime(), this.startTime + 1000);
    Assert.assertEquals(datasetState.getDuration(), 1000);

    Assert.assertEquals(datasetState.getCompletedTasks(), 3);
    for (int i = 0; i < datasetState.getCompletedTasks(); i++) {
      TaskState taskState = datasetState.getTaskStates().get(i);
      Assert.assertEquals(taskState.getJobId(), TEST_JOB_ID);
      Assert.assertEquals(taskState.getTaskId(), TEST_TASK_ID_PREFIX + i);
      Assert.assertEquals(taskState.getId(), TEST_TASK_ID_PREFIX + i);
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
    }
  }

  @Test(dependsOnMethods = "testGetDatasetState")
  public void testGetPreviousDatasetStatesByUrns() throws IOException {
    Map<String, JobState.DatasetState> datasetStatesByUrns =
        this.fsDatasetStateStore.getLatestDatasetStatesByUrns(TEST_JOB_NAME);
    Assert.assertEquals(datasetStatesByUrns.size(), 1);

    JobState.DatasetState datasetState = datasetStatesByUrns.get(TEST_DATASET_URN);
    Assert.assertEquals(datasetState.getDatasetUrn(), TEST_DATASET_URN);
    Assert.assertEquals(datasetState.getJobName(), TEST_JOB_NAME);
    Assert.assertEquals(datasetState.getJobId(), TEST_JOB_ID);
    Assert.assertEquals(datasetState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(datasetState.getStartTime(), this.startTime);
    Assert.assertEquals(datasetState.getEndTime(), this.startTime + 1000);
    Assert.assertEquals(datasetState.getDuration(), 1000);
  }

  @AfterClass
  public void tearDown() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration(false));
    Path rootDir = new Path(FsDatasetStateStoreTest.class.getSimpleName());
    if (fs.exists(rootDir)) {
      fs.delete(rootDir, true);
    }
  }
}
