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

import gobblin.config.ConfigBuilder;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.metastore.DatasetStateStore;
import gobblin.metastore.StateStore;
import gobblin.metastore.ZkStateStore;
import gobblin.metastore.ZkStateStoreConfigurationKeys;
import gobblin.util.ClassAliasResolver;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.curator.test.TestingServer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link ZkDatasetStateStore}.
 **/
@Test(groups = { "gobblin.runtime" })
public class ZkDatasetStateStoreTest {
  private static final String TEST_JOB_NAME = "TestJob";
  private static final String TEST_JOB_ID = "TestJob1";
  private static final String TEST_TASK_ID_PREFIX = "TestTask-";
  private static final String TEST_DATASET_URN = "TestDataset";

  private TestingServer testingServer;
  private StateStore<JobState> zkJobStateStore;
  private DatasetStateStore<JobState.DatasetState> zkDatasetStateStore;
  private long startTime = System.currentTimeMillis();

  private Properties props;

  @BeforeClass
  public void setUp() throws Exception {
    ConfigBuilder configBuilder = ConfigBuilder.create();
    testingServer = new TestingServer(-1);
    zkJobStateStore = new ZkStateStore<>(testingServer.getConnectString(), "/STATE_STORE/TEST", false, JobState.class);

    configBuilder.addPrimitive(ZkStateStoreConfigurationKeys.STATE_STORE_ZK_CONNECT_STRING_KEY,
        testingServer.getConnectString());
    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, "/STATE_STORE/TEST2");

    ClassAliasResolver<DatasetStateStore.Factory> resolver =
        new ClassAliasResolver<>(DatasetStateStore.Factory.class);
    DatasetStateStore.Factory stateStoreFactory =
        resolver.resolveClass("zk").newInstance();

    zkDatasetStateStore = stateStoreFactory.createStateStore(configBuilder.build());

    // clear data that may have been left behind by a prior test run
    zkJobStateStore.delete(TEST_JOB_NAME);
    zkDatasetStateStore.delete(TEST_JOB_NAME);
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

    zkJobStateStore.put(TEST_JOB_NAME,
        ZkDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + ZkDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        jobState);
  }

  @Test(dependsOnMethods = "testPersistJobState")
  public void testGetJobState() throws IOException {
    JobState jobState = zkJobStateStore.get(TEST_JOB_NAME,
        zkDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + zkDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
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

    zkDatasetStateStore.persistDatasetState(TEST_DATASET_URN, datasetState);
  }

  @Test(dependsOnMethods = "testPersistDatasetState")
  public void testGetDatasetState() throws IOException {
    JobState.DatasetState datasetState =
        zkDatasetStateStore.getLatestDatasetState(TEST_JOB_NAME, TEST_DATASET_URN);

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
        zkDatasetStateStore.getLatestDatasetStatesByUrns(TEST_JOB_NAME);
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

  @Test(dependsOnMethods = "testGetPreviousDatasetStatesByUrns")
  public void testDeleteJobState() throws IOException {
    JobState jobState = zkJobStateStore.get(TEST_JOB_NAME,
        zkDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + zkDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        TEST_JOB_ID);

    Assert.assertNotNull(jobState);
    Assert.assertEquals(jobState.getJobId(), TEST_JOB_ID);

    zkJobStateStore.delete(TEST_JOB_NAME);

    jobState = zkJobStateStore.get(TEST_JOB_NAME,
        zkDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + zkDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        TEST_JOB_ID);

    Assert.assertNull(jobState);
  }

  @Test(dependsOnMethods = "testGetPreviousDatasetStatesByUrns")
  public void testDeleteDatasetJobState() throws IOException {
    JobState.DatasetState datasetState = zkDatasetStateStore.get(TEST_JOB_NAME,
        TEST_DATASET_URN + "-" + zkDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX +
            zkDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX, TEST_DATASET_URN);

    Assert.assertNotNull(datasetState);
    Assert.assertEquals(datasetState.getJobId(), TEST_JOB_ID);

    zkDatasetStateStore.delete(TEST_JOB_NAME);

    datasetState = zkDatasetStateStore.get(TEST_JOB_NAME,
        TEST_DATASET_URN + "-" + zkDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX +
            zkDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX, TEST_DATASET_URN);

    Assert.assertNull(datasetState);
  }

  @AfterClass
  public void tearDown() throws IOException {
    zkJobStateStore.delete(TEST_JOB_NAME);
    zkDatasetStateStore.delete(TEST_JOB_NAME);

    if (testingServer != null) {
      testingServer.close();
    }
  }
}