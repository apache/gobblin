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

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.metastore.MysqlStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.util.ClassAliasResolver;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.dbcp.BasicDataSource;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link MysqlDatasetStateStore}.
 **/
@Test(groups = { "gobblin.runtime" })
public class MysqlDatasetStateStoreTest {

  private static final String TEST_STATE_STORE = "TestStateStore";
  private static final String TEST_JOB_NAME = "TestJob";
  private static final String TEST_JOB_ID = "TestJob1";
  private static final String TEST_TASK_ID_PREFIX = "TestTask-";
  private static final String TEST_DATASET_URN = "TestDataset";

  private StateStore<JobState> dbJobStateStore;
  private DatasetStateStore<JobState.DatasetState> dbDatasetStateStore;
  private long startTime = System.currentTimeMillis();

  private ITestMetastoreDatabase testMetastoreDatabase;
  private static final String TEST_USER = "testUser";
  private static final String TEST_PASSWORD = "testPassword";

  @BeforeClass
  public void setUp() throws Exception {
    testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    String jdbcUrl = testMetastoreDatabase.getJdbcUrl();
    ConfigBuilder configBuilder = ConfigBuilder.create();
    BasicDataSource mySqlDs = new BasicDataSource();

    mySqlDs.setDriverClassName(ConfigurationKeys.DEFAULT_STATE_STORE_DB_JDBC_DRIVER);
    mySqlDs.setDefaultAutoCommit(false);
    mySqlDs.setUrl(jdbcUrl);
    mySqlDs.setUsername(TEST_USER);
    mySqlDs.setPassword(TEST_PASSWORD);

    dbJobStateStore = new MysqlStateStore<>(mySqlDs, TEST_STATE_STORE, false, JobState.class);

    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_DB_URL_KEY, jdbcUrl);
    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_DB_USER_KEY, TEST_USER);
    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, TEST_PASSWORD);

    ClassAliasResolver<DatasetStateStore.Factory> resolver =
        new ClassAliasResolver<>(DatasetStateStore.Factory.class);

    DatasetStateStore.Factory stateStoreFactory =
          resolver.resolveClass("mysql").newInstance();
    dbDatasetStateStore = stateStoreFactory.createStateStore(configBuilder.build());

    // clear data that may have been left behind by a prior test run
    dbJobStateStore.delete(TEST_JOB_NAME);
    dbDatasetStateStore.delete(TEST_JOB_NAME);
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

    dbJobStateStore.put(TEST_JOB_NAME,
        MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        jobState);
  }

  @Test(dependsOnMethods = "testPersistJobState")
  public void testGetJobState() throws IOException {
    JobState jobState = dbJobStateStore.get(TEST_JOB_NAME,
        dbDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + dbDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
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

    dbDatasetStateStore.persistDatasetState(TEST_DATASET_URN, datasetState);
  }

  @Test(dependsOnMethods = "testPersistDatasetState")
  public void testGetDatasetState() throws IOException {
    JobState.DatasetState datasetState =
        dbDatasetStateStore.getLatestDatasetState(TEST_JOB_NAME, TEST_DATASET_URN);

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
        dbDatasetStateStore.getLatestDatasetStatesByUrns(TEST_JOB_NAME);
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
    JobState jobState = dbJobStateStore.get(TEST_JOB_NAME,
        dbDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + dbDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        TEST_JOB_ID);

    Assert.assertNotNull(jobState);
    Assert.assertEquals(jobState.getJobId(), TEST_JOB_ID);

    dbJobStateStore.delete(TEST_JOB_NAME);

    jobState = dbJobStateStore.get(TEST_JOB_NAME,
        dbDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + dbDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        TEST_JOB_ID);

    Assert.assertNull(jobState);
  }

  @Test(dependsOnMethods = "testGetPreviousDatasetStatesByUrns")
  public void testDeleteDatasetJobState() throws IOException {
    JobState.DatasetState datasetState = dbDatasetStateStore.get(TEST_JOB_NAME,
        TEST_DATASET_URN + "-" + dbDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX +
            dbDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX, TEST_DATASET_URN);

    Assert.assertNotNull(datasetState);
    Assert.assertEquals(datasetState.getJobId(), TEST_JOB_ID);

    dbDatasetStateStore.delete(TEST_JOB_NAME);

    datasetState = dbDatasetStateStore.get(TEST_JOB_NAME,
        TEST_DATASET_URN + "-" + dbDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX +
            dbDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX, TEST_DATASET_URN);

    Assert.assertNull(datasetState);
  }

  @AfterClass
  public void tearDown() throws IOException {
    dbJobStateStore.delete(TEST_JOB_NAME);
    dbDatasetStateStore.delete(TEST_JOB_NAME);
  }
}
