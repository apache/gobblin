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

package org.apache.gobblin.data.management.retention;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.retention.dataset.CleanableDataset;
import org.apache.gobblin.data.management.retention.dataset.finder.TimeBasedDatasetStoreDatasetFinder;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.metastore.DatasetStoreDataset;
import org.apache.gobblin.metastore.MysqlStateStore;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.MysqlDatasetStateStore;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Unit test for cleaning {@link MysqlStateStore} and {@link MysqlDatasetStateStore}
 */
@Test(singleThreaded = true, groups = {"disabledOnCI"})
public class CleanableMysqlDatasetStoreDatasetTest {
  private static final String TEST_STATE_STORE = "TestStateStore";
  private static final String TEST_JOB_NAME1 = "TestJob1";
  private static final String TEST_JOB_NAME2 = "TestJob2";
  private static final String TEST_JOB_ID = "TestJobId";
  private static final String TEST_TASK_ID_PREFIX = "TestTask-";
  private static final String TEST_DATASET_URN1 = "TestDataset1";
  private static final String TEST_DATASET_URN2 = "TestDataset2";

  private MysqlStateStore<JobState> dbJobStateStore;
  private DatasetStateStore<JobState.DatasetState> dbDatasetStateStore;
  private long startTime = System.currentTimeMillis();
  private Config config;

  private ITestMetastoreDatabase testMetastoreDatabase;
  private static final String TEST_USER = "testUser";
  private static final String TEST_PASSWORD = "testPassword";

  private static String getJobId(String jobIdBase, int index) {
    return jobIdBase + index;
  }

  @BeforeClass
  public void setUp() throws Exception {
    this.testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    String jdbcUrl = this.testMetastoreDatabase.getJdbcUrl();
    ConfigBuilder configBuilder = ConfigBuilder.create();
    HikariDataSource dataSource = new HikariDataSource();

    dataSource.setDriverClassName(ConfigurationKeys.DEFAULT_STATE_STORE_DB_JDBC_DRIVER);
    dataSource.setAutoCommit(false);
    dataSource.setJdbcUrl(jdbcUrl);
    dataSource.setUsername(TEST_USER);
    dataSource.setPassword(TEST_PASSWORD);

    this.dbJobStateStore = new MysqlStateStore<>(dataSource, TEST_STATE_STORE, false, JobState.class);

    configBuilder.addPrimitive("selection.timeBased.lookbackTime", "10m");
    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_TYPE_KEY, "mysql");
    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TEST_STATE_STORE);
    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_DB_URL_KEY, jdbcUrl);
    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_DB_USER_KEY, TEST_USER);
    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, TEST_PASSWORD);

    ClassAliasResolver<DatasetStateStore.Factory> resolver = new ClassAliasResolver<>(DatasetStateStore.Factory.class);

    DatasetStateStore.Factory stateStoreFactory = resolver.resolveClass("mysql").newInstance();
    this.config = configBuilder.build();
    this.dbDatasetStateStore = stateStoreFactory.createStateStore(configBuilder.build());

    // clear data that may have been left behind by a prior test run
    this.dbJobStateStore.delete(TEST_JOB_NAME1);
    this.dbDatasetStateStore.delete(TEST_JOB_NAME1);
    this.dbJobStateStore.delete(TEST_JOB_NAME2);
    this.dbDatasetStateStore.delete(TEST_JOB_NAME2);
  }

  /**
   * Test cleanup of the job state store
   * @throws IOException
   */
  @Test(enabled = false)
  public void testCleanJobStateStore() throws IOException {
    JobState jobState = new JobState(TEST_JOB_NAME1, getJobId(TEST_JOB_ID, 1));
    jobState.setId(getJobId(TEST_JOB_ID, 1));
    jobState.setProp("foo", "bar");
    jobState.setState(JobState.RunningState.COMMITTED);
    jobState.setStartTime(this.startTime);
    jobState.setEndTime(this.startTime + 1000);
    jobState.setDuration(1000);

    for (int i = 0; i < 3; i++) {
      TaskState taskState = new TaskState();
      taskState.setJobId(getJobId(TEST_JOB_ID, 1));
      taskState.setTaskId(TEST_TASK_ID_PREFIX + i);
      taskState.setId(TEST_TASK_ID_PREFIX + i);
      taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      jobState.addTaskState(taskState);
    }

    // set old time to test that this state is deleted
    this.dbJobStateStore.setTestTimestamp(System.currentTimeMillis()/1000 - (60 * 20));

    this.dbJobStateStore.put(TEST_JOB_NAME1,
        MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        jobState);

    jobState.setJobName(TEST_JOB_NAME2);
    jobState.setJobId(getJobId(TEST_JOB_ID, 2));
    jobState.setId(getJobId(TEST_JOB_ID, 2));

    // set current time to test that the state is not deleted
    this.dbJobStateStore.setTestTimestamp(0);

    this.dbJobStateStore.put(TEST_JOB_NAME2,
        MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        jobState);

    TimeBasedDatasetStoreDatasetFinder datasetFinder =
        new TimeBasedDatasetStoreDatasetFinder(FileSystem.get(new Configuration()), ConfigUtils.configToProperties(config));
    List<DatasetStoreDataset> datasets = datasetFinder.findDatasets();

    Assert.assertTrue(this.dbJobStateStore.exists(TEST_JOB_NAME1,
        MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));
    Assert.assertTrue(this.dbJobStateStore.exists(TEST_JOB_NAME2,
        MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    for (DatasetStoreDataset dataset : datasets) {
      ((CleanableDataset) dataset).clean();
    }

    Assert.assertFalse(this.dbJobStateStore.exists(TEST_JOB_NAME1,
        MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    // state with recent timestamp is not deleted
    Assert.assertTrue(this.dbJobStateStore.exists(TEST_JOB_NAME2,
        MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));
  }

  /**
   * Test cleanup of the dataset state store. This test uses the combined selection policy to test that the newest 2
   * entries are retained even when the timestamp is old.
   *
   * @throws IOException
   */
  @Test(enabled = false)
  public void testCleanDatasetStateStore() throws IOException {
    JobState.DatasetState datasetState = new JobState.DatasetState(TEST_JOB_NAME1, getJobId(TEST_JOB_ID, 1));

    datasetState.setDatasetUrn(TEST_DATASET_URN1);
    datasetState.setState(JobState.RunningState.COMMITTED);
    datasetState.setId(TEST_DATASET_URN1);
    datasetState.setStartTime(this.startTime);
    datasetState.setEndTime(this.startTime + 1000);
    datasetState.setDuration(1000);

    for (int i = 0; i < 3; i++) {
      TaskState taskState = new TaskState();
      taskState.setJobId(getJobId(TEST_JOB_ID, 1));
      taskState.setTaskId(TEST_TASK_ID_PREFIX + i);
      taskState.setId(TEST_TASK_ID_PREFIX + i);
      taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      datasetState.addTaskState(taskState);
    }

    // set old time to test that this state is deleted
    ((MysqlDatasetStateStore)this.dbDatasetStateStore).setTestTimestamp(this.startTime/1000 + 2 - (60 * 20));
    datasetState.setJobId(getJobId(TEST_JOB_ID, 1));
    this.dbDatasetStateStore.persistDatasetState(TEST_DATASET_URN1, datasetState);

    ((MysqlDatasetStateStore)this.dbDatasetStateStore).setTestTimestamp(this.startTime/1000 + 4 - (60 * 20));
    datasetState.setJobId(getJobId(TEST_JOB_ID, 2));
    this.dbDatasetStateStore.persistDatasetState(TEST_DATASET_URN1, datasetState);

    ((MysqlDatasetStateStore)this.dbDatasetStateStore).setTestTimestamp(this.startTime/1000 + 6 - (60 * 20));
    datasetState.setJobId(getJobId(TEST_JOB_ID, 3));
    this.dbDatasetStateStore.persistDatasetState(TEST_DATASET_URN1, datasetState);

    datasetState.setJobId(getJobId(TEST_JOB_ID, 1));

    // persist a second dataset state to test that retrieval of multiple dataset states works
    datasetState.setDatasetUrn(TEST_DATASET_URN2);
    datasetState.setId(TEST_DATASET_URN2);
    datasetState.setDuration(2000);

    // set current time to test that the state is not deleted
    ((MysqlDatasetStateStore)this.dbDatasetStateStore).setTestTimestamp(0);

    this.dbDatasetStateStore.persistDatasetState(TEST_DATASET_URN2, datasetState);
    datasetState.setJobName(TEST_JOB_NAME2);
    this.dbDatasetStateStore.persistDatasetState(TEST_DATASET_URN2, datasetState);


    Config cleanerConfig = config
        .withValue("gobblin.retention.selection.policy.class", ConfigValueFactory.fromAnyRef("org.apache.gobblin.data.management.policy.CombineSelectionPolicy"))
        .withValue("gobblin.retention.selection.combine.operation", ConfigValueFactory.fromAnyRef("intersect"))
        .withValue("gobblin.retention.selection.combine.policy.classes", ConfigValueFactory.fromAnyRef("org.apache.gobblin.data.management.policy.SelectBeforeTimeBasedPolicy,org.apache.gobblin.data.management.policy.NewestKSelectionPolicy"))
        .withValue("gobblin.retention.selection.timeBased.lookbackTime", ConfigValueFactory.fromAnyRef("10m"))
        .withValue("gobblin.retention.selection.newestK.versionsNotSelected", ConfigValueFactory.fromAnyRef("2"));

    TimeBasedDatasetStoreDatasetFinder datasetFinder =
        new TimeBasedDatasetStoreDatasetFinder(FileSystem.get(new Configuration()), ConfigUtils.configToProperties(cleanerConfig));
    List<DatasetStoreDataset> datasets = datasetFinder.findDatasets();

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME1,
        TEST_DATASET_URN1 + "-" + MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX
            + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME1,
        TEST_DATASET_URN1 + "-" +  getJobId(TEST_JOB_ID, 1)
    + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME1,
        TEST_DATASET_URN1 + "-" +  getJobId(TEST_JOB_ID, 2)
            + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME1,
        TEST_DATASET_URN1 + "-" +  getJobId(TEST_JOB_ID, 3)
            + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME1,
        TEST_DATASET_URN2 + "-" + MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX
            + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME1,
        TEST_DATASET_URN2 + "-" + getJobId(TEST_JOB_ID, 1) + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME2,
        TEST_DATASET_URN2 + "-" + MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX
            + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME2,
        TEST_DATASET_URN2 + "-" + getJobId(TEST_JOB_ID, 1) + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    for (DatasetStoreDataset dataset : datasets) {
      ((CleanableDataset) dataset).clean();
    }

    // the most recent two entries (current and job id 3) should be retained
    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME1,
        TEST_DATASET_URN1 + "-" + MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX
            + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME1,
        TEST_DATASET_URN1 + "-" +  getJobId(TEST_JOB_ID, 3)
            + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertFalse(this.dbDatasetStateStore.exists(TEST_JOB_NAME1,
        TEST_DATASET_URN1 + "-" +  getJobId(TEST_JOB_ID, 1)
            + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertFalse(this.dbDatasetStateStore.exists(TEST_JOB_NAME1,
        TEST_DATASET_URN1 + "-" +  getJobId(TEST_JOB_ID, 2)
            + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME1,
        TEST_DATASET_URN2 + "-" + MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX
            + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME1,
        TEST_DATASET_URN2 + "-" + getJobId(TEST_JOB_ID, 1) + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME2,
        TEST_DATASET_URN2 + "-" + MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX
            + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));

    Assert.assertTrue(this.dbDatasetStateStore.exists(TEST_JOB_NAME2,
        TEST_DATASET_URN2 + "-" + getJobId(TEST_JOB_ID, 1) + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX));
  }
}
