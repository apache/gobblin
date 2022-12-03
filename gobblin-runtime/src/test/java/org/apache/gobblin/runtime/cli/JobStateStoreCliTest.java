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

package org.apache.gobblin.runtime.cli;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

import com.zaxxer.hikari.HikariDataSource;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.metastore.MysqlStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.MysqlDatasetStateStore;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = { "gobblin.runtime" })
public class JobStateStoreCliTest {

  private static final String TEST_STATE_STORE = "TestStateStore";
  private static final String TEST_JOB_NAME = "TestJob";
  private static final String TEST_JOB_NAME2 = "TestJob2";
  private static final String TEST_JOB_NAME3 = "TestJob3";
  private static final String TEST_JOB_ID = "TestJob1";
  private static final String TEST_TASK_ID_PREFIX = "TestTask-";

  private StateStore<JobState> dbJobStateStore;
  private DatasetStateStore<JobState.DatasetState> dbDatasetStateStore;
  private long startTime = System.currentTimeMillis();
  private String configPath;

  private ITestMetastoreDatabase testMetastoreDatabase;
  private static final String TEST_USER = "testUser";
  private static final String TEST_PASSWORD = "testPassword";
  File deleteFile;

  @BeforeClass
  public void setUp() throws Exception {
    testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    String jdbcUrl = testMetastoreDatabase.getJdbcUrl();
    ConfigBuilder configBuilder = ConfigBuilder.create();
    HikariDataSource dataSource = new HikariDataSource();

    dataSource.setDriverClassName(ConfigurationKeys.DEFAULT_STATE_STORE_DB_JDBC_DRIVER);
    dataSource.setAutoCommit(false);
    dataSource.setJdbcUrl(jdbcUrl);
    dataSource.setUsername(TEST_USER);
    dataSource.setPassword(TEST_PASSWORD);

    dbJobStateStore = new MysqlStateStore<>(dataSource, TEST_STATE_STORE, false, JobState.class);

    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_DB_URL_KEY, jdbcUrl);
    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_DB_USER_KEY, TEST_USER);
    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, TEST_PASSWORD);
    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_TYPE_KEY, "mysql");
    configBuilder.addPrimitive(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TEST_STATE_STORE);

    // store the config into a temp file to be read by cli
    configPath = File.createTempFile("config.properties", null).getPath();
    try (OutputStream output = new FileOutputStream(configPath)) {
      ConfigUtils.configToProperties(configBuilder.build()).store(output, "");
    }

    ClassAliasResolver<DatasetStateStore.Factory> resolver =
        new ClassAliasResolver<>(DatasetStateStore.Factory.class);
    DatasetStateStore.Factory stateStoreFactory =
        resolver.resolveClass("mysql").newInstance();
    dbDatasetStateStore = stateStoreFactory.createStateStore(configBuilder.build());

    // clear data that may have been left behind by a prior test run
    dbJobStateStore.delete(TEST_JOB_NAME);
    dbJobStateStore.delete(TEST_JOB_NAME2);

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

    jobState.setJobName(TEST_JOB_NAME2);
    dbJobStateStore.put(TEST_JOB_NAME2,
        MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        jobState);

    jobState.setJobName(TEST_JOB_NAME3);
    dbJobStateStore.put(TEST_JOB_NAME3,
        MysqlDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + MysqlDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        jobState);
  }

  @Test
  public void testClBulkDelete() throws Exception {
    String deleteFileText = TEST_JOB_NAME +"\n" + TEST_JOB_NAME2;
    deleteFile = File.createTempFile("deleteFile.txt", null);
    FileOutputStream outputStream = new FileOutputStream(deleteFile.getPath());
    byte[] strToBytes = deleteFileText.getBytes();
    outputStream.write(strToBytes);

    outputStream.close();

    JobStateStoreCLI cli = new JobStateStoreCLI();
    String[] args = {"job-state-store", "-sc", configPath, "-bd", deleteFile.getPath()};
    cli.run(args);

    JobState jobState = dbJobStateStore.get(TEST_JOB_NAME,
        dbDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + dbDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        TEST_JOB_ID);

    JobState jobState2 = dbJobStateStore.get(TEST_JOB_NAME2,
        dbDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + dbDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        TEST_JOB_ID);

    JobState jobState3 = dbJobStateStore.get(TEST_JOB_NAME3,
        dbDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + dbDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        TEST_JOB_ID);

    Assert.assertNull(jobState);
    Assert.assertNull(jobState2);
    Assert.assertNotNull(jobState3);
  }

  @Test(dependsOnMethods = "testClBulkDelete")
  public void testCliDeleteSingle() throws Exception {
    JobStateStoreCLI cli = new JobStateStoreCLI();
    String[] args = {"job-state-store", "-sc", configPath, "-d", "-n", TEST_JOB_NAME3};
    cli.run(args);

    JobState jobState = dbJobStateStore.get(TEST_JOB_NAME3,
        dbDatasetStateStore.CURRENT_DATASET_STATE_FILE_SUFFIX + dbDatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX,
        TEST_JOB_ID);

    Assert.assertNull(jobState);
  }

}
