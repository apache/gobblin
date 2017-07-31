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

package org.apache.gobblin.metastore;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.linkedin.data.template.StringMap;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.rest.JobExecutionInfo;
import org.apache.gobblin.rest.JobExecutionQuery;
import org.apache.gobblin.rest.JobStateEnum;
import org.apache.gobblin.rest.LauncherTypeEnum;
import org.apache.gobblin.rest.Metric;
import org.apache.gobblin.rest.MetricArray;
import org.apache.gobblin.rest.MetricTypeEnum;
import org.apache.gobblin.rest.QueryIdTypeEnum;
import org.apache.gobblin.rest.Table;
import org.apache.gobblin.rest.TableTypeEnum;
import org.apache.gobblin.rest.TaskExecutionInfo;
import org.apache.gobblin.rest.TaskExecutionInfoArray;
import org.apache.gobblin.rest.TaskStateEnum;


/**
 * Unit tests for {@link DatabaseJobHistoryStore}.
 *
 * @author Yinan Li
 */
@Test(groups = {"gobblin.metastore"})
public abstract class DatabaseJobHistoryStoreTest {

  private final List<JobExecutionInfo> expectedJobExecutionInfos = Lists.newArrayList();

  private ITestMetastoreDatabase testMetastoreDatabase;
  private JobHistoryStore jobHistoryStore;

  protected abstract String getVersion();

  @BeforeClass
  public void setUp()
      throws Exception {
    ConfigFactory.invalidateCaches();
    testMetastoreDatabase = TestMetastoreDatabaseFactory.get(getVersion());
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY, testMetastoreDatabase.getJdbcUrl());
    Injector injector = Guice.createInjector(new MetaStoreModule(properties));
    this.jobHistoryStore = injector.getInstance(JobHistoryStore.class);
  }

  @Test
  public void testInsert()
      throws IOException {
    this.jobHistoryStore.put(create(0, false));
    this.jobHistoryStore.put(create(1, true));
  }

  @Test(dependsOnMethods = {"testInsert"})
  public void testUpdate()
      throws IOException {
    for (JobExecutionInfo jobExecutionInfo : this.expectedJobExecutionInfos) {
      jobExecutionInfo.setEndTime(System.currentTimeMillis());
      jobExecutionInfo.setDuration(jobExecutionInfo.getEndTime() - jobExecutionInfo.getStartTime());
      jobExecutionInfo.setState(JobStateEnum.COMMITTED);
      jobExecutionInfo.setCompletedTasks(jobExecutionInfo.getLaunchedTasks());
      for (TaskExecutionInfo taskExecutionInfo : jobExecutionInfo.getTaskExecutions()) {
        taskExecutionInfo.setEndTime(jobExecutionInfo.getEndTime());
        taskExecutionInfo.setDuration(taskExecutionInfo.getEndTime() - taskExecutionInfo.getStartTime());
        taskExecutionInfo.setState(TaskStateEnum.COMMITTED);
      }
      this.jobHistoryStore.put(jobExecutionInfo);
    }
  }

  @Test(dependsOnMethods = {"testUpdate"})
  public void testQueryByJobId()
      throws IOException {
    JobExecutionQuery queryByJobId = new JobExecutionQuery();
    queryByJobId.setIdType(QueryIdTypeEnum.JOB_ID);
    queryByJobId.setId(JobExecutionQuery.Id.create(this.expectedJobExecutionInfos.get(0).getJobId()));

    List<JobExecutionInfo> result = this.jobHistoryStore.get(queryByJobId);
    Assert.assertEquals(result.size(), 1);
    JobExecutionInfo actual = result.get(0);
    JobExecutionInfo expected = this.expectedJobExecutionInfos.get(0);
    assertJobExecution(actual, expected);
  }

  @Test(dependsOnMethods = {"testUpdate"})
  public void testQueryByJobName()
      throws IOException {
    JobExecutionQuery queryByJobName = new JobExecutionQuery();
    queryByJobName.setIdType(QueryIdTypeEnum.JOB_NAME);
    queryByJobName.setId(JobExecutionQuery.Id.create(this.expectedJobExecutionInfos.get(0).getJobName()));

    List<JobExecutionInfo> result = this.jobHistoryStore.get(queryByJobName);
    Assert.assertEquals(result.size(), 1);
    JobExecutionInfo actual = result.get(0);
    JobExecutionInfo expected = this.expectedJobExecutionInfos.get(0);
    assertJobExecution(actual, expected);
  }

  @Test(dependsOnMethods = {"testUpdate"})
  public void testQueryByTable()
      throws IOException {
    JobExecutionQuery queryByTable = new JobExecutionQuery();

    queryByTable.setIdType(QueryIdTypeEnum.TABLE);
    queryByTable.setId(
        JobExecutionQuery.Id.create(this.expectedJobExecutionInfos.get(0).getTaskExecutions().get(0).getTable()));

    List<JobExecutionInfo> result = this.jobHistoryStore.get(queryByTable);
    Assert.assertEquals(result.size(), 2);

    JobExecutionInfo actual = result.get(0);
    Assert.assertEquals(actual.getJobName(), this.expectedJobExecutionInfos.get(0).getJobName());
    Assert.assertEquals(actual.getJobId(), this.expectedJobExecutionInfos.get(0).getJobId());
    Assert.assertEquals(actual.getTaskExecutions().size(), 1);
    Assert.assertEquals(actual.getTaskExecutions().get(0).getTable(),
        this.expectedJobExecutionInfos.get(0).getTaskExecutions().get(0).getTable());

    actual = result.get(1);
    Assert.assertEquals(actual.getJobName(), this.expectedJobExecutionInfos.get(1).getJobName());
    Assert.assertEquals(actual.getJobId(), this.expectedJobExecutionInfos.get(1).getJobId());
    Assert.assertEquals(actual.getTaskExecutions().size(), 1);
    Assert.assertEquals(actual.getTaskExecutions().get(0).getTable(),
        this.expectedJobExecutionInfos.get(1).getTaskExecutions().get(0).getTable());

    queryByTable.setId(
        JobExecutionQuery.Id.create(this.expectedJobExecutionInfos.get(1).getTaskExecutions().get(1).getTable()));

    result = this.jobHistoryStore.get(queryByTable);
    Assert.assertEquals(result.size(), 1);

    actual = result.get(0);
    Assert.assertEquals(actual.getJobName(), this.expectedJobExecutionInfos.get(1).getJobName());
    Assert.assertEquals(actual.getJobId(), this.expectedJobExecutionInfos.get(1).getJobId());
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(actual.getTaskExecutions().size(), 1);
    Assert.assertEquals(actual.getTaskExecutions().get(0).getTable(),
        this.expectedJobExecutionInfos.get(1).getTaskExecutions().get(1).getTable());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    if (this.jobHistoryStore != null) {
      this.jobHistoryStore.close();
    }
    if (this.testMetastoreDatabase != null) {
      this.testMetastoreDatabase.close();
    }
  }

  private JobExecutionInfo create(int index, boolean differentTableType) {
    JobExecutionInfo jobExecutionInfo = new JobExecutionInfo();
    jobExecutionInfo.setJobName("TestJob" + index);
    jobExecutionInfo.setJobId(jobExecutionInfo.getJobName() + "_" + System.currentTimeMillis());
    jobExecutionInfo.setStartTime(System.currentTimeMillis());
    jobExecutionInfo.setState(JobStateEnum.PENDING);
    jobExecutionInfo.setLaunchedTasks(2);
    jobExecutionInfo.setCompletedTasks(0);
    jobExecutionInfo.setLauncherType(LauncherTypeEnum.LOCAL);
    jobExecutionInfo.setTrackingUrl("localhost");

    MetricArray jobMetrics = new MetricArray();
    Metric jobMetric1 = new Metric();
    jobMetric1.setGroup("JOB");
    jobMetric1.setName("jm1");
    jobMetric1.setType(MetricTypeEnum.COUNTER);
    jobMetric1.setValue("100");
    jobMetrics.add(jobMetric1);
    jobExecutionInfo.setMetrics(jobMetrics);

    Map<String, String> jobProperties = Maps.newHashMap();
    jobProperties.put("k" + index, "v" + index);
    jobExecutionInfo.setJobProperties(new StringMap(jobProperties));

    TaskExecutionInfoArray taskExecutionInfos = new TaskExecutionInfoArray();

    TaskExecutionInfo taskExecutionInfo1 = new TaskExecutionInfo();
    taskExecutionInfo1.setJobId(jobExecutionInfo.getJobId());
    taskExecutionInfo1.setTaskId(jobExecutionInfo.getJobId() + "_0");
    taskExecutionInfo1.setStartTime(System.currentTimeMillis());
    taskExecutionInfo1.setState(TaskStateEnum.PENDING);
    taskExecutionInfo1.setLowWatermark(0L);
    taskExecutionInfo1.setHighWatermark(1000L);
    Table table1 = new Table();
    table1.setNamespace("Test");
    table1.setName("Test1");
    table1.setType(TableTypeEnum.SNAPSHOT_ONLY);
    taskExecutionInfo1.setTable(table1);
    MetricArray taskMetrics1 = new MetricArray();
    Metric taskMetric1 = new Metric();
    taskMetric1.setGroup("TASK");
    taskMetric1.setName("tm1");
    taskMetric1.setType(MetricTypeEnum.COUNTER);
    taskMetric1.setValue("100");
    taskMetrics1.add(taskMetric1);
    taskExecutionInfo1.setMetrics(taskMetrics1);
    Map<String, String> taskProperties1 = Maps.newHashMap();
    taskProperties1.put("k1" + index, "v1" + index);
    taskExecutionInfo1.setTaskProperties(new StringMap(taskProperties1));
    taskExecutionInfos.add(taskExecutionInfo1);

    TaskExecutionInfo taskExecutionInfo2 = new TaskExecutionInfo();
    taskExecutionInfo2.setJobId(jobExecutionInfo.getJobId());
    taskExecutionInfo2.setTaskId(jobExecutionInfo.getJobId() + "_1");
    taskExecutionInfo2.setStartTime(System.currentTimeMillis());
    taskExecutionInfo2.setState(TaskStateEnum.PENDING);
    taskExecutionInfo2.setLowWatermark(0L);
    taskExecutionInfo2.setHighWatermark(2000L);
    Table table2 = new Table();
    table2.setNamespace("Test");
    table2.setName("Test2");
    table2.setType(differentTableType ? TableTypeEnum.SNAPSHOT_APPEND : TableTypeEnum.SNAPSHOT_ONLY);
    taskExecutionInfo2.setTable(table2);
    MetricArray taskMetrics2 = new MetricArray();
    Metric taskMetric2 = new Metric();
    taskMetric2.setGroup("TASK");
    taskMetric2.setName("tm2");
    taskMetric2.setType(MetricTypeEnum.COUNTER);
    taskMetric2.setValue("100");
    taskMetrics2.add(taskMetric2);
    taskExecutionInfo2.setMetrics(taskMetrics2);
    Map<String, String> taskProperties2 = Maps.newHashMap();
    taskProperties2.put("k2" + index, "v2" + index);
    taskExecutionInfo2.setTaskProperties(new StringMap(taskProperties2));
    taskExecutionInfos.add(taskExecutionInfo2);

    jobExecutionInfo.setTaskExecutions(taskExecutionInfos);
    this.expectedJobExecutionInfos.add(jobExecutionInfo);

    return jobExecutionInfo;
  }

  private void assertJobExecution(JobExecutionInfo actual, JobExecutionInfo expected) {
    Assert.assertEquals(actual.getJobName(), expected.getJobName());
    Assert.assertEquals(actual.getJobId(), expected.getJobId());
    if (expected.hasDuration()) {
      Assert.assertEquals(actual.getDuration(), expected.getDuration());
    } else {
      Assert.assertEquals(actual.getDuration().longValue(), 0L);
    }
    Assert.assertEquals(actual.getState(), expected.getState());
    Assert.assertEquals(actual.getLaunchedTasks(), expected.getLaunchedTasks());
    Assert.assertEquals(actual.getCompletedTasks(), expected.getCompletedTasks());
    Assert.assertEquals(actual.getLauncherType(), expected.getLauncherType());
    Assert.assertEquals(actual.getTrackingUrl(), expected.getTrackingUrl());

    Assert.assertEquals(actual.getMetrics(), expected.getMetrics());
    for (int i = 0; i < actual.getMetrics().size(); i++) {
      assertMetric(actual.getMetrics().get(i), expected.getMetrics().get(i));
    }

    Assert.assertEquals(actual.getJobProperties(), expected.getJobProperties());

    Assert.assertEquals(actual.getTaskExecutions().size(), expected.getTaskExecutions().size());
    for (int i = 0; i < actual.getTaskExecutions().size(); i++) {
      assertTaskExecution(actual.getTaskExecutions().get(i), expected.getTaskExecutions().get(i));
    }
  }

  private void assertTaskExecution(TaskExecutionInfo actual, TaskExecutionInfo expected) {
    Assert.assertEquals(actual.getJobId(), expected.getJobId());
    Assert.assertEquals(actual.getTaskId(), expected.getTaskId());
    if (expected.hasDuration()) {
      Assert.assertEquals(actual.getDuration(), expected.getDuration());
    } else {
      Assert.assertEquals(actual.getDuration().longValue(), 0L);
    }
    Assert.assertEquals(actual.getState(), expected.getState());
    Assert.assertEquals(actual.getLowWatermark(), expected.getLowWatermark());
    Assert.assertEquals(actual.getHighWatermark(), expected.getHighWatermark());
    Assert.assertEquals(actual.getTable(), expected.getTable());

    Assert.assertEquals(actual.getMetrics(), expected.getMetrics());
    for (int i = 0; i < actual.getMetrics().size(); i++) {
      assertMetric(actual.getMetrics().get(i), expected.getMetrics().get(i));
    }

    Assert.assertEquals(actual.getTaskProperties(), expected.getTaskProperties());
  }

  private void assertMetric(Metric actual, Metric expected) {
    Assert.assertEquals(actual.getGroup(), expected.getGroup());
    Assert.assertEquals(actual.getName(), expected.getName());
    Assert.assertEquals(actual.getType(), expected.getType());
    Assert.assertEquals(actual.getValue(), expected.getValue());
  }
}
