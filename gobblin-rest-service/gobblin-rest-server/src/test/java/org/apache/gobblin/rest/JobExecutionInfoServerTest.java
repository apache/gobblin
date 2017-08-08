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

package org.apache.gobblin.rest;

import java.io.IOException;
import java.net.ServerSocket;
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

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.JobHistoryStore;
import org.apache.gobblin.metastore.MetaStoreModule;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;


/**
 * Unit tests for {@link org.apache.gobblin.rest.JobExecutionInfoServer}.
 *
 * <p>
 *     This test case uses {@link org.apache.gobblin.rest.JobExecutionInfoClient} to talk to the
 *     {@link org.apache.gobblin.rest.JobExecutionInfoServer}, which runs the Rest.li resource
 *     {@link org.apache.gobblin.rest.JobExecutionInfoResource}. So this test case is also testing
 *     those classes.
 * </p>
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.rest" })
public class JobExecutionInfoServerTest {
  private ITestMetastoreDatabase testMetastoreDatabase;
  private JobHistoryStore jobHistoryStore;
  private JobExecutionInfoClient client;
  private JobExecutionInfoServer server;
  private JobExecutionInfo expected1;
  private JobExecutionInfo expected2;

  @BeforeClass
  public void setUp() throws Exception {
    testMetastoreDatabase = TestMetastoreDatabaseFactory.get();

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.JOB_HISTORY_STORE_URL_KEY, testMetastoreDatabase.getJdbcUrl());

    int randomPort = chooseRandomPort();
    properties.setProperty(ConfigurationKeys.REST_SERVER_PORT_KEY, Integer.toString(randomPort));

    Injector injector = Guice.createInjector(new MetaStoreModule(properties));
    this.jobHistoryStore = injector.getInstance(JobHistoryStore.class);

    this.client = new JobExecutionInfoClient(String.format("http://%s:%s/", "localhost", randomPort));
    this.server = new JobExecutionInfoServer(properties);
    this.server.startUp();

    this.expected1 = createJobExecutionInfo(1);
    this.expected2 = createJobExecutionInfo(2);

    this.jobHistoryStore.put(this.expected1);
    this.jobHistoryStore.put(this.expected2);
  }

  @Test
  public void testGet() throws Exception {
    JobExecutionQuery queryByJobId = new JobExecutionQuery();
    queryByJobId.setIdType(QueryIdTypeEnum.JOB_ID);
    queryByJobId.setId(JobExecutionQuery.Id.create(this.expected1.getJobId()));

    JobExecutionQueryResult result = this.client.get(queryByJobId);
    JobExecutionInfoArray jobExecutionInfos = result.getJobExecutions();
    Assert.assertEquals(jobExecutionInfos.size(), 1);
    JobExecutionInfo actual = jobExecutionInfos.get(0);
    assertJobExecution(actual, this.expected1);
  }

  @Test
  public void testBadGet() throws Exception {
    JobExecutionQuery queryByJobId = new JobExecutionQuery();
    queryByJobId.setIdType(QueryIdTypeEnum.JOB_ID);
    queryByJobId.setId(JobExecutionQuery.Id.create(this.expected1.getJobId() + "1"));

    JobExecutionQueryResult result = this.client.get(queryByJobId);
    Assert.assertTrue(result.getJobExecutions().isEmpty());
  }

  @Test
  public void testBatchGet() throws Exception {
    JobExecutionQuery queryByJobId1 = new JobExecutionQuery();
    queryByJobId1.setIdType(QueryIdTypeEnum.JOB_ID);
    queryByJobId1.setId(JobExecutionQuery.Id.create(this.expected1.getJobId()));

    JobExecutionQuery queryByJobId2 = new JobExecutionQuery();
    queryByJobId2.setIdType(QueryIdTypeEnum.JOB_ID);
    queryByJobId2.setId(JobExecutionQuery.Id.create(this.expected2.getJobId()));

    List<JobExecutionQuery> queries = Lists.newArrayList(queryByJobId1, queryByJobId2);
    List<JobExecutionQueryResult> result = Lists.newArrayList(this.client.batchGet(queries));
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0).getJobExecutions().size(), 1);
    Assert.assertEquals(result.get(1).getJobExecutions().size(), 1);

    JobExecutionInfo actual1 = result.get(0).getJobExecutions().get(0);
    JobExecutionInfo actual2 = result.get(1).getJobExecutions().get(0);
    if (actual1.getJobName().equals(this.expected1.getJobName())) {
      assertJobExecution(actual1, this.expected1);
      assertJobExecution(actual2, this.expected2);
    } else {
      assertJobExecution(actual1, this.expected2);
      assertJobExecution(actual2, this.expected1);
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() throws Exception {
    if (this.client != null) {
      this.client.close();
    }
    if (this.server != null) {
      this.server.shutDown();
    }
    if (this.jobHistoryStore != null) {
      this.jobHistoryStore.close();
    }
    if (this.testMetastoreDatabase != null) {
      this.testMetastoreDatabase.close();
    }
  }

  private static int chooseRandomPort() throws IOException {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      return socket.getLocalPort();
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }

  private static JobExecutionInfo createJobExecutionInfo(int index) {
    JobExecutionInfo jobExecutionInfo = new JobExecutionInfo();
    jobExecutionInfo.setJobName("TestJob" + index);
    jobExecutionInfo.setJobId(jobExecutionInfo.getJobName() + "_" + System.currentTimeMillis());
    jobExecutionInfo.setStartTime(System.currentTimeMillis());
    jobExecutionInfo.setState(JobStateEnum.PENDING);
    jobExecutionInfo.setLaunchedTasks(2);
    jobExecutionInfo.setCompletedTasks(0);

    MetricArray jobMetrics = new MetricArray();
    Metric jobMetric1 = new Metric();
    jobMetric1.setGroup("JOB");
    jobMetric1.setName("jm1");
    jobMetric1.setType(MetricTypeEnum.COUNTER);
    jobMetric1.setValue("100");
    jobMetrics.add(jobMetric1);
    jobExecutionInfo.setMetrics(jobMetrics);

    Map<String, String> jobProperties = Maps.newHashMap();
    jobProperties.put("k", "v");
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
    taskProperties1.put("k1", "v1");
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
    table2.setType(TableTypeEnum.SNAPSHOT_ONLY);
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
    taskProperties2.put("k2", "v2");
    taskExecutionInfo2.setTaskProperties(new StringMap(taskProperties2));
    taskExecutionInfos.add(taskExecutionInfo2);

    jobExecutionInfo.setTaskExecutions(taskExecutionInfos);

    return jobExecutionInfo;
  }

  private static void assertJobExecution(JobExecutionInfo actual, JobExecutionInfo expected) {
    Assert.assertEquals(actual.getJobName(), expected.getJobName());
    Assert.assertEquals(actual.getJobId(), expected.getJobId());
    if (expected.hasDuration()) {
      Assert.assertEquals(actual.getDuration(), expected.getDuration());
    } else {
      Assert.assertEquals(actual.getDuration().longValue(), -1L);
    }
    Assert.assertEquals(actual.getState(), expected.getState());
    Assert.assertEquals(actual.getLaunchedTasks(), expected.getLaunchedTasks());
    Assert.assertEquals(actual.getCompletedTasks(), expected.getCompletedTasks());

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

  private static void assertTaskExecution(TaskExecutionInfo actual, TaskExecutionInfo expected) {
    Assert.assertEquals(actual.getJobId(), expected.getJobId());
    Assert.assertEquals(actual.getTaskId(), expected.getTaskId());
    if (expected.hasDuration()) {
      Assert.assertEquals(actual.getDuration(), expected.getDuration());
    } else {
      Assert.assertEquals(actual.getDuration().longValue(), -1L);
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

  private static void assertMetric(Metric actual, Metric expected) {
    Assert.assertEquals(actual.getGroup(), expected.getGroup());
    Assert.assertEquals(actual.getName(), expected.getName());
    Assert.assertEquals(actual.getType(), expected.getType());
    Assert.assertEquals(actual.getValue(), expected.getValue());
  }
}
