/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;

import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;


/**
 * Unit tests for {@link InfluxDBJobMetricsStore}.
 *
 * @author ynli
 */
@Test(groups = {"ignore", "gobblin.metrics"})
public class InfluxDBJobMetricsStoreTest {

  private static final String FLUXDB_URL = "http://localhost:8086/";
  private static final String FLUXDB_USER_NAME = "root";
  private static final String FLUXDB_USER_PASSWORD = "root";
  private static final String JOB_METRICS_DB = "gobblin_job_metrics";
  private static final String TASK_METRICS_DB = "gobblin_task_metrics";

  private static final String JOB_NAME_1 = "test1";
  private static final String JOB_ID_1 = JOB_NAME_1 + "_" + System.currentTimeMillis();
  private static final String TASK_ID_1_0 = JOB_ID_1 + "_0";
  private static final String TASK_ID_1_1 = JOB_ID_1 + "_1";
  private static final String JOB_NAME_2 = "test2";
  private static final String JOB_ID_2 = JOB_NAME_2 + "_" + System.currentTimeMillis();
  private static final String TASK_ID_2_0 = JOB_ID_2 + "_0";
  private static final String TASK_ID_2_1 = JOB_ID_2 + "_1";
  private static final String RECORD_COUNTER = "records";
  private static final String BYTE_COUNTER = "bytes";

  private InfluxDB influxDB;
  private JobMetricsStore jobMetricsStore;

  @BeforeClass
  public void setUp() {
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.FLUXDB_URL_KEY, FLUXDB_URL);
    properties.setProperty(ConfigurationKeys.FLUXDB_USER_NAME_KEY, FLUXDB_USER_NAME);
    properties.setProperty(ConfigurationKeys.FLUXDB_USER_PASSWORD_KEY, FLUXDB_USER_PASSWORD);

    this.influxDB = InfluxDBFactory.connect(FLUXDB_URL, FLUXDB_USER_NAME, FLUXDB_USER_PASSWORD);
    this.influxDB.createDatabase(JOB_METRICS_DB);
    this.influxDB.createDatabase(TASK_METRICS_DB);
    Assert.assertTrue(this.influxDB.ping().getStatus().equalsIgnoreCase("ok"));

    this.jobMetricsStore = new InfluxDBJobMetricsStore(properties);
  }

  @Test
  public void testPut()
      throws IOException {
    JobMetrics jobMetrics1 = JobMetrics.get(JOB_NAME_1, JOB_ID_1);

    Counter jobRecordCounter = jobMetrics1.getCounter(JobMetrics.MetricGroup.JOB, JOB_ID_1, RECORD_COUNTER);
    jobRecordCounter.inc(1000);
    Counter jobByteCounter = jobMetrics1.getCounter(JobMetrics.MetricGroup.JOB, JOB_ID_1, BYTE_COUNTER);
    jobByteCounter.inc(1000 * 100);

    Counter taskRecordCounter0 = jobMetrics1.getCounter(JobMetrics.MetricGroup.TASK, TASK_ID_1_0, RECORD_COUNTER);
    taskRecordCounter0.inc(400);
    Counter taskByteCounter0 = jobMetrics1.getCounter(JobMetrics.MetricGroup.TASK, TASK_ID_1_0, BYTE_COUNTER);
    taskByteCounter0.inc(400 * 100);

    Counter taskRecordCounter1 = jobMetrics1.getCounter(JobMetrics.MetricGroup.TASK, TASK_ID_1_1, RECORD_COUNTER);
    taskRecordCounter1.inc(600);
    Counter taskByteCounter1 = jobMetrics1.getCounter(JobMetrics.MetricGroup.TASK, TASK_ID_1_1, BYTE_COUNTER);
    taskByteCounter1.inc(600 * 100);

    this.jobMetricsStore.put(jobMetrics1);

    JobMetrics jobMetrics2 = JobMetrics.get(JOB_NAME_2, JOB_ID_2);

    jobRecordCounter = jobMetrics2.getCounter(JobMetrics.MetricGroup.JOB, JOB_ID_2, RECORD_COUNTER);
    jobRecordCounter.inc(2000);
    jobByteCounter = jobMetrics2.getCounter(JobMetrics.MetricGroup.JOB, JOB_ID_2, BYTE_COUNTER);
    jobByteCounter.inc(2000 * 100);

    taskRecordCounter0 = jobMetrics2.getCounter(JobMetrics.MetricGroup.TASK, TASK_ID_2_0, RECORD_COUNTER);
    taskRecordCounter0.inc(1200);
    taskByteCounter0 = jobMetrics2.getCounter(JobMetrics.MetricGroup.TASK, TASK_ID_2_0, BYTE_COUNTER);
    taskByteCounter0.inc(1200 * 100);

    taskRecordCounter1 = jobMetrics2.getCounter(JobMetrics.MetricGroup.TASK, TASK_ID_2_1, RECORD_COUNTER);
    taskRecordCounter1.inc(800);
    taskByteCounter1 = jobMetrics2.getCounter(JobMetrics.MetricGroup.TASK, TASK_ID_2_1, BYTE_COUNTER);
    taskByteCounter1.inc(800 * 100);

    this.jobMetricsStore.put(jobMetrics2);
  }

  @Test(dependsOnMethods = "testPut")
  public void testGet()
      throws IOException {
    Map<Long, Object> values = this.jobMetricsStore
        .get(JOB_NAME_1, JobMetrics.MetricGroup.JOB, JOB_ID_1, RECORD_COUNTER, MetricNameSuffix.NONE);
    Assert.assertEquals(values.size(), 1);
    Assert.assertTrue(values.values().contains(1000d));

    values = this.jobMetricsStore
        .get(JOB_NAME_1, JobMetrics.MetricGroup.TASK, TASK_ID_1_0, RECORD_COUNTER, MetricNameSuffix.NONE);
    Assert.assertEquals(values.size(), 1);
    Assert.assertTrue(values.values().contains(400d));

    values = this.jobMetricsStore
        .get(JOB_NAME_1, JobMetrics.MetricGroup.TASK, TASK_ID_1_1, BYTE_COUNTER, MetricNameSuffix.NONE);
    Assert.assertEquals(values.size(), 1);
    Assert.assertTrue(values.values().contains(600 * 100d));

    values = this.jobMetricsStore
        .get(JOB_NAME_2, JobMetrics.MetricGroup.JOB, JOB_ID_2, RECORD_COUNTER, MetricNameSuffix.NONE);
    Assert.assertEquals(values.size(), 1);
    Assert.assertTrue(values.values().contains(2000d));

    values = this.jobMetricsStore
        .get(JOB_NAME_2, JobMetrics.MetricGroup.TASK, TASK_ID_2_0, RECORD_COUNTER, MetricNameSuffix.NONE);
    Assert.assertEquals(values.size(), 1);
    Assert.assertTrue(values.values().contains(1200d));

    values = this.jobMetricsStore
        .get(JOB_NAME_2, JobMetrics.MetricGroup.TASK, TASK_ID_2_1, BYTE_COUNTER, MetricNameSuffix.NONE);
    Assert.assertEquals(values.size(), 1);
    Assert.assertTrue(values.values().contains(800 * 100d));
  }

  @Test
  public void testAggregateOnTasks()
      throws IOException {
    Object value = this.jobMetricsStore
        .aggregateOnTask(JOB_NAME_1, TASK_ID_1_0, RECORD_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(((Double) value).longValue(), 400l);

    Map<String, Object> values = this.jobMetricsStore
        .aggregateOnTasks(JOB_NAME_1, JOB_ID_1, RECORD_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(values.size(), 2);
    Assert.assertEquals(((Double) values.get(TASK_ID_1_0)).longValue(), 400l);
    Assert.assertEquals(((Double) values.get(TASK_ID_1_1)).longValue(), 600l);

    value = this.jobMetricsStore
        .aggregateOnTask(JOB_NAME_1, TASK_ID_1_0, BYTE_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(((Double) value).longValue(), 400 * 100l);

    values = this.jobMetricsStore
        .aggregateOnTasks(JOB_NAME_1, JOB_ID_1, BYTE_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(values.size(), 2);
    Assert.assertEquals(((Double) values.get(TASK_ID_1_0)).longValue(), 400 * 100l);
    Assert.assertEquals(((Double) values.get(TASK_ID_1_1)).longValue(), 600 * 100l);

    value = this.jobMetricsStore
        .aggregateOnTask(JOB_NAME_2, TASK_ID_2_0, RECORD_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(((Double) value).longValue(), 1200l);

    values = this.jobMetricsStore
        .aggregateOnTasks(JOB_NAME_2, JOB_ID_2, RECORD_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(values.size(), 2);
    Assert.assertEquals(((Double) values.get(TASK_ID_2_0)).longValue(), 1200l);
    Assert.assertEquals(((Double) values.get(TASK_ID_2_1)).longValue(), 800l);

    value = this.jobMetricsStore
        .aggregateOnTask(JOB_NAME_2, TASK_ID_2_0, BYTE_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(((Double) value).longValue(), 1200 * 100l);

    values = this.jobMetricsStore
        .aggregateOnTasks(JOB_NAME_2, JOB_ID_2, BYTE_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(values.size(), 2);
    Assert.assertEquals(((Double) values.get(TASK_ID_2_0)).longValue(), 1200 * 100l);
    Assert.assertEquals(((Double) values.get(TASK_ID_2_1)).longValue(), 800 * 100l);
  }

  @Test
  public void testAggregateOnJobRuns()
      throws IOException {
    Map<String, Object> values = this.jobMetricsStore
        .aggregateOnJobRuns(JOB_NAME_1, RECORD_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(values.size(), 1);
    Assert.assertEquals(((Double) values.get(JOB_ID_1)).longValue(), 1000l);

    values = this.jobMetricsStore
        .aggregateOnJobRuns(JOB_NAME_1, BYTE_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(values.size(), 1);
    Assert.assertEquals(((Double) values.get(JOB_ID_1)).longValue(), 1000 * 100l);

    values = this.jobMetricsStore
        .aggregateOnJobRuns(JOB_NAME_2, RECORD_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(values.size(), 1);
    Assert.assertEquals(((Double) values.get(JOB_ID_2)).longValue(), 2000l);

    values = this.jobMetricsStore
        .aggregateOnJobRuns(JOB_NAME_2, BYTE_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(values.size(), 1);
    Assert.assertEquals(((Double) values.get(JOB_ID_2)).longValue(), 2000 * 100l);
  }

  @Test
  public void testAggregateOnJob()
      throws IOException {
    Object value =
        this.jobMetricsStore.aggregateOnJob(JOB_NAME_1, RECORD_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(((Double) value).longValue(), 1000l);

    value =
        this.jobMetricsStore.aggregateOnJob(JOB_NAME_1, BYTE_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(((Double) value).longValue(), 1000 * 100l);

    value =
        this.jobMetricsStore.aggregateOnJob(JOB_NAME_2, RECORD_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(((Double) value).longValue(), 2000l);

    value =
        this.jobMetricsStore.aggregateOnJob(JOB_NAME_2, BYTE_COUNTER, MetricNameSuffix.NONE, AggregationFunction.SUM);
    Assert.assertEquals(((Double) value).longValue(), 2000 * 100l);
  }

  @Test
  public void testAggregateOnJobs()
      throws IOException {
    Object value = this.jobMetricsStore
        .aggregateOnJobs(Lists.newArrayList(JOB_NAME_1, JOB_NAME_2), RECORD_COUNTER, MetricNameSuffix.NONE,
            AggregationFunction.SUM);
    Assert.assertEquals(((Double) value).longValue(), 3000l);

    value = this.jobMetricsStore
        .aggregateOnJobs(Lists.newArrayList(JOB_NAME_1, JOB_NAME_2), BYTE_COUNTER, MetricNameSuffix.NONE,
            AggregationFunction.SUM);
    Assert.assertEquals(((Double) value).longValue(), 3000 * 100l);
  }

  @AfterClass
  public void tearDown() {
    this.influxDB.deleteDatabase(JOB_METRICS_DB);
    this.influxDB.deleteDatabase(TASK_METRICS_DB);
  }
}
