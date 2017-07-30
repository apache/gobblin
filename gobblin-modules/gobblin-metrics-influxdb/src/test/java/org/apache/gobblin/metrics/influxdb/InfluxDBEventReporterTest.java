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

package gobblin.metrics.influxdb;

import gobblin.metrics.GobblinTrackingEvent;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.JobEvent;
import gobblin.metrics.event.MultiPartEvent;
import gobblin.metrics.event.TaskEvent;
import gobblin.metrics.test.TimestampedValue;

import java.io.IOException;
import java.util.Map;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;


/**
 * Test for InfluxDBReporter using a mock backend ({@link TestInfluxDB})
 *
 * @author Lorand Bendig
 *
 */
@Test(groups = { "gobblin.metrics" })
public class InfluxDBEventReporterTest {

  private TestInfluxDB influxDB = new TestInfluxDB();
  private InfluxDBPusher influxDBPusher;

  private static String DEFAULT_URL = "http://localhost:8086";
  private static String DEFAULT_USERNAME = "user";
  private static String DEFAULT_PASSWORD = "password";
  private static String DEFAULT_DATABASE = "default";

  private static String NAMESPACE = "gobblin.metrics.test";

  @BeforeClass
  public void setUp() throws IOException {
    InfluxDBConnectionType connectionType = Mockito.mock(InfluxDBConnectionType.class);
    Mockito.when(connectionType.createConnection(DEFAULT_URL, DEFAULT_USERNAME, DEFAULT_PASSWORD)).thenReturn(influxDB);
    this.influxDBPusher =
        new InfluxDBPusher.Builder(DEFAULT_URL, DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_DATABASE, connectionType)
            .build();
  }

  private InfluxDBEventReporter.BuilderImpl getBuilder(MetricContext metricContext) {
    return InfluxDBEventReporter.Factory.forContext(metricContext).withInfluxDBPusher(influxDBPusher);
  }

  @Test
  public void testSimpleEvent() throws IOException {
    try (
        MetricContext metricContext =
            MetricContext.builder(this.getClass().getCanonicalName() + ".testInfluxDBReporter1").build();

        InfluxDBEventReporter influxEventReporter = getBuilder(metricContext).build();) {

      Map<String, String> metadata = Maps.newHashMap();
      metadata.put(JobEvent.METADATA_JOB_ID, "job1");
      metadata.put(TaskEvent.METADATA_TASK_ID, "task1");

      metricContext.submitEvent(GobblinTrackingEvent.newBuilder()
          .setName(JobEvent.TASKS_SUBMITTED)
          .setNamespace(NAMESPACE)
          .setMetadata(metadata).build());

      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

      influxEventReporter.report();

      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

      TimestampedValue retrievedEvent = influxDB.getMetric(
          "gobblin.metrics.job1.task1.events.TasksSubmitted");

      Assert.assertEquals(retrievedEvent.getValue(), "0.0");
      Assert.assertTrue(retrievedEvent.getTimestamp() <= (System.currentTimeMillis()));

    }
  }

  @Test
  public void testMultiPartEvent() throws IOException {
    try (
        MetricContext metricContext =
            MetricContext.builder(this.getClass().getCanonicalName() + ".testInfluxDBReporter2").build();

        InfluxDBEventReporter influxEventReporter = getBuilder(metricContext).build();) {

      Map<String, String> metadata = Maps.newHashMap();
      metadata.put(JobEvent.METADATA_JOB_ID, "job2");
      metadata.put(TaskEvent.METADATA_TASK_ID, "task2");
      metadata.put(EventSubmitter.EVENT_TYPE, "JobStateEvent");
      metadata.put(JobEvent.METADATA_JOB_START_TIME, "1457736710521");
      metadata.put(JobEvent.METADATA_JOB_END_TIME, "1457736710734");
      metadata.put(JobEvent.METADATA_JOB_LAUNCHED_TASKS, "3");
      metadata.put(JobEvent.METADATA_JOB_COMPLETED_TASKS, "2");
      metadata.put(JobEvent.METADATA_JOB_STATE, "FAILED");

      metricContext.submitEvent(GobblinTrackingEvent.newBuilder()
          .setName(MultiPartEvent.JOBSTATE_EVENT.getEventName())
          .setNamespace(NAMESPACE)
          .setMetadata(metadata).build());

      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

      influxEventReporter.report();

      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

      String prefix = "gobblin.metrics.job2.task2.events.JobStateEvent";

      Assert.assertEquals(influxDB.getMetric(prefix + ".jobBeginTime").getValue(), "1457736710521.0");
      Assert.assertEquals(influxDB.getMetric(prefix + ".jobEndTime").getValue(), "1457736710734.0");
      Assert.assertEquals(influxDB.getMetric(prefix + ".jobLaunchedTasks").getValue(), "3.0");
      Assert.assertEquals(influxDB.getMetric(prefix + ".jobCompletedTasks").getValue(), "2.0");
      Assert.assertEquals(influxDB.getMetric(prefix + ".jobState").getValue(), "\"FAILED\"");

    }
  }
}
