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

package gobblin.metrics.graphite;

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
 * Test for GraphiteEventReporter using a mock backend ({@link TestGraphiteSender})
 *
 * @author Lorand Bendig
 *
 */
@Test(groups = { "gobblin.metrics" })
public class GraphiteEventReporterTest {

  private static int DEFAULT_PORT = 0;
  private static String DEFAULT_HOST = "localhost";
  private static String NAMESPACE = "gobblin.metrics.test";

  private TestGraphiteSender graphiteSender = new TestGraphiteSender();
  private GraphitePusher graphitePusher;

  @BeforeClass
  public void setUp() throws IOException {
    GraphiteConnectionType connectionType = Mockito.mock(GraphiteConnectionType.class);
    Mockito.when(connectionType.createConnection(DEFAULT_HOST, DEFAULT_PORT)).thenReturn(graphiteSender);
    this.graphitePusher = new GraphitePusher(DEFAULT_HOST, DEFAULT_PORT, connectionType);
  }

  private GraphiteEventReporter.BuilderImpl getBuilder(MetricContext metricContext) {
    return GraphiteEventReporter.Factory.forContext(metricContext).withGraphitePusher(graphitePusher);
  }

  @Test
  public void testSimpleEvent() throws IOException {
    try (
        MetricContext metricContext =
            MetricContext.builder(this.getClass().getCanonicalName() + ".testGraphiteReporter1").build();

        GraphiteEventReporter graphiteEventReporter = getBuilder(metricContext).withEmitValueAsKey(false).build();) {

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

      graphiteEventReporter.report();

      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

      TimestampedValue retrievedEvent = graphiteSender.getMetric("gobblin.metrics.job1.task1.events.TasksSubmitted");

      Assert.assertEquals(retrievedEvent.getValue(), "0");
      Assert.assertTrue(retrievedEvent.getTimestamp() <= (System.currentTimeMillis() / 1000l));

    }
  }

  @Test
  public void testMultiPartEvent() throws IOException {
    try (
        MetricContext metricContext =
            MetricContext.builder(this.getClass().getCanonicalName() + ".testGraphiteReporter2").build();

        GraphiteEventReporter graphiteEventReporter = getBuilder(metricContext).withEmitValueAsKey(true).build();) {

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

      graphiteEventReporter.report();

      try {
        Thread.sleep(100);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }

      String prefix = "gobblin.metrics.job2.task2.events.JobStateEvent";

      Assert.assertEquals(graphiteSender.getMetric(prefix + ".jobBeginTime").getValue(), "1457736710521");
      Assert.assertEquals(graphiteSender.getMetric(prefix + ".jobEndTime").getValue(), "1457736710734");
      Assert.assertEquals(graphiteSender.getMetric(prefix + ".jobLaunchedTasks").getValue(), "3");
      Assert.assertEquals(graphiteSender.getMetric(prefix + ".jobCompletedTasks").getValue(), "2");
      Assert.assertNotNull(graphiteSender.getMetric(prefix + ".jobState.FAILED"));

    }
  }
}
