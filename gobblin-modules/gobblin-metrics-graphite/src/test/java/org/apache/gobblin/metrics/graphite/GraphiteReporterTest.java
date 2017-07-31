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

package org.apache.gobblin.metrics.graphite;

import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.Measurements;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;

import java.io.IOException;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import static org.apache.gobblin.metrics.test.TestConstants.METRIC_PREFIX;
import static org.apache.gobblin.metrics.test.TestConstants.GAUGE;
import static org.apache.gobblin.metrics.test.TestConstants.COUNTER;
import static org.apache.gobblin.metrics.test.TestConstants.METER;
import static org.apache.gobblin.metrics.test.TestConstants.HISTOGRAM;
import static org.apache.gobblin.metrics.test.TestConstants.TIMER;
import static org.apache.gobblin.metrics.test.TestConstants.CONTEXT_NAME;


/**
 * Test for GraphiteReporter using a mock backend ({@link TestGraphiteSender})
 *
 * @author Lorand Bendig
 *
 */
@Test(groups = { "gobblin.metrics" })
public class GraphiteReporterTest {

  private static int DEFAULT_PORT = 0;
  private static String DEFAULT_HOST = "localhost";

  private TestGraphiteSender graphiteSender = new TestGraphiteSender();
  private GraphitePusher graphitePusher;

  @BeforeClass
  public void setUp() throws IOException {
    GraphiteConnectionType connectionType = Mockito.mock(GraphiteConnectionType.class);
    Mockito.when(connectionType.createConnection(DEFAULT_HOST, DEFAULT_PORT)).thenReturn(graphiteSender);
    this.graphitePusher = new GraphitePusher(DEFAULT_HOST, DEFAULT_PORT, connectionType);
  }

  @Test
  public void testWithoutTags() throws IOException {
    try (
        MetricContext metricContext =
            MetricContext.builder(this.getClass().getCanonicalName() + ".testGraphiteReporter").build();

        GraphiteReporter graphiteReporter =
            GraphiteReporter.Factory.newBuilder()
                .withGraphitePusher(graphitePusher)
                .withMetricContextName(CONTEXT_NAME)
                .build(new Properties());) {

      ContextAwareGauge<Long> contextAwareGauge =
          metricContext.newContextAwareGauge("com.linkedin.example.gauge", new Gauge<Long>() {
            @Override
            public Long getValue() {
              return 1000l;
            }
          });

      metricContext.register(MetricRegistry.name(METRIC_PREFIX, GAUGE), contextAwareGauge);
      Counter counter = metricContext.counter(MetricRegistry.name(METRIC_PREFIX, COUNTER));
      Meter meter = metricContext.meter(MetricRegistry.name(METRIC_PREFIX, METER));
      Histogram histogram = metricContext.histogram(MetricRegistry.name(METRIC_PREFIX, HISTOGRAM));
      Timer timer = metricContext.timer(MetricRegistry.name(METRIC_PREFIX, TIMER));

      counter.inc(3l);
      meter.mark(1l);
      meter.mark(2l);
      meter.mark(3l);
      histogram.update(1);
      histogram.update(1);
      histogram.update(2);
      timer.update(1, TimeUnit.SECONDS);
      timer.update(2, TimeUnit.SECONDS);
      timer.update(3, TimeUnit.SECONDS);

      graphiteReporter.report(metricContext.getGauges(), metricContext.getCounters(), metricContext.getHistograms(),
          metricContext.getMeters(), metricContext.getTimers(), metricContext.getTagMap());

      Assert.assertEquals(getMetricValue(COUNTER, Measurements.COUNT), Long.toString(3l));

      Assert.assertEquals(getMetricValue(GAUGE, null), Long.toString(1000l));
      Assert.assertTrue(getMetricTimestamp(GAUGE, null) <= System.currentTimeMillis() / 1000l);

      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.PERCENTILE_75TH), Double.toString(2d));
      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.PERCENTILE_98TH), Double.toString(2d));
      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.PERCENTILE_99TH), Double.toString(2d));
      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.PERCENTILE_999TH), Double.toString(2d));

      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.COUNT), Long.toString(3l));
      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.MIN), Long.toString(1l));
      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.MAX), Long.toString(2l));
      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.MEDIAN), Double.toString(1d));
      Assert.assertTrue(Double.valueOf(getMetricValue(HISTOGRAM, Measurements.MEAN)) > 1d);
      Assert.assertTrue(Double.valueOf(getMetricValue(HISTOGRAM, Measurements.STDDEV)) < 0.5d);

      Assert.assertEquals(getMetricValue(METER, Measurements.RATE_1MIN), Double.toString(0d));
      Assert.assertEquals(getMetricValue(METER, Measurements.RATE_5MIN), Double.toString(0d));
      Assert.assertEquals(getMetricValue(METER, Measurements.COUNT), Long.toString(6l));
      Assert.assertTrue(Double.valueOf(getMetricValue(METER, Measurements.MEAN_RATE)) > 0d);

      Assert.assertEquals(getMetricValue(TIMER, Measurements.RATE_1MIN), Double.toString(0d));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.RATE_5MIN), Double.toString(0d));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.PERCENTILE_75TH), Double.toString(3000d));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.PERCENTILE_98TH), Double.toString(3000d));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.PERCENTILE_99TH), Double.toString(3000d));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.PERCENTILE_999TH), Double.toString(3000d));

      Assert.assertEquals(getMetricValue(TIMER, Measurements.COUNT), Long.toString(3l));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.MIN), Double.toString(1000d));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.MAX), Double.toString(3000d));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.MEAN), Double.toString(2000d));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.MEDIAN), Double.toString(2000d));
      Assert.assertTrue(Double.valueOf(getMetricValue(TIMER, Measurements.MEAN_RATE)) > 0d);
      Assert.assertTrue(Double.valueOf(getMetricValue(TIMER, Measurements.STDDEV)) > 0d);

    }
  }

  @Test
  public void testWithTags() throws IOException {

    try (
        MetricContext metricContext =
            MetricContext.builder(this.getClass().getCanonicalName() + ".testGraphiteReporter")
                .addTag(new Tag<String>("taskId", "task_testjob_123"))
                .addTag(new Tag<String>("forkBranchName", "fork_1")).build();

        GraphiteReporter graphiteReporter =
            GraphiteReporter.Factory.newBuilder()
                .withGraphitePusher(graphitePusher)
                .withMetricContextName(CONTEXT_NAME)
                .build(new Properties());) {

      Counter counter = metricContext.counter(MetricRegistry.name(METRIC_PREFIX, COUNTER));
      counter.inc(5l);

      graphiteReporter.report(new TreeMap<String, Gauge>(), metricContext.getCounters(),
          new TreeMap<String, Histogram>(), new TreeMap<String, Meter>(), new TreeMap<String, Timer>(),
          metricContext.getTagMap());

      Assert.assertEquals(getMetricValue("task_testjob_123.fork_1." + METRIC_PREFIX, COUNTER, Measurements.COUNT),
          Long.toString(5l));
    }
  }

  private String getMetricValue(String metric, Measurements key) {
    return getMetricValue(METRIC_PREFIX, metric, key);
  }

  private String getMetricValue(String metricPrefix, String metric, Measurements key) {
    String metricKey =
        (key == null) ? MetricRegistry.name(CONTEXT_NAME, metricPrefix, metric) : MetricRegistry.name(CONTEXT_NAME,
            metricPrefix, metric, key.getName());
    return graphiteSender.getMetric(metricKey).getValue();
  }

  private long getMetricTimestamp(String metric, Measurements key) {
    String metricKey =
        (key == null) ? MetricRegistry.name(CONTEXT_NAME, METRIC_PREFIX, metric) : MetricRegistry.name(CONTEXT_NAME,
            METRIC_PREFIX, metric, key.getName());
    return graphiteSender.getMetric(metricKey).getTimestamp();
  }

}
