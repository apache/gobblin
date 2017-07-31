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

package org.apache.gobblin.metrics.influxdb;

import static org.apache.gobblin.metrics.test.TestConstants.CONTEXT_NAME;
import static org.apache.gobblin.metrics.test.TestConstants.COUNTER;
import static org.apache.gobblin.metrics.test.TestConstants.GAUGE;
import static org.apache.gobblin.metrics.test.TestConstants.HISTOGRAM;
import static org.apache.gobblin.metrics.test.TestConstants.METER;
import static org.apache.gobblin.metrics.test.TestConstants.METRIC_PREFIX;
import static org.apache.gobblin.metrics.test.TestConstants.TIMER;
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

/**
 * Test for InfluxDBReporter using a mock backend ({@link TestInfluxDB})
 *
 * @author Lorand Bendig
 *
 */
@Test(groups = { "gobblin.metrics" })
public class InfluxDBReporterTest {

  private TestInfluxDB influxDB = new TestInfluxDB();
  private InfluxDBPusher influxDBPusher;

  private static String DEFAULT_URL = "http://localhost:8086";
  private static String DEFAULT_USERNAME = "user";
  private static String DEFAULT_PASSWORD = "password";
  private static String DEFAULT_DATABASE = "default";

  @BeforeClass
  public void setUp() throws IOException {
    InfluxDBConnectionType connectionType = Mockito.mock(InfluxDBConnectionType.class);
    Mockito.when(connectionType.createConnection(DEFAULT_URL, DEFAULT_USERNAME, DEFAULT_PASSWORD)).thenReturn(influxDB);
    this.influxDBPusher =
        new InfluxDBPusher.Builder(DEFAULT_URL, DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_DATABASE, connectionType)
            .build();
  }

  @Test
  public void testWithoutTags() throws IOException {

    try (
        MetricContext metricContext =
            MetricContext.builder(this.getClass().getCanonicalName() + ".testInfluxDBReporter").build();

        InfluxDBReporter influxDBReporter = InfluxDBReporter.Factory.newBuilder()
            .withInfluxDBPusher(influxDBPusher)
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

      influxDBReporter.report(metricContext.getGauges(), metricContext.getCounters(), metricContext.getHistograms(),
          metricContext.getMeters(), metricContext.getTimers(), metricContext.getTagMap());

      //InfluxDB converts all values to float64 internally
      Assert.assertEquals(getMetricValue(COUNTER, Measurements.COUNT), Float.toString(3f));

      Assert.assertEquals(getMetricValue(GAUGE, null), Float.toString(1000l));
      Assert.assertTrue(getMetricTimestamp(GAUGE, null) <= System.currentTimeMillis());

      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.PERCENTILE_75TH), Float.toString(2f));
      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.PERCENTILE_98TH), Float.toString(2f));
      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.PERCENTILE_99TH), Float.toString(2f));
      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.PERCENTILE_999TH), Float.toString(2f));

      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.COUNT), Float.toString(3f));
      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.MIN), Float.toString(1f));
      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.MAX), Float.toString(2f));
      Assert.assertEquals(getMetricValue(HISTOGRAM, Measurements.MEDIAN), Float.toString(1f));
      Assert.assertTrue(Double.valueOf(getMetricValue(HISTOGRAM, Measurements.MEAN)) > 1f);
      Assert.assertTrue(Double.valueOf(getMetricValue(HISTOGRAM, Measurements.STDDEV)) < 0.5f);

      Assert.assertEquals(getMetricValue(METER, Measurements.RATE_1MIN), Float.toString(0f));
      Assert.assertEquals(getMetricValue(METER, Measurements.RATE_5MIN), Float.toString(0f));
      Assert.assertEquals(getMetricValue(METER, Measurements.COUNT), Float.toString(6f));
      Assert.assertTrue(Double.valueOf(getMetricValue(METER, Measurements.MEAN_RATE)) > 0f);

      Assert.assertEquals(getMetricValue(TIMER, Measurements.RATE_1MIN), Float.toString(0f));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.RATE_5MIN), Float.toString(0f));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.PERCENTILE_75TH), Float.toString(3000f));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.PERCENTILE_98TH), Float.toString(3000f));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.PERCENTILE_99TH), Float.toString(3000f));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.PERCENTILE_999TH), Float.toString(3000f));

      Assert.assertEquals(getMetricValue(TIMER, Measurements.COUNT), Float.toString(3f));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.MIN), Float.toString(1000f));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.MAX), Float.toString(3000f));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.MEAN), Float.toString(2000f));
      Assert.assertEquals(getMetricValue(TIMER, Measurements.MEDIAN), Float.toString(2000f));
      Assert.assertTrue(Double.valueOf(getMetricValue(TIMER, Measurements.MEAN_RATE)) > 0f);
      Assert.assertTrue(Double.valueOf(getMetricValue(TIMER, Measurements.STDDEV)) > 0f);

    }
  }

  @Test
  public void testWithTags() throws IOException {

    try (
        MetricContext metricContext =
            MetricContext.builder(this.getClass().getCanonicalName() + ".testGraphiteReporter")
                .addTag(new Tag<String>("taskId", "task_testjob_123"))
                .addTag(new Tag<String>("forkBranchName", "fork_1")).build();

        InfluxDBReporter influxDBReporter = InfluxDBReporter.Factory.newBuilder()
            .withInfluxDBPusher(influxDBPusher)
            .withMetricContextName(CONTEXT_NAME)
            .build(new Properties());) {

      Counter counter = metricContext.counter(MetricRegistry.name(METRIC_PREFIX, COUNTER));
      counter.inc(5l);

      influxDBReporter.report(new TreeMap<String, Gauge>(), metricContext.getCounters(),
          new TreeMap<String, Histogram>(), new TreeMap<String, Meter>(), new TreeMap<String, Timer>(),
          metricContext.getTagMap());

      //InfluxDB converts all values to float64 internally
      Assert.assertEquals(getMetricValue("task_testjob_123.fork_1." + METRIC_PREFIX, COUNTER, Measurements.COUNT),
          Float.toString(5f));
    }
  }

  private String getMetricValue(String metric, Measurements key) {
    return getMetricValue(METRIC_PREFIX, metric, key);
  }

  private String getMetricValue(String metricPrefix, String metric, Measurements key) {
    String metricKey =
        (key == null) ? MetricRegistry.name(CONTEXT_NAME, metricPrefix, metric) : MetricRegistry.name(CONTEXT_NAME,
            metricPrefix, metric, key.getName());
    return influxDB.getMetric(metricKey).getValue();
  }

  private long getMetricTimestamp(String metric, Measurements key) {
    String metricKey =
        (key == null) ? MetricRegistry.name(CONTEXT_NAME, METRIC_PREFIX, metric) : MetricRegistry.name(CONTEXT_NAME,
            METRIC_PREFIX, metric, key.getName());
    return influxDB.getMetric(metricKey).getTimestamp();
  }

}
