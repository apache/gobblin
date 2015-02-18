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

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;


/**
 * Unit tests for {@link JobMetrics}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.metrics"})
public class JobMetricsTest {

  private static final String JOB_NAME = "GobblinTest";
  private static final String JOB_ID = JOB_NAME + "_" + System.currentTimeMillis();
  private static final String TASK_ID = JOB_ID + "_0";

  private final JobMetrics metrics = JobMetrics.get(JOB_NAME, JOB_ID);

  @Test
  public void testAddMetrics() {
    Counter counter = this.metrics.getCounter(JobMetrics.MetricGroup.JOB, JOB_ID, "records");
    counter.inc();
    Assert.assertEquals(counter.getCount(), 1L);
    counter = this.metrics.getCounter(JobMetrics.MetricGroup.JOB, JOB_ID, "records");
    counter.inc(2);
    Assert.assertEquals(counter.getCount(), 3L);

    counter = this.metrics.getCounter(JobMetrics.MetricGroup.TASK, TASK_ID, "records");
    counter.inc();
    Assert.assertEquals(counter.getCount(), 1L);
    counter = this.metrics.getCounter(JobMetrics.MetricGroup.TASK, TASK_ID, "records");
    counter.inc(2);
    Assert.assertEquals(counter.getCount(), 3L);

    Meter meter = this.metrics.getMeter(JobMetrics.MetricGroup.JOB, JOB_ID, "recordsPerSec");
    meter.mark(3);
    Assert.assertEquals(meter.getCount(), 3L);
    meter = this.metrics.getMeter(JobMetrics.MetricGroup.TASK, TASK_ID, "recordsPerSec");
    meter.mark(3);
    Assert.assertEquals(meter.getCount(), 3L);

    Gauge<Long> gauge = this.metrics.getGauge(JobMetrics.MetricGroup.JOB, JOB_ID, "expectedRecords", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return 10L;
      }
    });
    Assert.assertEquals(gauge.getValue().longValue(), 10L);
  }

  @Test(dependsOnMethods = {"testAddMetrics"})
  public void testGetMetrics() {
    Map<String, Metric> metricMap = this.metrics.getMetrics();
    Assert.assertEquals(metricMap.size(), 5);
    Metric metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "records"));
    Assert.assertTrue(metric instanceof Counter);
    Assert.assertEquals(((Counter) metric).getCount(), 3L);
    metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, TASK_ID, "records"));
    Assert.assertTrue(metric instanceof Counter);
    Assert.assertEquals(((Counter) metric).getCount(), 3L);
    metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "recordsPerSec"));
    Assert.assertTrue(metric instanceof Meter);
    Assert.assertEquals(((Meter) metric).getCount(), 3L);
    metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, TASK_ID, "recordsPerSec"));
    Assert.assertTrue(metric instanceof Meter);
    Assert.assertEquals(((Meter) metric).getCount(), 3L);
    metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "expectedRecords"));
    Assert.assertTrue(metric instanceof Gauge);
    Assert.assertEquals(((Gauge<Long>) metric).getValue().longValue(), 10L);

    metricMap = this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.JOB);
    Assert.assertEquals(metricMap.size(), 3);
    metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "records"));
    Assert.assertTrue(metric instanceof Counter);
    Assert.assertEquals(((Counter) metric).getCount(), 3L);
    metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "recordsPerSec"));
    Assert.assertTrue(metric instanceof Meter);
    Assert.assertEquals(((Meter) metric).getCount(), 3L);
    metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "expectedRecords"));
    Assert.assertTrue(metric instanceof Gauge);
    Assert.assertEquals(((Gauge<Long>) metric).getValue().longValue(), 10L);

    metricMap = this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.TASK);
    Assert.assertEquals(metricMap.size(), 2);
    metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, TASK_ID, "records"));
    Assert.assertTrue(metric instanceof Counter);
    Assert.assertEquals(((Counter) metric).getCount(), 3L);
    metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, TASK_ID, "recordsPerSec"));
    Assert.assertTrue(metric instanceof Meter);
    Assert.assertEquals(((Meter) metric).getCount(), 3L);

    Map<String, ? extends Metric> metricMap1 =
        this.metrics.getMetricsOfType(JobMetrics.MetricType.COUNTER, JobMetrics.MetricGroup.JOB, JOB_ID);
    Assert.assertEquals(metricMap1.size(), 1);
    Counter counter = (Counter) metricMap1.get(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "records"));
    Assert.assertEquals(counter.getCount(), 3L);
    metricMap1 = this.metrics.getMetricsOfType(JobMetrics.MetricType.COUNTER, JobMetrics.MetricGroup.TASK, TASK_ID);
    Assert.assertEquals(metricMap1.size(), 1);
    counter = (Counter) metricMap1.get(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, TASK_ID, "records"));
    Assert.assertEquals(counter.getCount(), 3L);

    metricMap1 = this.metrics.getMetricsOfType(JobMetrics.MetricType.METER, JobMetrics.MetricGroup.JOB, JOB_ID);
    Assert.assertEquals(metricMap1.size(), 1);
    Meter meter = (Meter) metricMap1.get(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "recordsPerSec"));
    Assert.assertEquals(meter.getCount(), 3L);
    metricMap1 = this.metrics.getMetricsOfType(JobMetrics.MetricType.METER, JobMetrics.MetricGroup.TASK, TASK_ID);
    Assert.assertEquals(metricMap1.size(), 1);
    meter = (Meter) metricMap1.get(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, TASK_ID, "recordsPerSec"));
    Assert.assertEquals(meter.getCount(), 3L);

    metricMap1 = this.metrics.getMetricsOfType(JobMetrics.MetricType.GAUGE, JobMetrics.MetricGroup.JOB, JOB_ID);
    Assert.assertEquals(metricMap1.size(), 1);
    Gauge<Long> gauge =
        (Gauge<Long>) metricMap1.get(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "expectedRecords"));
    Assert.assertEquals(gauge.getValue().longValue(), 10L);
  }

  @Test(dependsOnMethods = {"testGetMetrics"})
  public void testRemoveMetrics() {
    Assert.assertEquals(this.metrics.getMetrics().size(), 5);
    Assert.assertEquals(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.JOB).size(), 3);
    Assert.assertEquals(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.TASK).size(), 2);

    this.metrics.removeMetric(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "records"));
    Assert.assertEquals(this.metrics.getMetrics().size(), 4);
    Assert.assertEquals(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.JOB).size(), 2);
    Assert.assertEquals(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.TASK).size(), 2);

    this.metrics.removeMetric(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "recordsPerSec"));
    Assert.assertEquals(this.metrics.getMetrics().size(), 3);
    Assert.assertEquals(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.JOB).size(), 1);
    Assert.assertEquals(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.TASK).size(), 2);

    this.metrics.removeMetric(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, TASK_ID, "records"));
    Assert.assertEquals(this.metrics.getMetrics().size(), 2);
    Assert.assertEquals(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.JOB).size(), 1);
    Assert.assertEquals(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.TASK).size(), 1);

    this.metrics.removeMetric(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, TASK_ID, "recordsPerSec"));
    Assert.assertEquals(this.metrics.getMetrics().size(), 1);
    Assert.assertEquals(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.JOB).size(), 1);
    Assert.assertTrue(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.TASK).isEmpty());

    this.metrics.removeMetric(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "expectedRecords"));
    Assert.assertTrue(this.metrics.getMetrics().isEmpty());
    Assert.assertTrue(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.JOB).isEmpty());
    Assert.assertTrue(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.TASK).isEmpty());
  }
}
