package com.linkedin.uif.metrics;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;

/**
 * Unit tests for {@link JobMetrics}.
 *
 * @author ynli
 */
@Test(groups = {"com.linkedin.uif.metrics"})
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

        counter = this.metrics.getCounter(JobMetrics.MetricGroup.TASK, TASK_ID, "records");
        counter.inc();
        Assert.assertEquals(counter.getCount(), 1L);

        counter = this.metrics.getCounter(JobMetrics.MetricGroup.JOB, JOB_ID, "records");
        counter.inc(2);
        Assert.assertEquals(counter.getCount(), 3L);

        counter = this.metrics.getCounter(JobMetrics.MetricGroup.TASK, TASK_ID, "records");
        counter.inc(2);
        Assert.assertEquals(counter.getCount(), 3L);
    }

    @Test(dependsOnMethods = {"testAddMetrics"})
    public void testGetMetrics() {
        Map<String, Metric> metricMap = this.metrics.getMetrics();
        Assert.assertEquals(metricMap.size(), 2);
        Metric metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "records"));
        Assert.assertTrue(metric instanceof Counter);
        Assert.assertEquals(((Counter) metric).getCount(), 3L);
        metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, TASK_ID, "records"));
        Assert.assertTrue(metric instanceof Counter);
        Assert.assertEquals(((Counter) metric).getCount(), 3L);

        metricMap = this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.JOB);
        Assert.assertEquals(metricMap.size(), 1);
        metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "records"));
        Assert.assertTrue(metric instanceof Counter);
        Assert.assertEquals(((Counter) metric).getCount(), 3L);

        metricMap = this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.TASK);
        Assert.assertEquals(metricMap.size(), 1);
        metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, TASK_ID, "records"));
        Assert.assertTrue(metric instanceof Counter);
        Assert.assertEquals(((Counter) metric).getCount(), 3L);
    }

    @Test(dependsOnMethods = {"testGetMetrics"})
    public void testRemoveMetrics() {
        this.metrics.removeMetric(JobMetrics.metricName(JobMetrics.MetricGroup.JOB, JOB_ID, "records"));
        Map<String, Metric> metricMap = this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.TASK);
        Assert.assertEquals(metricMap.size(), 1);
        Metric metric = metricMap.get(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, TASK_ID, "records"));
        Assert.assertEquals(((Counter) metric).getCount(), 3L);

        this.metrics.removeMetric(JobMetrics.metricName(JobMetrics.MetricGroup.TASK, TASK_ID, "records"));
        Assert.assertTrue(this.metrics.getMetricsOfGroup(JobMetrics.MetricGroup.TASK).isEmpty());
    }
}
