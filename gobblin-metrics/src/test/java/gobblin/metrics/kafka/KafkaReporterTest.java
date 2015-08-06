/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

import gobblin.metrics.Measurements;
import gobblin.metrics.Metric;
import gobblin.metrics.MetricContext;
import gobblin.metrics.MetricReport;
import gobblin.metrics.reporter.util.MetricReportUtils;
import gobblin.metrics.Tag;


@Test(groups = {"gobblin.metrics"})
public class KafkaReporterTest {


  public KafkaReporterTest() throws IOException, InterruptedException {
  }

  /**
   * Get builder for KafkaReporter (override if testing an extension of KafkaReporter)
   * @param registry metricregistry
   * @return KafkaReporter builder
   */
  public KafkaReporter.Builder<? extends KafkaReporter.Builder> getBuilder(MetricRegistry registry,
      KafkaPusher pusher) {
    return KafkaReporter.Factory.forRegistry(registry).withKafkaPusher(pusher);
  }

  public KafkaReporter.Builder<? extends KafkaReporter.Builder> getBuilderFromContext(MetricContext context,
      KafkaPusher pusher) {
    return KafkaReporter.Factory.forContext(context).withKafkaPusher(pusher);
  }

  @Test
  public void testKafkaReporter() throws IOException {
    MetricRegistry registry = new MetricRegistry();
    Counter counter = registry.counter("com.linkedin.example.counter");
    Meter meter = registry.meter("com.linkedin.example.meter");
    Histogram histogram = registry.histogram("com.linkedin.example.histogram");

    MockKafkaPusher pusher = new MockKafkaPusher();
    KafkaReporter kafkaReporter = getBuilder(registry, pusher).build("localhost:0000", "topic");

    counter.inc();
    meter.mark(2);
    histogram.update(1);
    histogram.update(1);
    histogram.update(2);

    kafkaReporter.report();

    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    Map<String, Double> expected = new HashMap<String, Double>();
    expected.put("com.linkedin.example.counter." + Measurements.COUNT, 1.0);
    expected.put("com.linkedin.example.meter." + Measurements.COUNT, 2.0);
    expected.put("com.linkedin.example.histogram." + Measurements.COUNT, 3.0);

    MetricReport nextReport = nextReport(pusher.messageIterator());

    expectMetricsWithValues(nextReport, expected);

    kafkaReporter.report();
    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    Set<String> expectedSet = new HashSet<String>();
    expectedSet.add("com.linkedin.example.counter." + Measurements.COUNT);
    expectedSet.add("com.linkedin.example.meter." + Measurements.COUNT);
    expectedSet.add("com.linkedin.example.meter." + Measurements.MEAN_RATE);
    expectedSet.add("com.linkedin.example.meter." + Measurements.RATE_1MIN);
    expectedSet.add("com.linkedin.example.meter." + Measurements.RATE_5MIN);
    expectedSet.add("com.linkedin.example.meter." + Measurements.RATE_15MIN);
    expectedSet.add("com.linkedin.example.histogram." + Measurements.MEAN);
    expectedSet.add("com.linkedin.example.histogram." + Measurements.MIN);
    expectedSet.add("com.linkedin.example.histogram." + Measurements.MAX);
    expectedSet.add("com.linkedin.example.histogram." + Measurements.MEDIAN);
    expectedSet.add("com.linkedin.example.histogram." + Measurements.PERCENTILE_75TH);
    expectedSet.add("com.linkedin.example.histogram." + Measurements.PERCENTILE_95TH);
    expectedSet.add("com.linkedin.example.histogram." + Measurements.PERCENTILE_99TH);
    expectedSet.add("com.linkedin.example.histogram." + Measurements.PERCENTILE_999TH);
    expectedSet.add("com.linkedin.example.histogram." + Measurements.COUNT);

    nextReport = nextReport(pusher.messageIterator());
    expectMetrics(nextReport, expectedSet, true);

    kafkaReporter.close();

  }

  @Test
  public void kafkaReporterTagsTest() throws IOException {
    MetricRegistry registry = new MetricRegistry();
    Counter counter = registry.counter("com.linkedin.example.counter");

    Tag<?> tag1 = new Tag<String>("tag1", "value1");
    Tag<?> tag2 = new Tag<Integer>("tag2", 2);

    MockKafkaPusher pusher = new MockKafkaPusher();
    KafkaReporter kafkaReporter = getBuilder(registry, pusher).
        withTags(Lists.newArrayList(tag1, tag2)).
        build("localhost:0000", "topic");

    counter.inc();

    kafkaReporter.report();

    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    MetricReport metricReport = nextReport(pusher.messageIterator());

    Assert.assertEquals(2, metricReport.getTags().size());
    Assert.assertTrue(metricReport.getTags().containsKey(tag1.getKey()));
    Assert.assertEquals(metricReport.getTags().get(tag1.getKey()),
        tag1.getValue().toString());
    Assert.assertTrue(metricReport.getTags().containsKey(tag2.getKey()));
    Assert.assertEquals(metricReport.getTags().get(tag2.getKey()),
        tag2.getValue().toString());
  }

  @Test
  public void kafkaReporterContextTest() throws IOException {
    Tag<?> tag1 = new Tag<String>("tag1", "value1");
    MetricContext context = MetricContext.builder("context").addTag(tag1).build();
    Counter counter = context.counter("com.linkedin.example.counter");

    MockKafkaPusher pusher = new MockKafkaPusher();
    KafkaReporter kafkaReporter = getBuilderFromContext(context, pusher).build("localhost:0000", "topic");

    counter.inc();

    kafkaReporter.report();

    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    MetricReport metricReport = nextReport(pusher.messageIterator());

    Assert.assertEquals(2, metricReport.getTags().size());
    Assert.assertTrue(metricReport.getTags().containsKey(tag1.getKey()));
    Assert.assertEquals(metricReport.getTags().get(tag1.getKey()),
        tag1.getValue().toString());

  }

  /**
   * Expect a list of metrics with specific values.
   * Fail if not all metrics are received, or some metric has the wrong value.
   * @param report MetricReport.
   * @param expected map of expected metric names and their values
   * @throws IOException
   */
  private void expectMetricsWithValues(MetricReport report, Map<String, Double> expected)
      throws IOException {
    List<Metric> metricIterator = report.getMetrics();

    for(Metric metric : metricIterator) {
      if (expected.containsKey(metric.getName())) {
        Assert.assertEquals(expected.get(metric.getName()), metric.getValue());
        expected.remove(metric.getName());
      }
    }

    Assert.assertTrue(expected.isEmpty());

  }

  /**
   * Expect a set of metric names. Will fail if not all of these metrics are received.
   * @param report MetricReport
   * @param expected set of expected metric names
   * @param strict if set to true, will fail if receiving any metric that is not expected
   * @throws IOException
   */
  private void expectMetrics(MetricReport report, Set<String> expected, boolean strict)
      throws IOException {
    List<Metric> metricIterator = report.getMetrics();
    for(Metric metric : metricIterator) {
      //System.out.println(String.format("expectedSet.add(\"%s\")", metric.name));
      if (expected.contains(metric.getName())) {
        expected.remove(metric.getName());
      } else if (strict) {
        Assert.assertTrue(false, "Metric present in report not expected: " + metric.toString());
      }
    }
    Assert.assertTrue(expected.isEmpty());
  }

  /**
   * Extract the next metric from the Kafka iterator
   * Assumes existence of the metric has already been checked.
   * @param it Kafka ConsumerIterator
   * @return next metric in the stream
   * @throws IOException
   */
  protected MetricReport nextReport(Iterator<byte[]> it) throws IOException {
    Assert.assertTrue(it.hasNext());
    return MetricReportUtils.deserializeReportFromJson(new MetricReport(), it.next());
  }

  @AfterClass
  public void after() {
    try {
    } catch(Exception e) {
    }
  }

  @AfterSuite
  public void afterSuite() {
  }

}
