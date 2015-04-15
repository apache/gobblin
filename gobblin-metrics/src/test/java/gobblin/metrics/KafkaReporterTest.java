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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;


@Test(groups = {"gobblin.metrics"})
public class KafkaReporterTest extends KafkaTestBase {

  private ObjectMapper mapper;

  public KafkaReporterTest(String topic)
      throws IOException, InterruptedException {
    super(topic);
    mapper = new ObjectMapper();
  }

  public KafkaReporterTest() throws IOException, InterruptedException {
    this("KafkaReporterTest");
  }

  /**
   * Get builder for KafkaReporter (override if testing an extension of KafkaReporter)
   * @param registry metricregistry
   * @return KafkaReporter builder
   */
  public KafkaReporter.Builder<?> getBuilder(MetricRegistry registry) {
    return KafkaReporter.forRegistry(registry);
  }

  public KafkaReporter.Builder<?> getBuilderFromContext(MetricContext context) {
    return KafkaReporter.forContext(context);
  }


  @Test
  public void testKafkaReporter() throws IOException {
    MetricRegistry registry = new MetricRegistry();
    Counter counter = registry.counter("com.linkedin.example.counter");
    Meter meter = registry.meter("com.linkedin.example.meter");
    Histogram histogram = registry.histogram("com.linkedin.example.histogram");

    KafkaReporter kafkaReporter = getBuilder(registry).build("localhost:" + kafkaPort, topic);

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
    expected.put("com.linkedin.example.counter", 1.0);
    expected.put("com.linkedin.example.meter.count", 2.0);
    expected.put("com.linkedin.example.histogram.count", 3.0);
    expectMetricsWithValues(iterator, expected);

    kafkaReporter.report();
    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    Set<String> expectedSet = new HashSet<String>();
    expectedSet.add("com.linkedin.example.counter");
    expectedSet.add("com.linkedin.example.meter.count");
    expectedSet.add("com.linkedin.example.meter.rate.mean");
    expectedSet.add("com.linkedin.example.meter.rate.1m");
    expectedSet.add("com.linkedin.example.meter.rate.5m");
    expectedSet.add("com.linkedin.example.meter.rate.15m");
    expectedSet.add("com.linkedin.example.histogram.mean");
    expectedSet.add("com.linkedin.example.histogram.min");
    expectedSet.add("com.linkedin.example.histogram.max");
    expectedSet.add("com.linkedin.example.histogram.median");
    expectedSet.add("com.linkedin.example.histogram.75percentile");
    expectedSet.add("com.linkedin.example.histogram.95percentile");
    expectedSet.add("com.linkedin.example.histogram.99percentile");
    expectedSet.add("com.linkedin.example.histogram.999percentile");
    expectedSet.add("com.linkedin.example.histogram.count");

    expectMetrics(iterator, expectedSet, true);

    kafkaReporter.close();

  }

  @Test
  public void kafkaReporterTagsEnvHostTest() throws IOException {
    MetricRegistry registry = new MetricRegistry();
    Counter counter = registry.counter("com.linkedin.example.counter");

    Tag<?> tag1 = new Tag<String>("tag1", "value1");
    Tag<?> tag2 = new Tag<Integer>("tag2", 2);

    KafkaReporter kafkaReporter = getBuilder(registry).
        withTags(Lists.newArrayList(tag1, tag2)).
        build("localhost:" + kafkaPort, topic);

    counter.inc();

    kafkaReporter.report();

    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    KafkaReporter.Metric metric = nextMetric(iterator);

    Assert.assertEquals(1, Integer.parseInt(metric.value.toString()));
    Assert.assertEquals(2, metric.tags.size());
    Assert.assertTrue(metric.tags.containsKey(tag1.getKey()));
    Assert.assertEquals(metric.tags.get(tag1.getKey()),
        tag1.getValue().toString());
    Assert.assertTrue(metric.tags.containsKey(tag2.getKey()));
    Assert.assertEquals(metric.tags.get(tag2.getKey()),
        tag2.getValue().toString());
  }

  @Test
  public void kafkaReporterContextTest() throws IOException {
    Tag<?> tag1 = new Tag<String>("tag1", "value1");
    MetricContext context = MetricContext.builder("context").addTag(tag1).build();
    Counter counter = context.counter("com.linkedin.example.counter");

    KafkaReporter kafkaReporter = getBuilderFromContext(context).build("localhost:" + kafkaPort, topic);

    counter.inc();

    kafkaReporter.report();

    try {
      Thread.sleep(100);
    } catch(InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    KafkaReporter.Metric metric = nextMetric(iterator);

    Assert.assertEquals(1, Integer.parseInt(metric.value.toString()));
    Assert.assertEquals(1, metric.tags.size());
    Assert.assertTrue(metric.tags.containsKey(tag1.getKey()));
    Assert.assertEquals(metric.tags.get(tag1.getKey()),
        tag1.getValue().toString());

  }

  /**
   * Expect a list of metrics with specific values.
   * Fail if not all metrics are received, or some metric has the wrong value.
   * @param it ConsumerIterator for Kafka topic
   * @param expected map of expected metric names and their values
   * @throws IOException
   */
  private void expectMetricsWithValues(ConsumerIterator<byte[], byte[]> it, Map<String, Double> expected)
      throws IOException {
    System.out.println("====Checking for expected messages and values. Will list all messages.====");
    try {
      while(it.hasNext()) {
        KafkaReporter.Metric metric = nextMetric(it);
        if (expected.containsKey(metric.name)) {
          Assert.assertEquals(expected.get(metric.name), Double.parseDouble(metric.value.toString()));
          expected.remove(metric.name);
        }
      }
    } catch (ConsumerTimeoutException e) {
      Assert.assertTrue(expected.isEmpty());
    }
  }

  /**
   * Expect a set of metric names. Will fail if not all of these metrics are received.
   * @param it ConsumerIterator for Kafka topic
   * @param expected set of expected metric names
   * @param strict if set to true, will fail if receiving any metric that is not expected
   * @throws IOException
   */
  private void expectMetrics(ConsumerIterator<byte[], byte[]> it, Set<String> expected, boolean strict)
      throws IOException {
    System.out.println("====Checking for expected messages. Will list all messages.====");
    try {
      while(it.hasNext()) {
        KafkaReporter.Metric metric = nextMetric(it);
        //System.out.println(String.format("expectedSet.add(\"%s\")", metric.name));
        if (expected.contains(metric.name)) {
          expected.remove(metric.name);
        } else if (strict) {
          Assert.assertTrue(false, "Message present in kafka not expected: " + metric.toString());
        }
      }
    } catch(ConsumerTimeoutException e) {
      Assert.assertTrue(expected.isEmpty());
    }
  }

  /**
   * Extract the next metric from the Kafka iterator
   * Assumes existence of the metric has already been checked.
   * @param it Kafka ConsumerIterator
   * @return next metric in the stream
   * @throws IOException
   */
  protected KafkaReporter.Metric nextMetric(ConsumerIterator<byte[], byte[]> it) throws IOException {
    String nextMessage = new String(it.next().message());
    KafkaReporter.Metric metric = mapper.readValue(nextMessage, KafkaReporter.Metric.class);
    System.out.println(nextMessage);
    return metric;
  }

  @AfterClass
  public void after() {
    try {
      close();
    } catch(Exception e) {
    }
  }

  @AfterSuite
  public void afterSuite() {
    closeServer();
  }

}
