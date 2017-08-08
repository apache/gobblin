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

package org.apache.gobblin.metrics;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.metrics.reporter.OutputStreamReporter;


@Test(groups = { "gobblin.metrics" })
public class OutputStreamReporterTest {

  private ByteArrayOutputStream stream = new ByteArrayOutputStream();

  @Test
  public void testReporter() throws IOException {

    MetricContext metricContext = MetricContext.builder(this.getClass().getCanonicalName()).build();
    Counter counter = metricContext.counter("com.linkedin.example.counter");
    Meter meter = metricContext.meter("com.linkedin.example.meter");
    Histogram histogram = metricContext.histogram("com.linkedin.example.histogram");

    OutputStreamReporter reporter =
        OutputStreamReporter.Factory.newBuilder().outputTo(this.stream).build(new Properties());

    counter.inc();
    meter.mark(2);
    histogram.update(1);
    histogram.update(1);
    histogram.update(2);

    reporter.report();

    String[] lines = this.stream.toString().split("\n");

    Map<String, Set<String>> expected = new HashMap<>();
    Set<String> counterSubMetrics = new HashSet<>();
    counterSubMetrics.add("count");
    expected.put("com.linkedin.example.counter", counterSubMetrics);
    Set<String> histogramSubMetrics = new HashSet<>();
    histogramSubMetrics.add("count");
    histogramSubMetrics.add("min");
    histogramSubMetrics.add("max");
    histogramSubMetrics.add("mean");
    histogramSubMetrics.add("stddev");
    histogramSubMetrics.add("median");
    histogramSubMetrics.add("75% <");
    histogramSubMetrics.add("95% <");
    expected.put("com.linkedin.example.histogram", histogramSubMetrics);
    Set<String> meterSubmetrics = new HashSet<>();
    meterSubmetrics.add("count");
    meterSubmetrics.add("mean rate");
    meterSubmetrics.add("1-minute rate");
    meterSubmetrics.add("5-minute rate");
    meterSubmetrics.add("15-minute rate");
    expected.put("com.linkedin.example.meter", meterSubmetrics);

    expectMetrics(expected, lines);

    reporter.close();

  }

  @Test
  public void testTags() throws IOException {
    MetricContext metricContext = MetricContext.builder(this.getClass().getCanonicalName()).build();
    Counter counter = metricContext.counter("com.linkedin.example.counter");

    Map<String, String> tags = new HashMap<>();
    tags.put("testKey", "testValue");
    tags.put("key2", "value2");

    OutputStreamReporter reporter =
        OutputStreamReporter.Factory.newBuilder().withTags(tags).outputTo(this.stream).build(new Properties());

    counter.inc();

    reporter.report();
    Assert.assertTrue(this.stream.toString().contains("key2=value2"));
    Assert.assertTrue(this.stream.toString().contains("testKey=testValue"));
    String[] lines = this.stream.toString().split("\n");

    Map<String, Set<String>> expected = new HashMap<>();

    expectMetrics(expected, lines);
    Set<String> counterSubMetrics = new HashSet<>();
    counterSubMetrics.add("count");
    expected.put("com.linkedin.example.counter", counterSubMetrics);

    reporter.close();

  }

  @Test
  public void testTagsFromContext() throws IOException {
    Tag<?> tag1 = new Tag<>("tag1", "value1");
    MetricContext context = MetricContext.builder("context").addTag(tag1).build();
    Counter counter = context.counter("com.linkedin.example.counter");

    OutputStreamReporter reporter =
        OutputStreamReporter.Factory.newBuilder().outputTo(this.stream).build(new Properties());

    counter.inc();

    reporter.report();
    Assert.assertTrue(this.stream.toString().contains("tag1=value1"));
    String[] lines = this.stream.toString().split("\n");

    Map<String, Set<String>> expected = new HashMap<>();

    expectMetrics(expected, lines);
    Set<String> counterSubMetrics = new HashSet<>();
    counterSubMetrics.add("count");
    expected.put("com.linkedin.example.counter", counterSubMetrics);

    reporter.close();

  }

  @BeforeMethod
  public void before() {
    this.stream.reset();
  }

  private void expectMetrics(Map<String, Set<String>> metrics, String[] lines) {

    Set<String> activeSet = new HashSet<>();
    String activeTopLevel = "";

    for (String line : lines) {
      System.out.println(line);
      if (line.contains("com.linkedin.example")) {
        Assert.assertTrue(activeSet.isEmpty(), String.format("%s does not contain all expected submetrics. Missing: %s",
            activeTopLevel, Arrays.toString(activeSet.toArray())));
        activeTopLevel = line.trim();
        if (metrics.containsKey(activeTopLevel)) {
          activeSet = metrics.get(activeTopLevel);
          metrics.remove(activeTopLevel);
        } else {
          activeSet = new HashSet<>();
        }
      } else if (line.contains("=")) {
        String submetric = line.split("=")[0].trim();
        activeSet.remove(submetric);
      }
    }

    Assert.assertTrue(activeSet.isEmpty(), String.format("%s does not contain all expected submetrics. Missing: %s",
        activeTopLevel, Arrays.toString(activeSet.toArray())));
    Assert.assertTrue(metrics.isEmpty(),
        String.format("Output does not contain all expected top level metrics. Missing: %s",
            Arrays.toString(metrics.keySet().toArray())));
  }
}
