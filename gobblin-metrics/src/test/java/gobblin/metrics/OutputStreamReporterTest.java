/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.metrics"})
public class OutputStreamReporterTest {

  private ByteArrayOutputStream stream = new ByteArrayOutputStream();

  @Test
  public void testReporter() {

    MetricRegistry registry = new MetricRegistry();
    Counter counter = registry.counter("com.linkedin.example.counter");
    Meter meter = registry.meter("com.linkedin.example.meter");
    Histogram histogram = registry.histogram("com.linkedin.example.histogram");

    OutputStreamReporter reporter = OutputStreamReporter.
        forRegistry(registry).
        outputTo(stream).
        build();

    counter.inc();
    meter.mark(2);
    histogram.update(1);
    histogram.update(1);
    histogram.update(2);

    reporter.report();

    String[] lines = stream.toString().split("\n");

    Map<String, Set<String>> expected = new HashMap<String, Set<String>>();
    Set<String> counterSubMetrics = new HashSet<String>();
    counterSubMetrics.add("count");
    expected.put("com.linkedin.example.counter", counterSubMetrics);
    Set<String> histogramSubMetrics = new HashSet<String>();
    histogramSubMetrics.add("count");
    histogramSubMetrics.add("min");
    histogramSubMetrics.add("max");
    histogramSubMetrics.add("mean");
    histogramSubMetrics.add("stddev");
    histogramSubMetrics.add("median");
    histogramSubMetrics.add("75% <");
    histogramSubMetrics.add("95% <");
    expected.put("com.linkedin.example.histogram", histogramSubMetrics);
    Set<String> meterSubmetrics = new HashSet<String>();
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
  public void testTags() {
    MetricRegistry registry = new MetricRegistry();
    Counter counter = registry.counter("com.linkedin.example.counter");

    Map<String, String> tags = new HashMap<String, String>();
    tags.put("testKey", "testValue");
    tags.put("key2", "value2");

    OutputStreamReporter reporter = OutputStreamReporter.
        forRegistry(registry).
        withTags(tags).
        outputTo(stream).
        build();

    counter.inc();

    reporter.report();
    Assert.assertTrue(stream.toString().contains("key2=value2"));
    Assert.assertTrue(stream.toString().contains("testKey=testValue"));
    String[] lines = stream.toString().split("\n");

    Map<String, Set<String>> expected = new HashMap<String, Set<String>>();

    expectMetrics(expected, lines);
    Set<String> counterSubMetrics = new HashSet<String>();
    counterSubMetrics.add("count");
    expected.put("com.linkedin.example.counter", counterSubMetrics);

    reporter.close();

  }

  @Test
  public void testTagsFromContext() {
    Tag<?> tag1 = new Tag<String>("tag1", "value1");
    MetricContext context = MetricContext.builder("context").addTag(tag1).build();
    Counter counter = context.counter("com.linkedin.example.counter");

    OutputStreamReporter reporter = OutputStreamReporter.
        forContext(context).
        outputTo(stream).
        build();

    counter.inc();

    reporter.report();
    Assert.assertTrue(stream.toString().contains("tag1=value1"));
    String[] lines = stream.toString().split("\n");

    Map<String, Set<String>> expected = new HashMap<String, Set<String>>();

    expectMetrics(expected, lines);
    Set<String> counterSubMetrics = new HashSet<String>();
    counterSubMetrics.add("count");
    expected.put("com.linkedin.example.counter", counterSubMetrics);

    reporter.close();

  }

  @BeforeMethod
  public void before() {
    stream.reset();
  }

  private void expectMetrics(Map<String, Set<String>> metrics, String[] lines) {

    Set<String> activeSet = new HashSet<String>();
    String activeTopLevel = "";

    for (String line : lines) {
      System.out.println(line);
      if(line.contains("com.linkedin.example")) {
        Assert.assertTrue(activeSet.isEmpty(),
            String.format("%s does not contain all expected submetrics. Missing: %s",
                activeTopLevel, Arrays.toString(activeSet.toArray())));
        activeTopLevel = line.trim();
        if (metrics.containsKey(activeTopLevel)) {
          activeSet = metrics.get(activeTopLevel);
          metrics.remove(activeTopLevel);
        } else {
          activeSet = new HashSet<String>();
        }
      } else if (line.contains("=")) {
        String submetric = line.split("=")[0].trim();
        activeSet.remove(submetric);
      }
    }

    Assert.assertTrue(activeSet.isEmpty(),
        String.format("%s does not contain all expected submetrics. Missing: %s", activeTopLevel,
            Arrays.toString(activeSet.toArray())));
    Assert.assertTrue(metrics.isEmpty(),
        String.format("Output does not contain all expected top level metrics. Missing: %s",
            Arrays.toString(metrics.keySet().toArray())));

  }


}
