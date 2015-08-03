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

package gobblin.metrics.graphite;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import com.codahale.metrics.MetricFilter;

import gobblin.metrics.Measurements;
import gobblin.metrics.MetricContext;
import static gobblin.metrics.TestConstants.*;


/**
 * Unit tests for {@link GraphiteReporter}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.metrics.graphite"})
public class GraphiteReporterTest {

  private MetricContext context;
  private TestGraphiteSender testGraphiteSender = new TestGraphiteSender();

  @BeforeClass
  public void setUp() {
    GraphiteReporter.Builder graphiteReporterBuilder = GraphiteReporter.builder(this.testGraphiteSender)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .filter(MetricFilter.ALL);
    this.context = MetricContext.builder(CONTEXT_NAME)
        .addContextAwareScheduledReporter(graphiteReporterBuilder)
        .build();
  }

  @Test
  public void testReportMetrics() {
    Counter recordsProcessed = this.context.contextAwareCounter(RECORDS_PROCESSED);
    recordsProcessed.inc(10l);

    Meter recordProcessRate = this.context.contextAwareMeter(RECORD_PROCESS_RATE);
    recordProcessRate.mark(1l);
    recordProcessRate.mark(2l);
    recordProcessRate.mark(3l);

    Histogram recordSizeDistribution = this.context.contextAwareHistogram(RECORD_SIZE_DISTRIBUTION);
    recordSizeDistribution.update(1);
    recordSizeDistribution.update(2);
    recordSizeDistribution.update(3);

    Timer totalDuration = this.context.contextAwareTimer(TOTAL_DURATION);
    totalDuration.update(1, TimeUnit.SECONDS);
    totalDuration.update(2, TimeUnit.SECONDS);
    totalDuration.update(3, TimeUnit.SECONDS);

    Gauge<Long> queueSize = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return 1000l;
      }
    };
    this.context.register(QUEUE_SIZE, queueSize);

    this.context.reportMetrics();
  }

  @Test(dependsOnMethods = "testReportMetrics")
  public void verifyMetrics() {
    TestGraphiteSender.TimestampedValue recordsProcessed = this.testGraphiteSender.getMetric(
        MetricRegistry.name(CONTEXT_NAME, RECORDS_PROCESSED, Measurements.COUNT.getName()));
    Assert.assertEquals(recordsProcessed.getValue(), Long.toString(10l));

    TestGraphiteSender.TimestampedValue recordsProcessRateCount = this.testGraphiteSender.getMetric(
        MetricRegistry.name(CONTEXT_NAME, RECORD_PROCESS_RATE, Measurements.COUNT.getName()));
    Assert.assertEquals(Long.parseLong(recordsProcessRateCount.getValue()), 6l);

    TestGraphiteSender.TimestampedValue recordsProcessMeanRate = this.testGraphiteSender.getMetric(
        MetricRegistry.name(CONTEXT_NAME, RECORD_PROCESS_RATE, Measurements.MEAN_RATE.getName()));
    Assert.assertTrue(Double.parseDouble(recordsProcessMeanRate.getValue()) > 0d);

    TestGraphiteSender.TimestampedValue recordsSizeDistributionCount = this.testGraphiteSender.getMetric(
        MetricRegistry.name(CONTEXT_NAME, RECORD_SIZE_DISTRIBUTION, Measurements.COUNT.getName()));
    Assert.assertEquals(Long.parseLong(recordsSizeDistributionCount.getValue()), 3l);

    TestGraphiteSender.TimestampedValue recordsSizeDistributionMin = this.testGraphiteSender.getMetric(
        MetricRegistry.name(CONTEXT_NAME, RECORD_SIZE_DISTRIBUTION, Measurements.MIN.getName()));
    Assert.assertEquals(Long.parseLong(recordsSizeDistributionMin.getValue()), 1l);

    TestGraphiteSender.TimestampedValue recordsSizeDistributionMax = this.testGraphiteSender.getMetric(
        MetricRegistry.name(CONTEXT_NAME, RECORD_SIZE_DISTRIBUTION, Measurements.MAX.getName()));
    Assert.assertEquals(Long.parseLong(recordsSizeDistributionMax.getValue()), 3l);

    TestGraphiteSender.TimestampedValue recordsSizeDistributionMedian = this.testGraphiteSender.getMetric(
        MetricRegistry.name(CONTEXT_NAME, RECORD_SIZE_DISTRIBUTION, Measurements.MEDIAN.getName()));
    Assert.assertEquals(Double.parseDouble(recordsSizeDistributionMedian.getValue()), 2d);

    TestGraphiteSender.TimestampedValue totalDurationCount = this.testGraphiteSender.getMetric(
        MetricRegistry.name(CONTEXT_NAME, TOTAL_DURATION, Measurements.COUNT.getName()));
    Assert.assertEquals(Long.parseLong(totalDurationCount.getValue()), 3l);

    TestGraphiteSender.TimestampedValue totalDurationMin = this.testGraphiteSender.getMetric(
        MetricRegistry.name(CONTEXT_NAME, TOTAL_DURATION, Measurements.MIN.getName()));
    Assert.assertEquals(Double.parseDouble(totalDurationMin.getValue()), 1000d);

    TestGraphiteSender.TimestampedValue totalDurationMax = this.testGraphiteSender.getMetric(
        MetricRegistry.name(CONTEXT_NAME, TOTAL_DURATION, Measurements.MAX.getName()));
    Assert.assertEquals(Double.parseDouble(totalDurationMax.getValue()), 3000d);

    TestGraphiteSender.TimestampedValue totalDurationMedian = this.testGraphiteSender.getMetric(
        MetricRegistry.name(CONTEXT_NAME, TOTAL_DURATION, Measurements.MEDIAN.getName()));
    Assert.assertEquals(Double.parseDouble(totalDurationMedian.getValue()), 2000d);

    TestGraphiteSender.TimestampedValue queueSize = this.testGraphiteSender.getMetric(
        MetricRegistry.name(CONTEXT_NAME, QUEUE_SIZE));
    Assert.assertEquals(Long.parseLong(queueSize.getValue()), 1000l);
    Assert.assertTrue(queueSize.getTimestamp() <= System.currentTimeMillis() / 1000l);
  }

  @AfterClass
  public void tearDown() throws IOException {
    if (this.context != null) {
      this.context.close();
    }
  }
}
