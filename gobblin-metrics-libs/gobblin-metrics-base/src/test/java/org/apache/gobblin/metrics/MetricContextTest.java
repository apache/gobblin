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

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;

import static org.apache.gobblin.metrics.test.TestConstants.*;

import org.apache.gobblin.metrics.reporter.ContextAwareScheduledReporter;


/**
 * Unit tests for {@link MetricContext}.
 *
 * <p>
 *   This test class also tests classes {@link ContextAwareCounter}, {@link ContextAwareMeter},
 *   {@link ContextAwareHistogram}, {@link ContextAwareTimer}, {@link ContextAwareGauge},
 *   {@link org.apache.gobblin.metrics.reporter.ContextAwareScheduledReporter}, and {@link TagBasedMetricFilter}.
 * </p>
 *
 * @author Yinan Li
 */
@Test(groups = {"gobblin.metrics"})
public class MetricContextTest {

  private static final String CHILD_CONTEXT_NAME = "TestChildContext";
  private static final String JOB_ID_KEY = "job.id";
  private static final String JOB_ID_PREFIX = "TestJob-";
  private static final String TASK_ID_KEY = "task.id";
  private static final String TASK_ID_PREFIX = "TestTask-";
  private static final String METRIC_GROUP_KEY = "metric.group";
  private static final String INPUT_RECORDS_GROUP = "INPUT_RECORDS";
  private static final String TEST_REPORTER_NAME = TestContextAwareScheduledReporter.class.getName();

  private MetricContext context;
  private MetricContext childContext;

  @BeforeClass
  public void setUp() {
    String contextName = CONTEXT_NAME + "_" + UUID.randomUUID().toString();
    this.context = MetricContext.builder(contextName)
        .addTag(new Tag<String>(JOB_ID_KEY, JOB_ID_PREFIX + 0))
        .build();

    Assert.assertEquals(this.context.getName(), contextName);
    Assert.assertTrue(this.context.getParent().isPresent());
    Assert.assertEquals(this.context.getParent().get(), RootMetricContext.get());
    Assert.assertEquals(this.context.getTags().size(), 3); // uuid and name tag gets added automatically
    Assert.assertEquals(this.context.getTags().get(0).getKey(), JOB_ID_KEY);
    Assert.assertEquals(this.context.getTags().get(0).getValue(), JOB_ID_PREFIX + 0);
    // Second tag should be uuid
    Assert.assertTrue(this.context.getTags().get(1).getValue().toString()
        .matches("[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"));
  }

  @Test
  public void testChildContext() {
    this.childContext = this.context.childBuilder(CHILD_CONTEXT_NAME)
        .addTag(new Tag<String>(TASK_ID_KEY, TASK_ID_PREFIX + 0))
        .build();

    Assert.assertEquals(this.childContext.getName(), CHILD_CONTEXT_NAME);
    Assert.assertTrue(this.childContext.getParent().isPresent());
    Assert.assertEquals(this.childContext.getParent().get(), this.context);
    Assert.assertEquals(this.childContext.getTags().size(), 4);
    Assert.assertEquals(this.childContext.getTags().get(0).getKey(), JOB_ID_KEY);
    Assert.assertEquals(this.childContext.getTags().get(0).getValue(), JOB_ID_PREFIX + 0);
    Assert.assertEquals(this.childContext.getTags().get(1).getKey(), MetricContext.METRIC_CONTEXT_ID_TAG_NAME);
    Assert.assertEquals(this.childContext.getTags().get(2).getKey(), MetricContext.METRIC_CONTEXT_NAME_TAG_NAME);
    Assert.assertEquals(this.childContext.getTags().get(3).getKey(), TASK_ID_KEY);
    Assert.assertEquals(this.childContext.getTags().get(3).getValue(), TASK_ID_PREFIX + 0);
  }

  @Test(dependsOnMethods = "testChildContext")
  public void testContextAwareCounter() {
    ContextAwareCounter jobRecordsProcessed = this.context.contextAwareCounter(RECORDS_PROCESSED);
    Assert.assertEquals(this.context.getCounters().get(jobRecordsProcessed.getName()),
        jobRecordsProcessed.getInnerMetric());
    Assert.assertEquals(jobRecordsProcessed.getContext(), this.context);
    Assert.assertEquals(jobRecordsProcessed.getName(), RECORDS_PROCESSED);

    jobRecordsProcessed.inc();
    Assert.assertEquals(jobRecordsProcessed.getCount(), 1l);
    jobRecordsProcessed.inc(5);
    Assert.assertEquals(jobRecordsProcessed.getCount(), 6l);
    jobRecordsProcessed.dec();
    Assert.assertEquals(jobRecordsProcessed.getCount(), 5l);
    jobRecordsProcessed.dec(3);
    Assert.assertEquals(jobRecordsProcessed.getCount(), 2l);

    ContextAwareCounter taskRecordsProcessed = this.childContext.contextAwareCounter(RECORDS_PROCESSED);
    Assert.assertEquals(this.childContext.getCounters()
            .get(taskRecordsProcessed.getName()),
        taskRecordsProcessed.getInnerMetric());
    Assert.assertEquals(taskRecordsProcessed.getContext(), this.childContext);
    Assert.assertEquals(taskRecordsProcessed.getName(), RECORDS_PROCESSED);

    taskRecordsProcessed.inc();
    Assert.assertEquals(taskRecordsProcessed.getCount(), 1l);
    Assert.assertEquals(jobRecordsProcessed.getCount(), 3l);
    taskRecordsProcessed.inc(3);
    Assert.assertEquals(taskRecordsProcessed.getCount(), 4l);
    Assert.assertEquals(jobRecordsProcessed.getCount(), 6l);
    taskRecordsProcessed.dec(4);
    Assert.assertEquals(taskRecordsProcessed.getCount(), 0l);
    Assert.assertEquals(jobRecordsProcessed.getCount(), 2l);
  }

  @Test(dependsOnMethods = "testChildContext")
  public void testContextAwareMeter() {
    ContextAwareMeter jobRecordsProcessRate = this.context.contextAwareMeter(RECORD_PROCESS_RATE);
    Assert.assertEquals(this.context.getMeters()
            .get(jobRecordsProcessRate.getName()),
        jobRecordsProcessRate.getInnerMetric());
    Assert.assertEquals(jobRecordsProcessRate.getContext(), this.context);
    Assert.assertEquals(jobRecordsProcessRate.getName(), RECORD_PROCESS_RATE);

    jobRecordsProcessRate.mark();
    jobRecordsProcessRate.mark(3);
    Assert.assertEquals(jobRecordsProcessRate.getCount(), 4l);

    ContextAwareMeter taskRecordsProcessRate = this.childContext.contextAwareMeter(RECORD_PROCESS_RATE);
    Assert.assertEquals(this.childContext.getMeters()
            .get(taskRecordsProcessRate.getName()),
        taskRecordsProcessRate.getInnerMetric());
    Assert.assertEquals(taskRecordsProcessRate.getContext(), this.childContext);
    Assert.assertEquals(taskRecordsProcessRate.getName(), RECORD_PROCESS_RATE);

    taskRecordsProcessRate.mark(2);
    Assert.assertEquals(taskRecordsProcessRate.getCount(), 2l);
    Assert.assertEquals(jobRecordsProcessRate.getCount(), 6l);
    taskRecordsProcessRate.mark(5);
    Assert.assertEquals(taskRecordsProcessRate.getCount(), 7l);
    Assert.assertEquals(jobRecordsProcessRate.getCount(), 11l);
  }

  @Test(dependsOnMethods = "testChildContext")
  public void testContextAwareHistogram() {
    ContextAwareHistogram jobRecordSizeDist = this.context.contextAwareHistogram(RECORD_SIZE_DISTRIBUTION);
    Assert.assertEquals(
        this.context.getHistograms().get(
            jobRecordSizeDist.getName()),
        jobRecordSizeDist.getInnerMetric());
    Assert.assertEquals(jobRecordSizeDist.getContext(), this.context);
    Assert.assertEquals(jobRecordSizeDist.getName(), RECORD_SIZE_DISTRIBUTION);

    jobRecordSizeDist.update(2);
    jobRecordSizeDist.update(4);
    jobRecordSizeDist.update(7);
    Assert.assertEquals(jobRecordSizeDist.getCount(), 3l);
    Assert.assertEquals(jobRecordSizeDist.getSnapshot().getMin(), 2l);
    Assert.assertEquals(jobRecordSizeDist.getSnapshot().getMax(), 7l);

    ContextAwareHistogram taskRecordSizeDist = this.childContext.contextAwareHistogram(RECORD_SIZE_DISTRIBUTION);
    Assert.assertEquals(this.childContext.getHistograms().get(taskRecordSizeDist.getName()),
        taskRecordSizeDist.getInnerMetric());
    Assert.assertEquals(taskRecordSizeDist.getContext(), this.childContext);
    Assert.assertEquals(taskRecordSizeDist.getName(), RECORD_SIZE_DISTRIBUTION);

    taskRecordSizeDist.update(3);
    taskRecordSizeDist.update(14);
    taskRecordSizeDist.update(11);
    Assert.assertEquals(taskRecordSizeDist.getCount(), 3l);
    Assert.assertEquals(taskRecordSizeDist.getSnapshot().getMin(), 3l);
    Assert.assertEquals(taskRecordSizeDist.getSnapshot().getMax(), 14l);
    Assert.assertEquals(jobRecordSizeDist.getCount(), 6l);
    Assert.assertEquals(jobRecordSizeDist.getSnapshot().getMin(), 2l);
    Assert.assertEquals(jobRecordSizeDist.getSnapshot().getMax(), 14l);
  }

  @Test
  public void testContextAwareTimer() {
    ContextAwareTimer jobTotalDuration = this.context.contextAwareTimer(TOTAL_DURATION);
    Assert.assertEquals(this.context.getTimers().get(jobTotalDuration.getName()), jobTotalDuration.getInnerMetric());
    Assert.assertEquals(jobTotalDuration.getContext(), this.context);
    Assert.assertEquals(jobTotalDuration.getName(), TOTAL_DURATION);

    jobTotalDuration.update(50, TimeUnit.SECONDS);
    jobTotalDuration.update(100, TimeUnit.SECONDS);
    jobTotalDuration.update(150, TimeUnit.SECONDS);
    Assert.assertEquals(jobTotalDuration.getCount(), 3l);
    Assert.assertEquals(jobTotalDuration.getSnapshot().getMin(), TimeUnit.SECONDS.toNanos(50l));
    Assert.assertEquals(jobTotalDuration.getSnapshot().getMax(), TimeUnit.SECONDS.toNanos(150l));

    Assert.assertTrue(jobTotalDuration.time().stop() >= 0l);
  }

  @Test
  public void testTaggableGauge() {
    ContextAwareGauge<Long> queueSize = this.context.newContextAwareGauge(
        QUEUE_SIZE,
        new Gauge<Long>() {
          @Override
          public Long getValue() {
            return 1000l;
          }
        });
    this.context.register(QUEUE_SIZE, queueSize);

    Assert.assertEquals(queueSize.getValue().longValue(), 1000l);

    Assert.assertEquals(
        this.context.getGauges().get(queueSize.getName()),
        queueSize.getInnerMetric());
    Assert.assertEquals(queueSize.getContext(), this.context);
    Assert.assertEquals(queueSize.getName(), QUEUE_SIZE);

  }

  @Test(dependsOnMethods = {
      "testContextAwareCounter",
      "testContextAwareMeter",
      "testContextAwareHistogram",
      "testContextAwareTimer",
      "testTaggableGauge"
  })
  public void testGetMetrics() {
    SortedSet<String> names = this.context.getNames();
    Assert.assertEquals(names.size(), 6);
    Assert.assertTrue(names.contains(RECORDS_PROCESSED));
    Assert.assertTrue(names.contains(RECORD_PROCESS_RATE));
    Assert.assertTrue(
        names.contains(RECORD_SIZE_DISTRIBUTION));
    Assert.assertTrue(names.contains(TOTAL_DURATION));
    Assert.assertTrue(names.contains(QUEUE_SIZE));

    SortedSet<String> childNames = this.childContext.getNames();
    Assert.assertEquals(childNames.size(), 4);
    Assert.assertTrue(
        childNames.contains(RECORDS_PROCESSED));
    Assert.assertTrue(
        childNames.contains(RECORD_PROCESS_RATE));
    Assert.assertTrue(
        childNames.contains(RECORD_SIZE_DISTRIBUTION));

    Map<String, Metric> metrics = this.context.getMetrics();
    Assert.assertEquals(metrics.size(), 6);
    Assert.assertTrue(
        metrics.containsKey(RECORDS_PROCESSED));
    Assert.assertTrue(
        metrics.containsKey(RECORD_PROCESS_RATE));
    Assert.assertTrue(
        metrics.containsKey(RECORD_SIZE_DISTRIBUTION));
    Assert.assertTrue(metrics.containsKey(TOTAL_DURATION));
    Assert.assertTrue(metrics.containsKey(QUEUE_SIZE));

    Map<String, Counter> counters = this.context.getCounters();
    Assert.assertEquals(counters.size(), 1);
    Assert.assertTrue(
        counters.containsKey(RECORDS_PROCESSED));

    Map<String, Meter> meters = this.context.getMeters();
    Assert.assertEquals(meters.size(), 1);
    Assert.assertTrue(
        meters.containsKey(RECORD_PROCESS_RATE));

    Map<String, Histogram> histograms = this.context.getHistograms();
    Assert.assertEquals(histograms.size(), 1);
    Assert.assertTrue(
        histograms.containsKey(RECORD_SIZE_DISTRIBUTION));

    Map<String, Timer> timers = this.context.getTimers();
    Assert.assertEquals(timers.size(), 2);
    Assert.assertTrue(timers.containsKey(TOTAL_DURATION));

    Map<String, Gauge> gauges = this.context.getGauges();
    Assert.assertEquals(gauges.size(), 1);
    Assert.assertTrue(gauges.containsKey(QUEUE_SIZE));
  }

  @Test(dependsOnMethods = "testGetMetrics")
  @SuppressWarnings("unchecked")
  public void testGetMetricsWithFilter() {
    MetricFilter filter = new MetricFilter() {
      @Override public boolean matches(String name, Metric metric) {
        return !name.equals(MetricContext.GOBBLIN_METRICS_NOTIFICATIONS_TIMER_NAME);
      }
    };

    Map<String, Counter> counters = this.context.getCounters(filter);
    Assert.assertEquals(counters.size(), 1);
    Assert.assertTrue(
        counters.containsKey(RECORDS_PROCESSED));

    Map<String, Meter> meters = this.context.getMeters(filter);
    Assert.assertEquals(meters.size(), 1);
    Assert.assertTrue(
        meters.containsKey(RECORD_PROCESS_RATE));

    Map<String, Histogram> histograms = this.context.getHistograms(filter);
    Assert.assertEquals(histograms.size(), 1);
    Assert.assertTrue(
        histograms.containsKey(RECORD_SIZE_DISTRIBUTION));

    Map<String, Timer> timers = this.context.getTimers(filter);
    Assert.assertEquals(timers.size(), 1);
    Assert.assertTrue(timers.containsKey(TOTAL_DURATION));

    Map<String, Gauge> gauges = this.context.getGauges(filter);
    Assert.assertEquals(gauges.size(), 1);
    Assert.assertTrue(gauges.containsKey(QUEUE_SIZE));
  }

  @Test(dependsOnMethods = {
      "testGetMetricsWithFilter"
  })
  public void testRemoveMetrics() {
    Assert.assertTrue(this.childContext.remove(RECORDS_PROCESSED));
    Assert.assertTrue(this.childContext.getCounters().isEmpty());

    Assert.assertTrue(this.childContext.remove(RECORD_PROCESS_RATE));
    Assert.assertTrue(this.childContext.getMeters().isEmpty());

    Assert.assertTrue(this.childContext.remove(RECORD_SIZE_DISTRIBUTION));
    Assert.assertTrue(this.childContext.getHistograms().isEmpty());

    Assert.assertEquals(this.childContext.getNames().size(), 1);
  }

  @AfterClass
  public void tearDown() throws IOException {
    if (this.context != null) {
      this.context.close();
    }
  }

  private static class TestContextAwareScheduledReporter extends ContextAwareScheduledReporter {

    protected TestContextAwareScheduledReporter(MetricContext context, String name, MetricFilter filter,
        TimeUnit rateUnit, TimeUnit durationUnit) {
      super(context, name, filter, rateUnit, durationUnit);
    }

    @Override
    protected void reportInContext(MetricContext context,
                                   SortedMap<String, Gauge> gauges,
                                   SortedMap<String, Counter> counters,
                                   SortedMap<String, Histogram> histograms,
                                   SortedMap<String, Meter> meters,
                                   SortedMap<String, Timer> timers) {

      Assert.assertEquals(context.getName(), CONTEXT_NAME);

      Assert.assertEquals(gauges.size(), 1);
      Assert.assertTrue(gauges.containsKey(QUEUE_SIZE));

      Assert.assertEquals(counters.size(), 1);
      Assert.assertTrue(counters.containsKey(RECORDS_PROCESSED));

      Assert.assertEquals(histograms.size(), 1);
      Assert.assertTrue(
          histograms.containsKey(RECORD_SIZE_DISTRIBUTION));

      Assert.assertEquals(meters.size(), 1);
      Assert.assertTrue(meters.containsKey(RECORD_PROCESS_RATE));

      Assert.assertEquals(timers.size(), 2);
      Assert.assertTrue(timers.containsKey(TOTAL_DURATION));
    }

    private static class TestContextAwareScheduledReporterBuilder extends Builder {

      public TestContextAwareScheduledReporterBuilder(String name) {
        super(name);
      }

      @Override
      public ContextAwareScheduledReporter build(MetricContext context) {
        return new MetricContextTest.TestContextAwareScheduledReporter(
            context, this.name, this.filter, this.rateUnit, this.durationUnit);
      }
    }
  }
}
