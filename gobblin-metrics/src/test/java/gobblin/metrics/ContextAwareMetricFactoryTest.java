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

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;


/**
 * Unit tests for {@link ContextAwareMetricFactory}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.metrics"})
public class ContextAwareMetricFactoryTest {

  private static final String CONTEXT_NAME = "TestContext";
  private static final String JOB_ID_KEY = "job.id";
  private static final String JOB_ID = "TestJob-0";
  private static final String RECORDS_PROCESSED = "recordsProcessed";
  private static final String RECORD_PROCESS_RATE = "recordProcessRate";
  private static final String RECORD_SIZE_DISTRIBUTION = "recordSizeDistribution";
  private static final String TOTAL_DURATION = "totalDuration";

  private MetricContext context;
  private ContextAwareCounter counter;
  private ContextAwareMeter meter;
  private ContextAwareHistogram histogram;
  private ContextAwareTimer timer;

  @BeforeClass
  public void setUp() {
    this.context = MetricContext.builder(CONTEXT_NAME).build();

    this.counter = ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_COUNTER_FACTORY.newMetric(
        this.context, RECORDS_PROCESSED);
    this.counter.addTag(new Tag<String>(JOB_ID_KEY, JOB_ID));
    this.meter = ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_METER_FACTORY.newMetric(
        this.context, RECORD_PROCESS_RATE);
    this.meter.addTag(new Tag<String>(JOB_ID_KEY, JOB_ID));
    this.histogram = ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_HISTOGRAM_FACTORY.newMetric(
        this.context, RECORD_SIZE_DISTRIBUTION);
    this.histogram.addTag(new Tag<String>(JOB_ID_KEY, JOB_ID));
    this.timer = ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_TIMER_FACTORY.newMetric(
        this.context, TOTAL_DURATION);
    this.timer.addTag(new Tag<String>(JOB_ID_KEY, JOB_ID));
  }

  @Test
  public void testContextAwareCounterFactory() {
    Assert.assertTrue(ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_COUNTER_FACTORY.isInstance(this.counter));
    Assert.assertFalse(ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_COUNTER_FACTORY.isInstance(this.meter));
    Assert.assertEquals(this.counter.getContext(), this.context);
    Assert.assertEquals(this.counter.getName(), RECORDS_PROCESSED);
    Assert.assertEquals(this.counter.getFullyQualifiedName(false), MetricRegistry.name(JOB_ID, RECORDS_PROCESSED));
  }

  @Test
  public void testContextAwareMeterFactory() {
    Assert.assertTrue(ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_METER_FACTORY.isInstance(this.meter));
    Assert.assertFalse(ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_METER_FACTORY.isInstance(this.histogram));
    Assert.assertEquals(this.meter.getContext(), this.context);
    Assert.assertEquals(this.meter.getName(), RECORD_PROCESS_RATE);
    Assert.assertEquals(this.meter.getFullyQualifiedName(false), MetricRegistry.name(JOB_ID, RECORD_PROCESS_RATE));
  }

  @Test
  public void testContextAwareHistogramFactory() {
    Assert.assertTrue(ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_HISTOGRAM_FACTORY.isInstance(this.histogram));
    Assert.assertFalse(ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_HISTOGRAM_FACTORY.isInstance(this.timer));
    Assert.assertEquals(this.histogram.getContext(), this.context);
    Assert.assertEquals(this.histogram.getName(), RECORD_SIZE_DISTRIBUTION);
    Assert.assertEquals(
        this.histogram.getFullyQualifiedName(false), MetricRegistry.name(JOB_ID, RECORD_SIZE_DISTRIBUTION));
  }

  @Test
  public void testContextAwareTimerFactory() {
    Assert.assertTrue(ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_TIMER_FACTORY.isInstance(this.timer));
    Assert.assertFalse(ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_TIMER_FACTORY.isInstance(this.counter));
    Assert.assertEquals(this.timer.getContext(), this.context);
    Assert.assertEquals(this.timer.getName(), TOTAL_DURATION);
    Assert.assertEquals(this.timer.getFullyQualifiedName(false), MetricRegistry.name(JOB_ID, TOTAL_DURATION));
  }

  @AfterClass
  public void tearDown() throws IOException {
    this.context.close();
  }
}
