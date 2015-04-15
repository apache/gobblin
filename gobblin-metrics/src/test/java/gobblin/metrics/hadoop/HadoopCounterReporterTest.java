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

package gobblin.metrics.hadoop;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;

import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;

import com.google.common.collect.ImmutableSortedMap;

import gobblin.metrics.MetricContext;


/**
 * Unit tests for {@link HadoopCounterReporter}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.metrics.hadoop"})
public class HadoopCounterReporterTest {

  private static final String CONTEXT_NAME = "TestContext";
  private static final String RECORDS_PROCESSED = "recordsProcessed";
  private HadoopCounterReporter hadoopCounterReporter;
  private Counters.Counter recordsProcessed;

  @BeforeClass
  public void setUp() {
    Reporter mockedReporter = Mockito.mock(Reporter.class);
    this.recordsProcessed = Mockito.mock(Counters.Counter.class);
    Mockito.when(mockedReporter.getCounter(CONTEXT_NAME, RECORDS_PROCESSED)).thenReturn(this.recordsProcessed);

    this.hadoopCounterReporter = HadoopCounterReporter.builder(mockedReporter)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.SECONDS)
        .filter(MetricFilter.ALL)
        .build(MetricContext.builder(CONTEXT_NAME).build());
  }

  @Test
  public void testReportMetrics() {
    Counter recordsProcessedCounter = new Counter();
    recordsProcessedCounter.inc(10l);

    this.hadoopCounterReporter.report(
        ImmutableSortedMap.<String, Gauge>naturalOrder().build(),
        ImmutableSortedMap.<String, Counter>naturalOrder().put(RECORDS_PROCESSED, recordsProcessedCounter).build(),
        ImmutableSortedMap.<String, Histogram>naturalOrder().build(),
        ImmutableSortedMap.<String, Meter>naturalOrder().build(),
        ImmutableSortedMap.<String, Timer>naturalOrder().build()
    );

    // Verify the reporter has been attempting to set the counter value
    Mockito.verify(this.recordsProcessed).setValue(10l);
  }

  @AfterClass
  public void tearDown() {
    this.hadoopCounterReporter.close();
  }
}
