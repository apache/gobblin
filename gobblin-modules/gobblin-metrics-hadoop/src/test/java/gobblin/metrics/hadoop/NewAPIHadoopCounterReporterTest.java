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

package gobblin.metrics.hadoop;

import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import com.google.common.collect.ImmutableSortedMap;

import gobblin.metrics.Measurements;
import gobblin.metrics.MetricContext;
import static gobblin.metrics.test.TestConstants.*;


/**
 * Unit tests for {@link NewAPIHadoopCounterReporter}.
 *
 * @author Yinan Li
 */
@Test(groups = {"gobblin.metrics.hadoop"})
public class NewAPIHadoopCounterReporterTest {

  private NewAPIHadoopCounterReporter<Object, Object, Object, Object> hadoopCounterReporter;
  private Counter recordsProcessedCount;
  private Counter recordProcessRateCount;
  private Counter recordSizeDistributionCount;
  private Counter totalDurationCount;
  private Counter queueSize;

  private String name = CONTEXT_NAME + "_" + UUID.randomUUID().toString();

  @BeforeClass
  @SuppressWarnings("unchecked")
  public void setUp() {
    TaskInputOutputContext<Object, Object, Object, Object> mockContext = Mockito.mock(TaskInputOutputContext.class);

    this.recordsProcessedCount = Mockito.mock(Counter.class);
    Mockito.when(mockContext.getCounter(
        this.name, MetricRegistry.name(RECORDS_PROCESSED, Measurements.COUNT.getName())))
        .thenReturn(this.recordsProcessedCount);

    this.recordProcessRateCount = Mockito.mock(Counter.class);
    Mockito.when(mockContext.getCounter(
        this.name, MetricRegistry.name(RECORD_PROCESS_RATE, Measurements.COUNT.getName())))
        .thenReturn(this.recordProcessRateCount);

    this.recordSizeDistributionCount = Mockito.mock(Counter.class);
    Mockito.when(mockContext.getCounter(
        this.name, MetricRegistry.name(RECORD_SIZE_DISTRIBUTION, Measurements.COUNT.getName())))
        .thenReturn(this.recordSizeDistributionCount);

    this.totalDurationCount = Mockito.mock(Counter.class);
    Mockito.when(mockContext.getCounter(
        this.name, MetricRegistry.name(TOTAL_DURATION, Measurements.COUNT.getName())))
        .thenReturn(this.totalDurationCount);

    this.queueSize = Mockito.mock(Counter.class);
    Mockito.when(mockContext.getCounter(this.name, QUEUE_SIZE)).thenReturn(this.queueSize);

    this.hadoopCounterReporter = NewAPIHadoopCounterReporter.builder(mockContext)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.SECONDS)
        .filter(MetricFilter.ALL)
        .build(MetricContext.builder(this.name).build());
  }

  @Test
  public void testReportMetrics() {
    Gauge<Integer> queueSizeGauge = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return 1000;
      }
    };

    com.codahale.metrics.Counter recordsProcessedCounter = new com.codahale.metrics.Counter();
    recordsProcessedCounter.inc(10l);

    Histogram recordSizeDistributionHistogram = new Histogram(new ExponentiallyDecayingReservoir());
    recordSizeDistributionHistogram.update(1);
    recordSizeDistributionHistogram.update(2);
    recordSizeDistributionHistogram.update(3);

    Meter recordProcessRateMeter = new Meter();
    recordProcessRateMeter.mark(1l);
    recordProcessRateMeter.mark(2l);
    recordProcessRateMeter.mark(3l);

    Timer totalDurationTimer = new Timer();
    totalDurationTimer.update(1, TimeUnit.SECONDS);
    totalDurationTimer.update(2, TimeUnit.SECONDS);
    totalDurationTimer.update(3, TimeUnit.SECONDS);

    SortedMap<String, com.codahale.metrics.Counter> counters =
        ImmutableSortedMap.<String, com.codahale.metrics.Counter>naturalOrder()
        .put(RECORDS_PROCESSED, recordsProcessedCounter).build();
    SortedMap<String, Gauge> gauges = ImmutableSortedMap.<String, Gauge>naturalOrder()
        .put(QUEUE_SIZE, queueSizeGauge).build();
    SortedMap<String, Histogram> histograms = ImmutableSortedMap.<String, Histogram>naturalOrder()
        .put(RECORD_SIZE_DISTRIBUTION, recordSizeDistributionHistogram).build();
    SortedMap<String, Meter> meters = ImmutableSortedMap.<String, Meter>naturalOrder()
        .put(RECORD_PROCESS_RATE, recordProcessRateMeter).build();
    SortedMap<String, Timer> timers = ImmutableSortedMap.<String, Timer>naturalOrder()
        .put(TOTAL_DURATION, totalDurationTimer).build();

    this.hadoopCounterReporter.report(gauges, counters, histograms, meters, timers);

    Mockito.verify(this.recordsProcessedCount).increment(10l);
    Mockito.verify(this.recordProcessRateCount).increment(6l);
    Mockito.verify(this.recordSizeDistributionCount).increment(3l);
    Mockito.verify(this.totalDurationCount).increment(3l);
    Mockito.verify(this.queueSize).setValue(1000);

    recordsProcessedCounter.inc(5l);
    recordSizeDistributionHistogram.update(4);
    recordProcessRateMeter.mark(4l);
    totalDurationTimer.update(4, TimeUnit.SECONDS);

    this.hadoopCounterReporter.report(gauges, counters, histograms, meters, timers);

    Mockito.verify(this.recordsProcessedCount).increment(5l);
    Mockito.verify(this.recordProcessRateCount).increment(4l);
    Mockito.verify(this.recordSizeDistributionCount).increment(1l);
    Mockito.verify(this.totalDurationCount).increment(1l);
  }

  @AfterClass
  public void tearDown() {
    if (this.hadoopCounterReporter != null) {
      this.hadoopCounterReporter.close();
    }
  }
}
