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

package gobblin.metrics.influxdb;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Serie;
import org.influxdb.impl.InfluxDBImpl;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;

import gobblin.metrics.Measurements;
import gobblin.metrics.MetricContext;
import static gobblin.metrics.TestConstants.*;


/**
 * Unit tests for {@link InfluxDBReporter}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.metrics.influxdb", "ignore"})
public class InfluxDBReporterTest {

  private static final String URL = "http://localhost:8086";
  private static final String USER = "root";
  private static final String PASSWORD = "root";
  private static final String DATABASE = "test";

  private TestInfluxDB influxDB;
  private InfluxDBReporter influxDBReporter;

  @BeforeClass
  public void setUp() {
    this.influxDB = new TestInfluxDB(URL, USER, PASSWORD);
    this.influxDBReporter = new InfluxDBReporter(MetricContext.builder(CONTEXT_NAME).build(),
        InfluxDBReporter.class.getSimpleName(), MetricFilter.ALL, TimeUnit.SECONDS,
        TimeUnit.MILLISECONDS, this.influxDB, DATABASE, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testReportMetrics() {
    Gauge<Integer> queueSizeGauge = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return 1000;
      }
    };

    Counter recordsProcessedCounter = new Counter();
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

    SortedMap<String, Counter> counters = ImmutableSortedMap.<String, Counter>naturalOrder()
        .put(RECORDS_PROCESSED, recordsProcessedCounter).build();
    SortedMap<String, Gauge> gauges = ImmutableSortedMap.<String, Gauge>naturalOrder()
        .put(QUEUE_SIZE, queueSizeGauge).build();
    SortedMap<String, Histogram> histograms = ImmutableSortedMap.<String, Histogram>naturalOrder()
        .put(RECORD_SIZE_DISTRIBUTION, recordSizeDistributionHistogram).build();
    SortedMap<String, Meter> meters = ImmutableSortedMap.<String, Meter>naturalOrder()
        .put(RECORD_PROCESS_RATE, recordProcessRateMeter).build();
    SortedMap<String, Timer> timers = ImmutableSortedMap.<String, Timer>naturalOrder()
        .put(TOTAL_DURATION, totalDurationTimer).build();

    this.influxDBReporter.report(gauges, counters, histograms, meters, timers);

    Assert.assertEquals(
        ((Integer) this.influxDB.getSerie(MetricRegistry.name(CONTEXT_NAME, QUEUE_SIZE)).getRows().get(0)
            .get(InfluxDBReporter.VALUE)).intValue(),
        1000);
    Assert.assertEquals(
        ((Long) this.influxDB.getSerie(MetricRegistry.name(CONTEXT_NAME, RECORDS_PROCESSED,
            Measurements.COUNT.getName())).getRows().get(0).get(InfluxDBReporter.VALUE)).longValue(),
        10l);
    Assert.assertEquals(
        ((Long) this.influxDB.getSerie(MetricRegistry.name(CONTEXT_NAME, RECORD_PROCESS_RATE,
            Measurements.COUNT.getName())).getRows().get(0).get(InfluxDBReporter.VALUE)).longValue(),
        6l);
    Assert.assertEquals(
        ((Long) this.influxDB.getSerie(MetricRegistry.name(CONTEXT_NAME, RECORD_SIZE_DISTRIBUTION,
            Measurements.COUNT.getName())).getRows().get(0).get(InfluxDBReporter.VALUE)).longValue(),
        3l);
    Assert.assertEquals(
        ((Long) this.influxDB.getSerie(MetricRegistry.name(CONTEXT_NAME, TOTAL_DURATION,
            Measurements.COUNT.getName())).getRows().get(0).get(InfluxDBReporter.VALUE)).longValue(),
        3l);

    recordsProcessedCounter.inc(5l);
    recordSizeDistributionHistogram.update(4);
    recordProcessRateMeter.mark(4l);
    totalDurationTimer.update(4, TimeUnit.SECONDS);

    this.influxDBReporter.report(gauges, counters, histograms, meters, timers);

    Assert.assertEquals(
        ((Integer) this.influxDB.getSerie(MetricRegistry.name(CONTEXT_NAME, QUEUE_SIZE)).getRows().get(0)
            .get(InfluxDBReporter.VALUE)).intValue(),
        1000);
    Assert.assertEquals(
        ((Long) this.influxDB.getSerie(MetricRegistry.name(CONTEXT_NAME, RECORDS_PROCESSED,
            Measurements.COUNT.getName())).getRows().get(0).get(InfluxDBReporter.VALUE)).longValue(),
        15l);
    Assert.assertEquals(
        ((Long) this.influxDB.getSerie(MetricRegistry.name(CONTEXT_NAME, RECORD_PROCESS_RATE,
            Measurements.COUNT.getName())).getRows().get(0).get(InfluxDBReporter.VALUE)).longValue(),
        10l);
    Assert.assertEquals(
        ((Long) this.influxDB.getSerie(MetricRegistry.name(CONTEXT_NAME, RECORD_SIZE_DISTRIBUTION,
            Measurements.COUNT.getName())).getRows().get(0).get(InfluxDBReporter.VALUE)).longValue(),
        4l);
    Assert.assertEquals(
        ((Long) this.influxDB.getSerie(MetricRegistry.name(CONTEXT_NAME, TOTAL_DURATION,
            Measurements.COUNT.getName())).getRows().get(0).get(InfluxDBReporter.VALUE)).longValue(),
        4l);
  }

  @AfterClass
  public void tearDown() {
    if (this.influxDBReporter != null) {
      this.influxDBReporter.close();
    }
  }

  /**
   * An implementation of {@link InfluxDB} for testing.
   */
  private static class TestInfluxDB extends InfluxDBImpl {

    private final Map<String, Serie> series = Maps.newHashMap();

    public TestInfluxDB(String url, String username, String password) {
      super(url, username, password);
    }

    @Override
    public void write(String database, TimeUnit precision, Serie... series) {
      for (Serie serie : series) {
        this.series.put(serie.getName(), serie);
      }
    }

    public Serie getSerie(String name) {
      return this.series.get(name);
    }
  }
}
