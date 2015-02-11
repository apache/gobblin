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
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;


/**
 * A {@link com.codahale.metrics.ScheduledReporter} that reports metrics to InfluxDB.
 *
 * @author ynli
 */
public class InfluxDBReporter extends ScheduledReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBReporter.class);

  private final JobMetrics jobMetrics;
  private final JobMetricsStore metricsStore;

  public InfluxDBReporter(JobMetrics jobMetrics, Properties properties, TimeUnit rateUnit, TimeUnit durationUnit,
      MetricFilter filter) {

    super(jobMetrics.getMetricRegistry(), "influxdb-reporter", filter, rateUnit, durationUnit);
    this.jobMetrics = jobMetrics;
    this.metricsStore = new InfluxDBJobMetricsStore(properties);
  }

  /**
   * Returns a new {@link Builder} for {@link InfluxDBReporter}.
   *
   * @param jobMetrics the {@link JobMetrics} to report
   * @return new {@link Builder} instance for {@link InfluxDBReporter}
   */
  public static Builder forMetricSet(JobMetrics jobMetrics) {
    return new Builder(jobMetrics);
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {

    try {
      this.metricsStore.put(this.jobMetrics);
    } catch (IOException ioe) {
      LOGGER.error("Failed to put metrics for job " + this.jobMetrics.getJobId(), ioe);
    }
  }

  /**
   * A builder class for {@link InfluxDBReporter}.
   *
   * @author ynli
   */
  public static class Builder {

    private final JobMetrics jobMetrics;
    private Properties properties;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;

    private Builder(JobMetrics jobMetrics) {
      this.jobMetrics = jobMetrics;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
    }

    /**
     * Use the given {@link java.util.Properties} instance for Gobblin configuration.
     *
     * @param properties given {@link java.util.Properties} instance
     * @return {@code this}
     */
    public Builder withProperties(Properties properties) {
      this.properties = properties;
      return this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit given rate time unit
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit given duration time unit
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Use the given {@link com.codahale.metrics.MetricFilter}.
     *
     * @param filter given {@link com.codahale.metrics.MetricFilter}
     * @return {@code this}
     */
    public Builder withFilter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Build a new {@link InfluxDBReporter} instance.
     *
     * @return newly built {@link InfluxDBReporter} instance
     */
    public InfluxDBReporter build() {
      return new InfluxDBReporter(this.jobMetrics, this.properties, this.rateUnit, this.durationUnit, this.filter);
    }
  }
}
