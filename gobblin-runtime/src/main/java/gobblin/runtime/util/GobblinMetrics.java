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

package gobblin.runtime.util;

import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.MapMaker;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.metrics.MetricContext;


/**
 * A class that represents a set of metrics associated with a given name.
 *
 * @author ynli
 */
public class GobblinMetrics {

  /**
   * Enumeration of metric types.
   */
  public enum MetricType {
    COUNTER, METER, GAUGE
  }

  /**
   * Enumeration of metric groups used internally.
   */
  public enum MetricGroup {
    JOB, TASK
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinMetrics.class);

  // Mapping from job ID to metrics set. This map is needed so an instance of
  // this class for a job run can be accessed from anywhere in the same JVM.
  // This map uses weak references for values (instances of this class) so
  // they can be garbage-collected if they are no longer in regular use.
  protected static final ConcurrentMap<String, GobblinMetrics> METRICS_MAP = new MapMaker().weakValues().makeMap();

  protected final String id;
  protected MetricContext metricContext;

  // Closer for closing the metric output stream
  protected final Closer closer = Closer.create();

  protected GobblinMetrics(String id) {
    this.id = id;
    this.metricContext = null;
  }

  /**
   * Remove the {@link GobblinMetrics} instance for the given job.
   *
   * @param id job ID
   * @return removed {@link GobblinMetrics} instance or <code>null</code> if no {@link GobblinMetrics}
   *         instance for the given job is not found
   */
  public static GobblinMetrics remove(String id) {
    return METRICS_MAP.remove(id);
  }

  /**
   * Check whether metrics collection and reporting are enabled or not.
   *
   * @param properties Configuration properties
   * @return whether metrics collection and reporting are enabled
   */
  public static boolean isEnabled(Properties properties) {
    return Boolean.valueOf(
        properties.getProperty(ConfigurationKeys.METRICS_ENABLED_KEY, ConfigurationKeys.DEFAULT_METRICS_ENABLED));
  }

  /**
   * Check whether metrics collection and reporting are enabled or not.
   *
   * @param state a {@link State} object containing configuration properties
   * @return whether metrics collection and reporting are enabled
   */
  public static boolean isEnabled(State state) {
    return Boolean
        .valueOf(state.getProp(ConfigurationKeys.METRICS_ENABLED_KEY, ConfigurationKeys.DEFAULT_METRICS_ENABLED));
  }

  /**
   * Get the wrapped {@link com.codahale.metrics.MetricRegistry} instance.
   *
   * @return wrapped {@link com.codahale.metrics.MetricRegistry} instance
   */
  public MetricContext getMetricContext() {
    return this.metricContext;
  }

  /**
   * Get the job ID of this metrics set.
   *
   * @return job ID of this metrics set
   */
  public String getId() {
    return this.id;
  }

  public Meter getMeter(String prefix, String... suffixes) {
    return this.metricContext.meter(MetricRegistry.name(prefix, suffixes));
  }

  public Counter getCounter(String prefix, String... suffixes) {
    return this.metricContext.counter(MetricRegistry.name(prefix, suffixes));
  }

  public Histogram getHistogram(String prefix, String... suffixes) {
    return this.metricContext.histogram(MetricRegistry.name(prefix, suffixes));
  }

  public Timer getTimer(String prefix, String... suffixes) {
    return this.metricContext.timer(MetricRegistry.name(prefix, suffixes));
  }

}
