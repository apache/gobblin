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

package gobblin.metrics.reporter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import lombok.Getter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.typesafe.config.Config;

import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;


/**
 * Scheduled reporter with metrics reporting configuration.
 *
 * <p>
 * Concrete reporters need to subclass this class and implement {@link ScheduledReporter#report(SortedMap , SortedMap, SortedMap, SortedMap, SortedMap, Map)}
 * that emits the metrics in the desired (textual/serialized) form.
 * </p>
 *
 * @author Lorand Bendig
 *
 */
public abstract class ConfiguredScheduledReporter extends ScheduledReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfiguredScheduledReporter.class);

  private static final String FINAL_TAG_KEY = "finalReport";

  @Getter
  private final TimeUnit rateUnit;

  @Getter
  private final TimeUnit durationUnit;

  private final double rateFactor;
  private final double durationFactor;

  protected final ImmutableMap<String, String> tags;
  protected final Closer closer;
  protected final String metricContextName;

  protected static final Joiner JOINER = Joiner.on('.').skipNulls();

  public ConfiguredScheduledReporter(Builder<?> builder, Config config) {
    super(builder.name, config);
    this.rateUnit = builder.rateUnit;
    this.durationUnit = builder.durationUnit;
    this.rateFactor = builder.rateUnit.toSeconds(1);
    this.durationFactor = 1.0 / builder.durationUnit.toNanos(1);
    this.tags = ImmutableMap.copyOf(builder.tags);
    this.closer = Closer.create();
    this.metricContextName = builder.metricContextName;
  }

  /**
   * Builder for {@link ConfiguredScheduledReporter}. Defaults to no filter, reporting rates in seconds and times in
   * milliseconds.
   */
  public static abstract class Builder<T extends Builder<T>> {

    protected String name;
    protected TimeUnit rateUnit;
    protected TimeUnit durationUnit;
    protected Map<String, String> tags;
    protected String metricContextName;

    protected Builder() {
      this.name = "ConfiguredScheduledReporter";
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.tags = Maps.newHashMap();
    }

    protected abstract T self();

    /**
     * Set the name of the reporter
     *
     * @param name name of the metric reporter
     * @return {@code this}
     */
    public T name(String name) {
      this.name = name;
      return self();
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public T convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return self();
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public T convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return self();
    }

    /**
     * Add tags
     * @param tags additional {@link gobblin.metrics.Tag}s for the reporter.
     * @return {@code this}
     */
    public T withTags(Map<String, String> tags) {
      this.tags.putAll(tags);
      return self();
    }

    /**
     * Add tags.
     * @param tags List of {@link gobblin.metrics.Tag}
     * @return {@code this}
     */
    public T withTags(List<Tag<?>> tags) {
      for(Tag<?> tag : tags) {
        this.tags.put(tag.getKey(), tag.getValue().toString());
      }
      return self();
    }

    /**
     * Add tag.
     * @param key tag key
     * @param value tag value
     * @return {@code this}
     */
    public T withTag(String key, String value) {
      this.tags.put(key, value);
      return self();
    }

    /**
     * Add the name of the base metrics context as prefix to the metric keys
     *
     * @param metricContextName name of the metrics context
     * @return {@code this}
     */
    public T withMetricContextName(String metricContextName) {
      this.metricContextName = metricContextName;
      return self();
    }

  }

  @Override
  protected void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers,
      Map<String, Object> tags, boolean isFinal) {
    if (isFinal) {
      report(gauges, counters, histograms, meters, timers,
          ImmutableMap.<String, Object>builder().putAll(tags).put(FINAL_TAG_KEY, Boolean.TRUE).build());
    } else {
      report(gauges, counters, histograms, meters, timers, tags);
    }
  }

  protected double convertDuration(double duration) {
    return duration * this.durationFactor;
  }

  protected double convertRate(double rate) {
    return rate * this.rateFactor;
  }

  /**
   * Constructs the prefix of metric key to be emitted.
   * Enriches {@link ConfiguredScheduledReporter#metricContextName} with the current task id and fork id to
   * be able to identify the emitted metric by its origin
   *
   * @param tags
   * @return Prefix of the metric key
   */
  protected String getMetricNamePrefix(Map<String, Object> tags){
    String currentContextName = (String) tags.get(MetricContext.METRIC_CONTEXT_NAME_TAG_NAME);
    if (metricContextName == null || (currentContextName.indexOf(metricContextName) > -1)) {
      return currentContextName;
    }
    return JOINER.join(metricContextName, tags.get("taskId"), tags.get("forkBranchName"), tags.get("class"));
  }

  @Override
  public void close() throws IOException {
    try {
      this.closer.close();
    } catch(Exception e) {
      LOGGER.warn("Exception when closing ConfiguredScheduledReporter", e);
    } finally {
      super.close();
    }
  }
}
