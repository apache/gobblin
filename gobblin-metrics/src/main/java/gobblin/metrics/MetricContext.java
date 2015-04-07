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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;


/**
 * A class representing.
 *
 * @author ynli
 */
public class MetricContext implements MetricSet {

  // Name of this context
  private final String name;

  // The central registry where all metrics in this context are registered
  private final MetricRegistry metricRegistry = new MetricRegistry();

  // Reference to the parent context wrapped in an Optional as there may be no parent context
  private final Optional<MetricContext> parent;

  // A LinkedHashSet is used here to maintain the insertion order
  private final Set<MetricTag> tags = Sets.newLinkedHashSet();

  // List of scheduled metric reporters associated with this context
  private final Set<ScheduledReporter> scheduledReporters = Sets.newHashSet();

  public MetricContext(String name) {
    this.name = name;
    this.parent = Optional.absent();
  }

  public MetricContext(String name, MetricContext parent) {
    this.name = name;
    this.parent = Optional.of(parent);
    Set<MetricTag> tagsCopy = Sets.newLinkedHashSet(this.tags);
    this.tags.clear();
    // Add tags associated with the parent context first
    this.tags.addAll(parent.tags);
    // Then add existing tags associated with this context
    this.tags.addAll(tagsCopy);
    this.scheduledReporters.addAll(parent.scheduledReporters);
  }

  @Override
  public Map<String, Metric> getMetrics() {
    return this.metricRegistry.getMetrics();
  }

  /**
   * Get the name of this {@link MetricContext}.
   *
   * @return the name of this {@link MetricContext}
   */
  public String getName() {
    return this.name;
  }

  /**
   * Get the wrapped {@link com.codahale.metrics.MetricRegistry} instance.
   *
   * @return wrapped {@link com.codahale.metrics.MetricRegistry} instance
   */
  public MetricRegistry getMetricRegistry() {
    return this.metricRegistry;
  }

  /**
   * Get a {@link com.codahale.metrics.Counter} with the given name.
   *
   * @param name counter name
   * @return the {@link com.codahale.metrics.Counter} with the given name
   */
  public Counter getCounter(String name) {
    return this.metricRegistry.counter(MetricRegistry.name(metricNamePrefix(), name));
  }

  /**
   * Get a {@link com.codahale.metrics.Meter} with the given name.
   *
   * @param name meter name
   * @return the {@link com.codahale.metrics.Meter} with the given name
   */
  public Meter getMeter(String name) {
    return this.metricRegistry.meter(MetricRegistry.name(metricNamePrefix(), name));
  }

  /**
   * Register a {@link com.codahale.metrics.Gauge} with the given name.
   *
   * @param name gauge name
   * @param gauge the {@link com.codahale.metrics.Gauge} to register
   * @param <T> gauge data type
   * @return the {@link com.codahale.metrics.Gauge} with the given name
   */
  public <T> Gauge<T> registerGauge(String name, Gauge<T> gauge) {
    return this.metricRegistry.register(MetricRegistry.name(metricNamePrefix(), name), gauge);
  }

  /**
   * Get a {@link com.codahale.metrics.Histogram} with the given name.
   *
   * @param name histogram name
   * @return the {@link com.codahale.metrics.Histogram} with the given name
   */
  public Histogram getHistogram(String name) {
    return this.metricRegistry.histogram(MetricRegistry.name(metricNamePrefix(), name));
  }

  /**
   * Get a {@link com.codahale.metrics.Timer} with the given name.
   *
   * @param name timer name
   * @return newly created {@link com.codahale.metrics.Timer}
   */
  public Timer getTimer(String name) {
    return this.metricRegistry.timer(MetricRegistry.name(metricNamePrefix(), name));
  }

  /**
   * Remove the metric object associated with the given name.
   *
   * @param name metric name
   */
  public boolean removeMetric(String name) {
    return this.metricRegistry.remove(MetricRegistry.name(metricNamePrefix(), name));
  }

  /**
   * Add a new {@link MetricTag}.
   *
   * <p>
   *   The order in which {@link MetricTag}s are added is important as this is the order
   *   the tag names appear in the metric name prefix (see {@link #metricNamePrefix}.
   * </p>
   *
   * @param tag the {@link MetricTag} to add
   */
  public void addTag(MetricTag tag) {
    Preconditions.checkNotNull(tag);
    this.tags.add(tag);
  }

  /**
   * Get the set of {@link MetricTag}s associated with this {@link MetricContext} stored in a list.
   *
   * @return the set of {@link MetricTag}s associated with this {@link MetricContext} stored in a list
   */
  public List<MetricTag> getTags() {
    return ImmutableList.<MetricTag>builder().addAll(this.tags).build();
  }

  /**
   * Get the set of {@link MetricTag}s of the given {@link MetricTag.TagType} associated with this
   * {@link MetricContext} stored in a list.
   *
   * @param type the given {@link MetricTag.TagType}
   * @return the set of {@link MetricTag}s of the given {@link MetricTag.TagType} associated with this
   *         {@link MetricContext} stored in a list
   */
  public List<MetricTag> getTags(MetricTag.TagType type) {
    ImmutableList.Builder<MetricTag> immutableListBuilder = ImmutableList.builder();
    for (MetricTag tag : this.tags) {
      if (tag.getType().equals(type)) {
        immutableListBuilder.add(tag);
      }
    }

    return immutableListBuilder.build();
  }

  /**
   * Add a new {@link com.codahale.metrics.ScheduledReporter}.
   *
   * @param reporter the new {@link com.codahale.metrics.ScheduledReporter} to add
   */
  public void addScheduledReporter(ScheduledReporter reporter) {
    Preconditions.checkNotNull(reporter);
    this.scheduledReporters.add(reporter);
  }

  /**
   * Get the parent {@link MetricContext} of this {@link MetricContext} wrapped in an
   * {@link com.google.common.base.Optional}, which may be absent if it has not parent
   * {@link MetricContext}.
   *
   * @return the parent {@link MetricContext} of this {@link MetricContext} wrapped in an
   *         {@link com.google.common.base.Optional}
   */
  public Optional<MetricContext> getParent() {
    return this.parent;
  }

  /**
   * Create a new child {@link MetricContext}.
   *
   * @param childContextName name of the new child {@link MetricContext}
   * @return a new child {@link MetricContext}
   */
  public MetricContext newChildContext(String childContextName) {
    return new MetricContext(childContextName, this);
  }

  /**
   * Get the metric name prefix constructed by joining the name of this {@link MetricContext}
   * and the names of the {@link MetricTag}s of type {@link MetricTag.TagType#STATIC} on dot.
   *
   * @return the metric name prefix
   */
  private String metricNamePrefix() {
    return Joiner.on(".").skipNulls().join(this.name, getTags(MetricTag.TagType.STATIC));
  }
}
