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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;


/**
 * This class models a {@link MetricSet} that optionally has a list of {@link Tag}s
 * and a set of {@link com.codahale.metrics.ScheduledReporter}s associated with it. The
 * {@link Tag}s associated with a {@link MetricContext} are used to construct the
 * common metric name prefix of registered {@link com.codahale.metrics.Metric}s.
 *
 * <p>
 *   {@link MetricContext}s can form a hierarchy and any {@link MetricContext} can create
 *   children {@link MetricContext}s. A child {@link MetricContext} inherit all the
 *   {@link Tag}s associated with its parent, in additional to the {@link Tag}s
 *   of itself. {@link Tag}s inherited from its parent will appear in front of those
 *   of itself when constructing the metric name prefix.
 * </p>
 *
 * @author ynli
 */
public class MetricContext extends MetricRegistry implements Taggable, Closeable {

  // Name of this context
  private final String name;

  // A map from simple names to context-aware metrics. All metrics registered with this context (using
  // their simple names) are also registered with the MetricRegistry using their fully-qualified names.
  private final ConcurrentMap<String, Metric> contextAwareMetrics = Maps.newConcurrentMap();

  // List of context-aware scheduled metric reporters associated with this context
  private final Map<String, ContextAwareScheduledReporter> contextAwareScheduledReporters = Maps.newHashMap();

  // Reference to the parent context wrapped in an Optional as there may be no parent context
  private final Optional<MetricContext> parent;

  // A map from child context names to child contexts
  private final ConcurrentMap<String, MetricContext> children = new MapMaker().weakValues().makeMap();

  // This is used to work on tags associated with this context
  private final Tagged tagged;

  // This flag tells if the context should report fully-qualified metric names (including the tags)
  private final boolean reportFullyQualifiedNames;

  // This flag tells if the fully-qualified metric names should include tag keys
  private final boolean includeTagKeys;

  // This is used to close all children context when this context is to be closed
  private final Closer closer = Closer.create();

  private MetricContext(String name, MetricContext parent, List<Tag<?>> tags,
      Map<String, ContextAwareScheduledReporter.Builder> builders, boolean reportFullyQualifiedNames,
      boolean includeTagKeys) {
    this.name = name;
    this.parent = Optional.fromNullable(parent);
    this.tagged = new Tagged(tags);
    this.reportFullyQualifiedNames = reportFullyQualifiedNames;
    this.includeTagKeys = includeTagKeys;

    // Add as a child to the parent context if a parent exists
    if (this.parent.isPresent()) {
      this.parent.get().addChildContext(name, this);
    }

    for (Map.Entry<String, ContextAwareScheduledReporter.Builder> entry : builders.entrySet()) {
      this.contextAwareScheduledReporters.put(entry.getKey(), entry.getValue().build(this));
    }
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
   * Add a child {@link MetricContext}.
   *
   * @param childContext the child {@link MetricContext} to add
   */
  public void addChildContext(String childContextName, MetricContext childContext) {
    if (this.children.putIfAbsent(childContextName, childContext) != null) {
      throw new IllegalArgumentException("A child context named " + childContextName + " already exists");
    }
  }

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getNames()}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  @Override
  public SortedSet<String> getNames() {
    return this.reportFullyQualifiedNames ? super.getNames() : getSimpleNames();
  }

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getMetrics()}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  @Override
  public Map<String, Metric> getMetrics() {
    return this.reportFullyQualifiedNames ? super.getMetrics() : getSimplyNamedMetrics();
  }

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getGauges()}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  @Override
  public SortedMap<String, Gauge> getGauges() {
    return this.reportFullyQualifiedNames ?
        super.getGauges() : getSimplyNamedMetrics(Gauge.class, Optional.<MetricFilter>absent());
  }

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getGauges(com.codahale.metrics.MetricFilter)}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  @Override
  public SortedMap<String, Gauge> getGauges(MetricFilter filter) {
    return this.reportFullyQualifiedNames ?
        super.getGauges(filter) : getSimplyNamedMetrics(Gauge.class, Optional.of(filter));
  }

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getCounters()}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  @Override
  public SortedMap<String, Counter> getCounters() {
    return this.reportFullyQualifiedNames ?
        super.getCounters() : getSimplyNamedMetrics(Counter.class, Optional.<MetricFilter>absent());
  }

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getCounters(com.codahale.metrics.MetricFilter)}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  @Override
  public SortedMap<String, Counter> getCounters(MetricFilter filter) {
    return this.reportFullyQualifiedNames ?
        super.getCounters(filter) : getSimplyNamedMetrics(Counter.class, Optional.of(filter));
  }

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getHistograms()}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  @Override
  public SortedMap<String, Histogram> getHistograms() {
    return this.reportFullyQualifiedNames ?
        super.getHistograms() : getSimplyNamedMetrics(Histogram.class, Optional.<MetricFilter>absent());
  }

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getHistograms(com.codahale.metrics.MetricFilter)}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  @Override
  public SortedMap<String, Histogram> getHistograms(MetricFilter filter) {
    return this.reportFullyQualifiedNames ?
        super.getHistograms(filter) : getSimplyNamedMetrics(Histogram.class, Optional.of(filter));
  }

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getMeters()}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  @Override
  public SortedMap<String, Meter> getMeters() {
    return this.reportFullyQualifiedNames ?
        super.getMeters() : getSimplyNamedMetrics(Meter.class, Optional.<MetricFilter>absent());
  }

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getMeters(com.codahale.metrics.MetricFilter)}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  @Override
  public SortedMap<String, Meter> getMeters(MetricFilter filter) {
    return this.reportFullyQualifiedNames ?
        super.getMeters(filter) : getSimplyNamedMetrics(Meter.class, Optional.of(filter));
  }

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getTimers()}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  @Override
  public SortedMap<String, Timer> getTimers() {
    return this.reportFullyQualifiedNames ?
        super.getTimers() : getSimplyNamedMetrics(Timer.class, Optional.<MetricFilter>absent());
  }

  /**
   * See {@link com.codahale.metrics.MetricRegistry#getTimers(com.codahale.metrics.MetricFilter)}.
   *
   * <p>
   *   This method will return fully-qualified metric names if the {@link MetricContext} is configured
   *   to report fully-qualified metric names.
   * </p>
   */
  @Override
  public SortedMap<String, Timer> getTimers(MetricFilter filter) {
    return this.reportFullyQualifiedNames ?
        super.getTimers(filter) : getSimplyNamedMetrics(Timer.class, Optional.of(filter));
  }

  /**
   * This is equivalent to {@link #contextAwareCounter(String)}.
   */
  @Override
  public Counter counter(String name) {
    return contextAwareCounter(name);
  }

  /**
   * This is equivalent to {@link #contextAwareMeter(String)}.
   */
  @Override
  public Meter meter(String name) {
    return contextAwareMeter(name);
  }

  /**
   * This is equivalent to {@link #contextAwareHistogram(String)}.
   */
  @Override
  public Histogram histogram(String name) {
    return contextAwareHistogram(name);
  }

  /**
   * This is equivalent to {@link #contextAwareTimer(String)}.
   */
  @Override
  public Timer timer(String name) {
    return contextAwareTimer(name);
  }

  /**
   * Register a given metric under a given name.
   *
   * <p>
   *   This method does not support registering {@link com.codahale.metrics.MetricSet}s.
   *   See{@link #registerAll(com.codahale.metrics.MetricSet}.
   * </p>
   *
   * <p>
   *   This method will not register a metric with the same name in the parent context (if it exists).
   * </p>
   */
  @Override
  public <T extends Metric> T register(String name, T metric)
      throws IllegalArgumentException {
    if (metric instanceof MetricSet) {
      registerAll((MetricSet) metric);
    }
    if (this.contextAwareMetrics.putIfAbsent(name, metric) != null) {
      throw new IllegalArgumentException("A metric named " + name + " already exists");
    }
    // Also register the metric with the MetricRegistry using its fully-qualified name
    return super.register(MetricRegistry.name(metricNamePrefix(this.includeTagKeys), name), metric);
  }

  @Override
  public void registerAll(MetricSet metrics)
      throws IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  /**
   * Get a {@link ContextAwareCounter} with a given name.
   *
   * @param name name of the {@link ContextAwareCounter}
   * @return the {@link ContextAwareCounter} with the given name
   */
  public ContextAwareCounter contextAwareCounter(String name) {
    return contextAwareCounter(name, ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_COUNTER_FACTORY);
  }

  /**
   * Get a {@link ContextAwareCounter} with a given name.
   *
   * @param name name of the {@link ContextAwareCounter}
   * @param factory a {@link ContextAwareMetricFactory} for building {@link ContextAwareCounter}s
   * @return the {@link ContextAwareCounter} with the given name
   */
  public ContextAwareCounter contextAwareCounter(String name, ContextAwareMetricFactory<ContextAwareCounter> factory) {
    return getOrCreate(name, factory);
  }

  /**
   * Get a {@link ContextAwareMeter} with a given name.
   *
   * @param name name of the {@link ContextAwareMeter}
   * @return the {@link ContextAwareMeter} with the given name
   */
  public ContextAwareMeter contextAwareMeter(String name) {
    return contextAwareMeter(name, ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_METER_FACTORY);
  }

  /**
   * Get a {@link ContextAwareMeter} with a given name.
   *
   * @param name name of the {@link ContextAwareMeter}
   * @param factory a {@link ContextAwareMetricFactory} for building {@link ContextAwareMeter}s
   * @return the {@link ContextAwareMeter} with the given name
   */
  public ContextAwareMeter contextAwareMeter(String name, ContextAwareMetricFactory<ContextAwareMeter> factory) {
    return getOrCreate(name, factory);
  }

  /**
   * Get a {@link ContextAwareHistogram} with a given name.
   *
   * @param name name of the {@link ContextAwareHistogram}
   * @return the {@link ContextAwareHistogram} with the given name
   */
  public ContextAwareHistogram contextAwareHistogram(String name) {
    return contextAwareHistogram(name, ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_HISTOGRAM_FACTORY);
  }

  /**
   * Get a {@link ContextAwareHistogram} with a given name.
   *
   * @param name name of the {@link ContextAwareHistogram}
   * @param factory a {@link ContextAwareMetricFactory} for building {@link ContextAwareHistogram}s
   * @return the {@link ContextAwareHistogram} with the given name
   */
  public ContextAwareHistogram contextAwareHistogram(String name,
      ContextAwareMetricFactory<ContextAwareHistogram> factory) {
    return getOrCreate(name, factory);
  }

  /**
   * Get a {@link ContextAwareTimer} with a given name.
   *
   * @param name name of the {@link ContextAwareTimer}
   * @return the {@link ContextAwareTimer} with the given name
   */
  public ContextAwareTimer contextAwareTimer(String name) {
    return contextAwareTimer(name, ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_TIMER_FACTORY);
  }

  /**
   * Get a {@link ContextAwareTimer} with a given name.
   *
   * @param name name of the {@link ContextAwareTimer}
   * @param factory a {@link ContextAwareMetricFactory} for building {@link ContextAwareTimer}s
   * @return the {@link ContextAwareTimer} with the given name
   */
  public ContextAwareTimer contextAwareTimer(String name, ContextAwareMetricFactory<ContextAwareTimer> factory) {
    return getOrCreate(name, factory);
  }

  /**
   * Create a new {@link ContextAwareGauge} wrapping a given {@link com.codahale.metrics.Gauge}.
   *
   * @param name name of the {@link ContextAwareGauge}
   * @param gauge the {@link com.codahale.metrics.Gauge} to be wrapped by the {@link ContextAwareGauge}
   * @param <T> the type of the {@link ContextAwareGauge}'s value
   * @return a new {@link ContextAwareGauge}
   */
  public <T> ContextAwareGauge<T> newContextAwareGauge(String name, Gauge<T> gauge) {
    return new ContextAwareGauge<T>(this, name, gauge);
  }

  /**
   * Remove a metric with a given name.
   *
   * <p>
   *   This method will remove the metric with the given name from this {@link MetricContext}
   *   as well as metrics with the same name from every child {@link MetricContext}s.
   * </p>
   *
   * @param name name of the metric to be removed
   * @return whether or not the metric has been removed
   */
  @Override
  public boolean remove(String name) {
    return this.contextAwareMetrics.remove(name) != null &&
           super.remove(MetricRegistry.name(metricNamePrefix(this.includeTagKeys), name)) &&
           removeChildrenMetrics(name);
  }

  @Override
  public void removeMatching(MetricFilter filter) {
    for (Map.Entry<String, Metric> entry : this.contextAwareMetrics.entrySet()) {
      if (filter.matches(entry.getKey(), entry.getValue())) {
        remove(entry.getKey());
      }
    }
  }

  /**
   * Start all the {@link com.codahale.metrics.ScheduledReporter}s associated with this {@link MetricContext}.
   *
   * @param period the amount of time between polls
   * @param timeUnit the unit for {@code period}
   */
  public void startMetricReporting(long period, TimeUnit timeUnit) {
    for (ScheduledReporter reporter : this.contextAwareScheduledReporters.values()) {
      reporter.start(period, timeUnit);
    }
  }

  /**
   * Stop all the {@link com.codahale.metrics.ScheduledReporter}s associated with this {@link MetricContext}.
   */
  public void stopMetricReporting() {
    for (ScheduledReporter reporter : this.contextAwareScheduledReporters.values()) {
      reporter.stop();
    }
  }

  /**
   * Call the {@link com.codahale.metrics.ScheduledReporter#report} method of the
   * {@link com.codahale.metrics.ScheduledReporter}s associated with this {@link MetricContext}.
   */
  public void reportMetrics() {
    for (ScheduledReporter reporter : this.contextAwareScheduledReporters.values()) {
      reporter.report();
    }
  }

  @Override
  public void addTag(Tag tag) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addTags(Collection<Tag<?>> tags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Tag<?>> getTags() {
    return this.tagged.getTags();
  }

  @Override
  public String metricNamePrefix(boolean includeTagKeys) {
    return this.tagged.metricNamePrefix(includeTagKeys);
  }

  @Override
  public void close() throws IOException {
    stopMetricReporting();
    reportMetrics();
    this.closer.close();
  }

  /**
   * Get a new {@link MetricContext.Builder} for building child {@link MetricContext}s.
   *
   * @param name name of the child {@link MetricContext} to be built
   * @return a new {@link MetricContext.Builder} for building child {@link MetricContext}s
   */
  public Builder childBuilder(String name) {
    return builder(name).hasParent(this);
  }

  /**
   * Get a new {@link MetricContext.Builder}.
   *
   * @param name name of the {@link MetricContext} to be built
   * @return a new {@link MetricContext.Builder}
   */
  public static Builder builder(String name) {
    return new Builder(name);
  }

  private SortedSet<String> getSimpleNames() {
    return ImmutableSortedSet.copyOf(this.contextAwareMetrics.keySet());
  }

  private Map<String, Metric> getSimplyNamedMetrics() {
    return ImmutableMap.copyOf(this.contextAwareMetrics);
  }

  @SuppressWarnings("unchecked")
  private <T extends Metric> SortedMap<String, T> getSimplyNamedMetrics(Class<T> mClass,
      Optional<MetricFilter> filter) {
    ImmutableSortedMap.Builder<String, T> builder = ImmutableSortedMap.naturalOrder();
    for (Map.Entry<String, Metric> entry : this.contextAwareMetrics.entrySet()) {
      if (mClass.isInstance(entry.getValue())) {
        if (filter.isPresent() && !filter.get().matches(entry.getKey(), entry.getValue())) {
          continue;
        }
        builder.put(entry.getKey(), (T) entry.getValue());
      }
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private <T extends ContextAwareMetric> T getOrCreate(String name, ContextAwareMetricFactory<T> factory) {
    Metric metric = this.contextAwareMetrics.get(name);
    if (metric != null) {
      if (factory.isInstance(metric)) {
        return (T) metric;
      }
      throw new IllegalArgumentException(name + " is already used for a different type of metric");
    }

    Metric newMetric = factory.newMetric(this, name);
    register(name, newMetric);
    return (T) newMetric;
  }

  private boolean removeChildrenMetrics(String name) {
    boolean removed = true;
    for (MetricContext child : this.children.values()) {
      if (!child.remove(name)) {
        removed = false;
      }
    }
    return removed;
  }

  /**
   * A builder class for {@link MetricContext}.
   */
  public static class Builder {

    private final String name;
    private MetricContext parent = null;
    private final List<Tag<?>> tags = Lists.newArrayList();
    private final Map<String, ContextAwareScheduledReporter.Builder> contextAwareReporterBuilders = Maps.newHashMap();
    private boolean reportFullyQualifiedNames = false;
    private boolean includeTagKeys = false;

    public Builder(String name) {
      this.name = name;
    }

    /**
     * Set the parent {@link MetricContext} of this {@link MetricContext} instance.
     *
     * <p>
     *   This method is intentionally made private and is only called in {@link MetricContext#childBuilder(String)}
     *   so users will not mistakenly call this method twice if they use {@link MetricContext#childBuilder(String)}.
     * </p>
     * @param parent the parent {@link MetricContext}
     * @return {@code this}
     */
    private Builder hasParent(MetricContext parent) {
      this.parent = parent;
      // Inherit parent context's tags
      this.tags.addAll(parent.getTags());
      return this;
    }

    /**
     * Add a single {@link Tag}.
     *
     * @param tag the {@link Tag} to add
     * @return {@code this}
     */
    public Builder addTag(Tag<?> tag) {
      this.tags.add(tag);
      return this;
    }

    /**
     * Add a collection of {@link Tag}s.
     *
     * @param tags the collection of {@link Tag}s to add
     * @return {@code this}
     */
    public Builder addTags(Collection<Tag<?>> tags) {
      this.tags.addAll(tags);
      return this;
    }

    /**
     * Add a {@link ContextAwareScheduledReporter}.
     *
     * @param name name of the {@link ContextAwareScheduledReporter}
     * @param builder a {@link ContextAwareScheduledReporter.Builder} used to build the
     *                {@link ContextAwareScheduledReporter}
     * @return {@code this}
     */
    public Builder addContextAwareScheduledReporter(String name, ContextAwareScheduledReporter.Builder builder) {
      this.contextAwareReporterBuilders.put(name, builder);
      return this;
    }

    /**
     * Add {@link ContextAwareScheduledReporter}s.
     *
     * @param builders a map from reporter names to {@link ContextAwareScheduledReporter.Builder}s
     *                 used to build the {@link ContextAwareScheduledReporter}s
     * @return {@code this}
     */
    public Builder addContextAwareScheduledReporters(Map<String, ContextAwareScheduledReporter.Builder> builders) {
      this.contextAwareReporterBuilders.putAll(builders);
      return this;
    }

    /**
     * Configure the {@link MetricContext} to report fully-qualified metric names (including the tags).
     *
     * @param includeTagKeys whether to include tag keys in the metric name prefix
     * @return {@code this}
     */
    public Builder reportFullyQualifiedNames(boolean includeTagKeys) {
      this.reportFullyQualifiedNames = true;
      this.includeTagKeys = includeTagKeys;
      return this;
    }

    /**
     * Builder a new {@link MetricContext}.
     *
     * <p>
     *   See {@link Taggable#metricNamePrefix(boolean)} for the semantic of {@code includeTagKeys}.
     * </p>
     *
     * @return the newly built {@link MetricContext}
     */
    public MetricContext build() {
      return new MetricContext(this.name, this.parent, this.tags, this.contextAwareReporterBuilders,
          this.reportFullyQualifiedNames, this.includeTagKeys);
    }
  }
}
