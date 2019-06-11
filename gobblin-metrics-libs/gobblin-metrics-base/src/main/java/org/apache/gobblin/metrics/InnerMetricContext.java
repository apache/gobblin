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

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import lombok.Getter;

import org.apache.gobblin.metrics.context.ContextWeakReference;
import org.apache.gobblin.metrics.context.NameConflictException;
import org.apache.gobblin.metrics.context.ReportableContext;
import org.apache.gobblin.metrics.metric.InnerMetric;


/**
 * Contains list of {@link Metric}s, {@link Tag}s, as well as references to parent and child {@link MetricContext} for
 * a {@link MetricContext}. This object is only references by the corresponding {@link MetricContext} as well as the
 * {@link RootMetricContext}, and it is used to report metrics one last time after the corresponding {@link MetricContext}
 * has been GCed.
 */
public class InnerMetricContext extends MetricRegistry implements ReportableContext, Closeable {

  private final Closer closer;

  // Name of this context
  private final String name;

  // A map from simple names to context-aware metrics. All metrics registered with this context (using
  // their simple names) are also registered with the MetricRegistry using their fully-qualified names.
  private final ConcurrentMap<String, InnerMetric> contextAwareMetrics = Maps.newConcurrentMap();

  // This is used to work on tags associated with this context
  private final Tagged tagged;

  // Reference to the parent context wrapped in an Optional as there may be no parent context
  private final Optional<MetricContext> parent;

  // A map from child context names to child contexts
  private final Cache<String, MetricContext> children = CacheBuilder.newBuilder().weakValues().build();

  @Getter
  private final WeakReference<MetricContext> metricContext;

  protected InnerMetricContext(MetricContext context, String name, MetricContext parent, List<Tag<?>> tags)
      throws NameConflictException, ExecutionException {

    this.name = name;
    this.closer = Closer.create();
    this.parent = Optional.fromNullable(parent);
    if (this.parent.isPresent()) {
      this.parent.get().addChildContext(this.name, context);
      this.metricContext = new ContextWeakReference(context, this);
    } else {
      this.metricContext = new WeakReference<>(context);
    }
    this.tagged = new Tagged(tags);
    this.tagged.addTag(new Tag<>(MetricContext.METRIC_CONTEXT_ID_TAG_NAME, UUID.randomUUID().toString()));
    this.tagged.addTag(new Tag<>(MetricContext.METRIC_CONTEXT_NAME_TAG_NAME, name));
  }

  /**
   * Get the name of this {@link MetricContext}.
   *
   * @return the name of this {@link MetricContext}
   */
  @Override
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
  @Override
  public Optional<MetricContext> getParent() {
    return this.parent;
  }

  /**
   * Add a {@link MetricContext} as a child of this {@link MetricContext} if it is not currently a child.
   *
   * @param childContextName the name of the child {@link MetricContext}
   * @param childContext the child {@link MetricContext} to add
   */
  public synchronized void addChildContext(String childContextName, final MetricContext childContext)
      throws NameConflictException, ExecutionException {
    if (this.children.get(childContextName, new Callable<MetricContext>() {
      @Override
      public MetricContext call() throws Exception {
        return childContext;
      }
    }) != childContext) {
      throw new NameConflictException("A child context with that name already exists.");
    }
  }

  /**
   * Get a view of the child {@link org.apache.gobblin.metrics.MetricContext}s as a {@link com.google.common.collect.ImmutableMap}.
   * @return {@link com.google.common.collect.ImmutableMap} of
   *      child {@link org.apache.gobblin.metrics.MetricContext}s keyed by their names.
   */
  @Override
  public Map<String, MetricContext> getChildContextsAsMap() {
    return ImmutableMap.copyOf(this.children.asMap());
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
    return getSimpleNames();
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
  public Map<String, com.codahale.metrics.Metric> getMetrics() {
    return getSimplyNamedMetrics();
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
    return getSimplyNamedMetrics(Gauge.class, Optional.of(filter));
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
    return getSimplyNamedMetrics(Counter.class, Optional.of(filter));
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
    return getSimplyNamedMetrics(Histogram.class, Optional.of(filter));
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
    return getSimplyNamedMetrics(Meter.class, Optional.of(filter));
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
    return getSimplyNamedMetrics(Timer.class, Optional.of(filter));
  }

  /**
   * Register a given metric under a given name.
   *
   * <p>
   *   This method does not support registering {@link com.codahale.metrics.MetricSet}s.
   *   See{@link #registerAll(com.codahale.metrics.MetricSet)}.
   * </p>
   *
   * <p>
   *   This method will not register a metric with the same name in the parent context (if it exists).
   * </p>
   */
  @Override
  public synchronized <T extends com.codahale.metrics.Metric> T register(String name, T metric)
      throws IllegalArgumentException {
    if (!(metric instanceof ContextAwareMetric)) {
      throw new UnsupportedOperationException("Can only register ContextAwareMetrics");
    }
    if (this.contextAwareMetrics.putIfAbsent(name, ((ContextAwareMetric) metric).getInnerMetric()) != null) {
      throw new IllegalArgumentException("A metric named " + name + " already exists");
    }
    MetricContext metricContext = this.metricContext.get();
    if (metricContext != null) {
      metricContext.addToMetrics((ContextAwareMetric) metric);
    }
    // Also register the metric with the MetricRegistry using its fully-qualified name
    return metric;
  }

  /**
   * Register a {@link org.apache.gobblin.metrics.ContextAwareMetric} under its own name.
   */
  public <T extends ContextAwareMetric> T register(T metric) throws IllegalArgumentException {
    return register(metric.getName(), metric);
  }

  @Override
  public void registerAll(MetricSet metrics) throws IllegalArgumentException {
    throw new UnsupportedOperationException();
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
  public synchronized boolean remove(String name) {
    MetricContext metricContext = this.metricContext.get();
    if (metricContext != null) {
      metricContext.removeFromMetrics(this.contextAwareMetrics.get(name).getContextAwareMetric());
    }
    return this.contextAwareMetrics.remove(name) != null && removeChildrenMetrics(name);
  }

  @Override
  public void removeMatching(MetricFilter filter) {
    for (Map.Entry<String, InnerMetric> entry : this.contextAwareMetrics.entrySet()) {
      if (filter.matches(entry.getKey(), entry.getValue().getContextAwareMetric())) {
        remove(entry.getKey());
      }
    }
  }

  @Override
  public List<Tag<?>> getTags() {
    return this.tagged.getTags();
  }

  @Override
  public Map<String, Object> getTagMap() {
    return this.tagged.getTagMap();
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  private SortedSet<String> getSimpleNames() {
    return ImmutableSortedSet.copyOf(this.contextAwareMetrics.keySet());
  }

  private Map<String, com.codahale.metrics.Metric> getSimplyNamedMetrics() {
    return ImmutableMap.<String, com.codahale.metrics.Metric> copyOf(this.contextAwareMetrics);
  }

  @SuppressWarnings("unchecked")
  private <T extends com.codahale.metrics.Metric> SortedMap<String, T> getSimplyNamedMetrics(Class<T> mClass,
      Optional<MetricFilter> filter) {
    ImmutableSortedMap.Builder<String, T> builder = ImmutableSortedMap.naturalOrder();
    for (Map.Entry<String, InnerMetric> entry : this.contextAwareMetrics.entrySet()) {
      if (mClass.isInstance(entry.getValue())) {
        if (filter.isPresent() && !filter.get().matches(entry.getKey(), entry.getValue().getContextAwareMetric())) {
          continue;
        }
        builder.put(entry.getKey(), (T) entry.getValue());
      }
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  protected synchronized <T extends ContextAwareMetric> T getOrCreate(String name,
      ContextAwareMetricFactory<T> factory) {
    InnerMetric metric = this.contextAwareMetrics.get(name);
    if (metric != null) {
      if (factory.isInstance(metric)) {
        return (T) metric.getContextAwareMetric();
      }
      throw new IllegalArgumentException(name + " is already used for a different type of metric");
    }

    T newMetric = factory.newMetric(this.metricContext.get(), name);
    this.register(name, newMetric);
    return newMetric;
  }

  @SuppressWarnings("unchecked")
  protected synchronized <T extends ContextAwareMetric> T getOrCreate(
      ContextAwareMetricFactory<T> factory, ContextAwareMetricFactoryArgs args) {
    String name = args.getName();
    InnerMetric metric = this.contextAwareMetrics.get(name);
    if (metric != null) {
      if (factory.isInstance(metric)) {
        return (T) metric.getContextAwareMetric();
      }
      throw new IllegalArgumentException(name + " is already used for a different type of metric");
    }

    T newMetric = factory.newMetric(args);
    this.register(name, newMetric);
    return newMetric;
  }

  private boolean removeChildrenMetrics(String name) {
    boolean removed = true;
    for (MetricContext child : getChildContextsAsMap().values()) {
      if (!child.remove(name)) {
        removed = false;
      }
    }
    return removed;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("InnerMetricContext Name: ");
    stringBuilder.append(this.name);
    if (this.getParent().isPresent()) {
      stringBuilder.append(", Parent Name: ");
      stringBuilder.append(this.getParent().get().getName());
    } else {
      stringBuilder.append(", No Parent Context");
    }
    stringBuilder.append(", Number of Children: ");
    stringBuilder.append(this.getChildContextsAsMap().size());
    stringBuilder.append(", Tags: ");
    stringBuilder.append(Joiner.on(", ").withKeyValueSeparator(" : ").useForNull("NULL").join(this.getTagMap()));
    return stringBuilder.toString();
  }
}
