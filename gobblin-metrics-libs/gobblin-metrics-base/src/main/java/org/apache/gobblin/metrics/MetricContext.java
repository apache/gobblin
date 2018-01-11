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

import lombok.Getter;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.gobblin.metrics.context.NameConflictException;
import org.apache.gobblin.metrics.context.ReportableContext;
import org.apache.gobblin.metrics.notification.EventNotification;
import org.apache.gobblin.metrics.notification.Notification;
import org.apache.gobblin.util.ExecutorsUtils;


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
 * @author Yinan Li
 */
public class MetricContext extends MetricRegistry implements ReportableContext, Closeable {

  protected final Closer closer;

  public static final String METRIC_CONTEXT_ID_TAG_NAME = "metricContextID";
  public static final String METRIC_CONTEXT_NAME_TAG_NAME = "metricContextName";

  @Getter
  private final InnerMetricContext innerMetricContext;

  private static final Logger LOG = LoggerFactory.getLogger(MetricContext.class);

  public static final String GOBBLIN_METRICS_NOTIFICATIONS_TIMER_NAME = "gobblin.metrics.notifications.timer";

  // Targets for notifications.
  private final Map<UUID, Function<Notification, Void>> notificationTargets;
  private final ContextAwareTimer notificationTimer;

  private Optional<ExecutorService> executorServiceOptional;

  // This set exists so that metrics that have no hard references in code don't get GCed while the MetricContext
  // is alive.
  private final Set<ContextAwareMetric> contextAwareMetricsSet;

  protected MetricContext(String name, MetricContext parent, List<Tag<?>> tags, boolean isRoot) throws NameConflictException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name));

    this.closer = Closer.create();

    try {
      this.innerMetricContext = this.closer.register(new InnerMetricContext(this, name, parent, tags));
    } catch(ExecutionException ee) {
      throw Throwables.propagate(ee);
    }

    this.contextAwareMetricsSet = Sets.newConcurrentHashSet();

    this.notificationTargets = Maps.newConcurrentMap();
    this.executorServiceOptional = Optional.absent();

    this.notificationTimer = new ContextAwareTimer(this, GOBBLIN_METRICS_NOTIFICATIONS_TIMER_NAME);
    register(this.notificationTimer);

    if (!isRoot) {
      RootMetricContext.get().addMetricContext(this);
    }
  }

  private synchronized ExecutorService getExecutorService() {
    if(!this.executorServiceOptional.isPresent()) {
      this.executorServiceOptional = Optional.of(MoreExecutors.getExitingExecutorService(
          (ThreadPoolExecutor) Executors.newCachedThreadPool(ExecutorsUtils.newThreadFactory(Optional.of(LOG),
              Optional.of("MetricContext-" + getName() + "-%d"))), 5,
          TimeUnit.MINUTES));
    }
    return this.executorServiceOptional.get();
  }

  /**
   * Get the name of this {@link MetricContext}.
   *
   * @return the name of this {@link MetricContext}
   */
  public String getName() {
    return this.innerMetricContext.getName();
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
    return this.innerMetricContext.getParent();
  }

  /**
   * Get a view of the child {@link org.apache.gobblin.metrics.MetricContext}s as a {@link com.google.common.collect.ImmutableMap}.
   * @return {@link com.google.common.collect.ImmutableMap} of
   *      child {@link org.apache.gobblin.metrics.MetricContext}s keyed by their names.
   */
  public Map<String, MetricContext> getChildContextsAsMap() {
    return this.innerMetricContext.getChildContextsAsMap();
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
    return this.innerMetricContext.getNames();
  }

  /**
   * Inject the tags of this {@link MetricContext} to the given {@link GobblinTrackingEvent}
   */
  private void injectTagsToEvent(GobblinTrackingEvent event) {
    Map<String, String> originalMetadata = event.getMetadata();
    Map<String, Object> tags = getTagMap();
    Map<String, String> newMetadata = Maps.newHashMap();
    for(Map.Entry<String, Object> entry : tags.entrySet()) {
      newMetadata.put(entry.getKey(), entry.getValue().toString());
    }
    newMetadata.putAll(originalMetadata);
    event.setMetadata(newMetadata);
  }

  /**
   * Submit {@link org.apache.gobblin.metrics.GobblinTrackingEvent} to all notification listeners attached to this or any
   * ancestor {@link org.apache.gobblin.metrics.MetricContext}s. The argument for this method is mutated by the method, so it
   * should not be reused by the caller.
   *
   * @param nonReusableEvent {@link GobblinTrackingEvent} to submit. This object will be mutated by the method,
   *                                                     so it should not be reused by the caller.
   */
  public void submitEvent(GobblinTrackingEvent nonReusableEvent) {
    nonReusableEvent.setTimestamp(System.currentTimeMillis());
    injectTagsToEvent(nonReusableEvent);

    EventNotification notification = new EventNotification(nonReusableEvent);
    sendNotification(notification);
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
    return this.innerMetricContext.getMetrics();
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
    return this.innerMetricContext.getGauges(MetricFilter.ALL);
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
    return this.innerMetricContext.getGauges(filter);
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
    return this.innerMetricContext.getCounters(MetricFilter.ALL);
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
    return this.innerMetricContext.getCounters(filter);
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
    return this.innerMetricContext.getHistograms(MetricFilter.ALL);
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
    return this.innerMetricContext.getHistograms(filter);
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
    return this.innerMetricContext.getMeters(MetricFilter.ALL);
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
    return this.innerMetricContext.getMeters(filter);
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
    return this.innerMetricContext.getTimers(MetricFilter.ALL);
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
    return this.innerMetricContext.getTimers(filter);
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
  public ContextAwareTimer timer(String name) {
    return contextAwareTimer(name);
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
  public synchronized <T extends Metric> T register(String name, T metric)
      throws IllegalArgumentException {
    if(!(metric instanceof ContextAwareMetric)) {
      throw new UnsupportedOperationException("Can only register ContextAwareMetrics.");
    }
    return this.innerMetricContext.register(name, metric);
  }

  /**
   * Register a {@link org.apache.gobblin.metrics.ContextAwareMetric} under its own name.
   */
  public <T extends ContextAwareMetric> T register(T metric) throws IllegalArgumentException {
    return register(metric.getName(), metric);
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
    return this.innerMetricContext.getOrCreate(name, factory);
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
    return this.innerMetricContext.getOrCreate(name, factory);
  }

  /**
   * Get a {@link ContextAwareHistogram} with a given name.
   *
   * @param name name of the {@link ContextAwareHistogram}
   * @return the {@link ContextAwareHistogram} with the given name
   */
  public ContextAwareHistogram contextAwareHistogram(String name) {
    return this.innerMetricContext.getOrCreate(name, ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_HISTOGRAM_FACTORY);
  }

  /**
   * Get a {@link ContextAwareHistogram} with a given name and a customized {@link com.codahale.metrics.SlidingTimeWindowReservoir}
   *
   * @param name name of the {@link ContextAwareHistogram}
   * @param windowSize normally the duration of the time window
   * @param unit the unit of time
   * @return the {@link ContextAwareHistogram} with the given name
   */
  public ContextAwareHistogram contextAwareHistogram(String name, long windowSize, TimeUnit unit) {
    ContextAwareMetricFactoryArgs.SlidingTimeWindowArgs args = new ContextAwareMetricFactoryArgs.SlidingTimeWindowArgs(
        this.innerMetricContext.getMetricContext().get(), name, windowSize, unit);
    return this.innerMetricContext.getOrCreate(ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_HISTOGRAM_FACTORY, args);
  }

  /**
   * Get a {@link ContextAwareTimer} with a given name.
   *
   * @param name name of the {@link ContextAwareTimer}
   * @return the {@link ContextAwareTimer} with the given name
   */
  public ContextAwareTimer contextAwareTimer(String name) {
    return this.innerMetricContext.getOrCreate(name, ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_TIMER_FACTORY);
  }

  /**
   * Get a {@link ContextAwareTimer} with a given name and a customized {@link com.codahale.metrics.SlidingTimeWindowReservoir}
   *
   * @param name name of the {@link ContextAwareTimer}
   * @param windowSize normally the duration of the time window
   * @param unit the unit of time
   * @return the {@link ContextAwareTimer} with the given name
   */
  public ContextAwareTimer contextAwareTimer(String name, long windowSize, TimeUnit unit) {
    ContextAwareMetricFactoryArgs.SlidingTimeWindowArgs args = new ContextAwareMetricFactoryArgs.SlidingTimeWindowArgs(
        this.innerMetricContext.getMetricContext().get(), name, windowSize, unit);
    return this.innerMetricContext.getOrCreate(ContextAwareMetricFactory.DEFAULT_CONTEXT_AWARE_TIMER_FACTORY, args);
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
  public synchronized boolean remove(String name) {
    return this.innerMetricContext.remove(name);
  }

  @Override
  public void removeMatching(MetricFilter filter) {
    this.innerMetricContext.removeMatching(filter);
  }

  public List<Tag<?>> getTags() {
    return this.innerMetricContext.getTags();
  }

  public Map<String, Object> getTagMap() {
    return this.innerMetricContext.getTagMap();
  }

  @Override
  public void close() throws IOException {
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

  /**
   * Add a target for {@link org.apache.gobblin.metrics.notification.Notification}s.
   * @param target A {@link com.google.common.base.Function} that will be run every time
   *               there is a new {@link org.apache.gobblin.metrics.notification.Notification} in this context.
   * @return a key for this notification target. Can be used to remove the notification target later.
   */
  public UUID addNotificationTarget(Function<Notification, Void> target) {

    UUID uuid = UUID.randomUUID();
    if(this.notificationTargets.containsKey(uuid)) {
      throw new RuntimeException("Failed to create notification target.");
    }

    this.notificationTargets.put(uuid, target);
    return uuid;

  }

  /**
   * Remove notification target identified by the given key.
   * @param key key for the notification target to remove.
   */
  public void removeNotificationTarget(UUID key) {
    this.notificationTargets.remove(key);
  }

  /**
   * Send a notification to all targets of this context and to the parent of this context.
   * @param notification {@link org.apache.gobblin.metrics.notification.Notification} to send.
   */
  public void sendNotification(final Notification notification) {

    ContextAwareTimer.Context timer = this.notificationTimer.time();
    if(!this.notificationTargets.isEmpty()) {
        for (final Map.Entry<UUID, Function<Notification, Void>> entry : this.notificationTargets.entrySet()) {
          try {
            entry.getValue().apply(notification);
          } catch (RuntimeException exception) {
            LOG.warn("RuntimeException when running notification target. Skipping.", exception);
          }
        }
    }

    if(getParent().isPresent()) {
      getParent().get().sendNotification(notification);
    }
    timer.stop();
  }

  void addChildContext(String childContextName, MetricContext childContext) throws NameConflictException,
      ExecutionException {
    this.innerMetricContext.addChildContext(childContextName, childContext);
  }

  void addToMetrics(ContextAwareMetric metric) {
    this.contextAwareMetricsSet.add(metric);
  }

  void removeFromMetrics(ContextAwareMetric metric) {
    this.contextAwareMetricsSet.remove(metric);
  }

  @VisibleForTesting
  void clearNotificationTargets() {
    this.notificationTargets.clear();
  }

  /**
   * A builder class for {@link MetricContext}.
   */
  public static class Builder {

    private String name;
    private MetricContext parent = null;
    private final List<Tag<?>> tags = Lists.newArrayList();

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
     * Builder a new {@link MetricContext}.
     *
     * <p>
     *   See {@link Taggable#metricNamePrefix(boolean)} for the semantic of {@code includeTagKeys}.
     * </p>
     *
     * <p>
     *   Note this builder may change the name of the built {@link MetricContext} if the parent context already has a child with
     *   that name. If this is unacceptable, use {@link #buildStrict} instead.
     * </p>
     *
     * @return the newly built {@link MetricContext}
     */
    public MetricContext build() {
      try {
        return buildStrict();
      } catch (NameConflictException nce) {
        String uuid = UUID.randomUUID().toString();
        LOG.warn("MetricContext with specified name already exists, appending UUID to the given name: " + uuid);

        this.name = this.name + "_" + uuid;
        try {
          return buildStrict();
        } catch (NameConflictException nce2) {
          throw Throwables.propagate(nce2);
        }
      }
    }

    /**
     * Builder a new {@link MetricContext}.
     *
     * <p>
     *   See {@link Taggable#metricNamePrefix(boolean)} for the semantic of {@code includeTagKeys}.
     * </p>
     *
     * @return the newly built {@link MetricContext}
     * @throws NameConflictException if the parent {@link MetricContext} already has a child with this name.
     */
    public MetricContext buildStrict() throws NameConflictException {
      if(this.parent == null) {
        this.parent = RootMetricContext.get();
      }
      return new MetricContext(this.name, this.parent, this.tags, false);
    }

  }
}
