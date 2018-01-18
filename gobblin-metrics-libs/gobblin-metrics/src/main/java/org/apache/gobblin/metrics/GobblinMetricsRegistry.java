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

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;


/**
 * Registry that stores instances of {@link GobblinMetrics} identified by an arbitrary string id. The static method
 * {@link #getInstance()} provides a static instance of this this class that should be considered the global registry of
 * metrics.
 *
 * <p>
 *   An application could also instantiate one or more registries to, for example, separate instances of
 *   {@link GobblinMetrics} into different scopes.
 * </p>
 */
public class GobblinMetricsRegistry {

  private static final GobblinMetricsRegistry GLOBAL_INSTANCE = new GobblinMetricsRegistry();

  private final Cache<String, GobblinMetrics> metricsCache = CacheBuilder.newBuilder().build();

  private GobblinMetricsRegistry() {
    // Do nothing
  }

  /**
   * Associate a {@link GobblinMetrics} instance with a given ID if the ID is
   * not already associated with a {@link GobblinMetrics} instance.
   *
   * @param id the given {@link GobblinMetrics} ID
   * @param gobblinMetrics the {@link GobblinMetrics} instance to be associated with the given ID
   * @return the previous {@link GobblinMetrics} instance associated with the ID or {@code null}
   *         if there's no previous {@link GobblinMetrics} instance associated with the ID
   */
  public GobblinMetrics putIfAbsent(String id, GobblinMetrics gobblinMetrics) {
    return this.metricsCache.asMap().putIfAbsent(id, gobblinMetrics);
  }

  /**
   * Get the {@link GobblinMetrics} instance associated with a given ID.
   *
   * @param id the given {@link GobblinMetrics} ID
   * @return the {@link GobblinMetrics} instance associated with the ID, wrapped in an {@link Optional} or
   *         {@link Optional#absent()} if no {@link GobblinMetrics} instance for the given ID is found
   */
  public Optional<GobblinMetrics> get(String id) {
    return Optional.fromNullable(this.metricsCache.getIfPresent(id));
  }

  /**
   * Get the {@link GobblinMetrics} instance associated with a given ID. If the ID is not found this method returns the
   * {@link GobblinMetrics} returned by the given {@link Callable}, and creates a mapping between the specified ID
   * and the {@link GobblinMetrics} instance returned by the {@link Callable}.
   *
   * @param id the given {@link GobblinMetrics} ID
   * @param valueLoader a {@link Callable} that returns a {@link GobblinMetrics}, the {@link Callable} is only invoked
   *                    if the given id is not found
   *
   * @return a {@link GobblinMetrics} instance associated with the id
   */
  public GobblinMetrics getOrDefault(String id, Callable<? extends GobblinMetrics> valueLoader) {
    try {
      return this.metricsCache.get(id, valueLoader);
    } catch (ExecutionException ee) {
      throw Throwables.propagate(ee);
    }
  }

  /**
   * Remove the {@link GobblinMetrics} instance with a given ID.
   *
   * @param id the given {@link GobblinMetrics} ID
   * @return removed {@link GobblinMetrics} instance or {@code null} if no
   *         {@link GobblinMetrics} instance for the given ID is found
   */
  public GobblinMetrics remove(String id) {
    return this.metricsCache.asMap().remove(id);
  }

  /**
   * Get an instance of {@link GobblinMetricsRegistry}.
   *
   * @return an instance of {@link GobblinMetricsRegistry}
   */
  public static GobblinMetricsRegistry getInstance() {
    return GLOBAL_INSTANCE;
  }

  /**
   * <p>
   * Creates {@link org.apache.gobblin.metrics.MetricContext}. Tries to read the name of the parent context
   * from key "metrics.context.name" at state, and tries to get the parent context by name from
   * the {@link org.apache.gobblin.metrics.MetricContext} registry (the parent context must be registered).
   * </p>
   *
   * <p>
   * Automatically adds two tags to the inner context:
   * <ul>
   * <li> component: attempts to determine which component type within gobblin-api generated this instance. </li>
   * <li> class: the specific class of the object that generated this instance of Instrumented </li>
   * </ul>
   * </p>
   *
   */
  public MetricContext getMetricContext(State state, Class<?> klazz, List<Tag<?>> tags) {
    int randomId = new Random().nextInt(Integer.MAX_VALUE);

    List<Tag<?>> generatedTags = Lists.newArrayList();

    if (!klazz.isAnonymousClass()) {
      generatedTags.add(new Tag<>("class", klazz.getCanonicalName()));
    }

    Optional<GobblinMetrics> gobblinMetrics = state.contains(ConfigurationKeys.METRIC_CONTEXT_NAME_KEY)
        ? GobblinMetricsRegistry.getInstance().get(state.getProp(ConfigurationKeys.METRIC_CONTEXT_NAME_KEY))
        : Optional.<GobblinMetrics> absent();

    MetricContext.Builder builder = gobblinMetrics.isPresent()
        ? gobblinMetrics.get().getMetricContext().childBuilder(klazz.getCanonicalName() + "." + randomId)
        : MetricContext.builder(klazz.getCanonicalName() + "." + randomId);
    return builder.addTags(generatedTags).addTags(tags).build();
  }
}
