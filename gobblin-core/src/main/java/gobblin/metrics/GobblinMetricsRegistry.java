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

package gobblin.metrics;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;


/**
 * Registry that stores instances of {@link GobblinMetrics} identified by an arbitrary string id.
 * The static method getInstance() provides a static instance of this this class that should be considered
 * the global registry of metrics.
 * An application could also instantiate one or more registries to for example separate instances of
 * {@link GobblinMetrics} into different scopes.
 */
public class GobblinMetricsRegistry {

  private static final GobblinMetricsRegistry GLOBAL_INSTANCE = new GobblinMetricsRegistry();

  private final Cache<String, GobblinMetrics> metricsMap = CacheBuilder.newBuilder().softValues().build();

  private GobblinMetricsRegistry() {

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
    return this.metricsMap.asMap().putIfAbsent(id, gobblinMetrics);
  }

  /**
   * Get the {@link GobblinMetrics} instance associated with a given ID.
   *
   * @param id the given {@link GobblinMetrics} ID
   * @return the {@link GobblinMetrics} instance associated with the ID or {@code null}
   *         if no {@link GobblinMetrics} instance for the given ID is found
   */
  public GobblinMetrics get(String id) {
    return this.metricsMap.getIfPresent(id);
  }

  /**
   * Get the {@link GobblinMetrics} instance associated with a given ID or the given default
   * {@link GobblinMetrics} instance if no {@link GobblinMetrics} instance for the given ID
   * is found.
   *
   * @param id the given {@link GobblinMetrics} ID
   * @param defaultValue the default {@link GobblinMetrics} instance
   * @return the {@link GobblinMetrics} instance associated with a given ID or the given default
   *         {@link GobblinMetrics} instance if no {@link GobblinMetrics} instance for the given ID
   *         is found
   */
  public GobblinMetrics getOrDefault(String id, final GobblinMetrics defaultValue) {
    try {
      return this.metricsMap.get(id, new Callable<GobblinMetrics>() {
        @Override
        public GobblinMetrics call() throws Exception {
          return defaultValue;
        }
      });
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
    return this.metricsMap.asMap().remove(id);
  }

  /**
   * Get an instance of {@link GobblinMetricsRegistry}.
   *
   * @return an instance of {@link GobblinMetricsRegistry}
   */
  public static GobblinMetricsRegistry getInstance() {
    return GLOBAL_INSTANCE;
  }

}
