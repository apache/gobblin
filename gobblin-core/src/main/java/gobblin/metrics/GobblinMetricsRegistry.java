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

  public static GobblinMetricsRegistry getInstance() {
    return GLOBAL_INSTANCE;
  }

  private final Cache<String, GobblinMetrics> metricsMap = CacheBuilder.newBuilder().softValues().build();

  public GobblinMetrics putIfAbsent(String id, GobblinMetrics gobblinMetrics) {
    return this.metricsMap.asMap().putIfAbsent(id, gobblinMetrics);
  }

  public boolean containsKey(String id) {
    return this.metricsMap.asMap().containsKey(id);
  }

  public GobblinMetrics get(String id) {
    return this.metricsMap.getIfPresent(id);
  }

  /**
   * Remove the {@link GobblinMetrics} instance with the given ID.
   *
   * @param id the given {@link GobblinMetrics} ID
   * @return removed {@link GobblinMetrics} instance or <code>null</code> if no {@link GobblinMetrics}
   *         instance for the given job is not found
   */
  public GobblinMetrics remove(String id) {
    return this.metricsMap.asMap().remove(id);
  }

}
