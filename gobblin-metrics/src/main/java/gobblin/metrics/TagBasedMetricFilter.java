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

import java.util.Collection;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;

import com.google.common.collect.ImmutableSet;


/**
 * A type of {@link com.codahale.metrics.MetricFilter}s that matches {@link com.codahale.metrics.Metric}s
 * of type {@link Taggable} that have some given {@link Tag}s.
 *
 * @author ynli
 */
public class TagBasedMetricFilter implements MetricFilter {

  private final Collection<Tag> tags;

  public TagBasedMetricFilter(Collection<Tag> tags) {
    this.tags = ImmutableSet.copyOf(tags);
  }

  @Override
  public boolean matches(String name, Metric metric) {
    return metric instanceof Taggable && ((Taggable) metric).getTags().containsAll(this.tags);
  }
}
