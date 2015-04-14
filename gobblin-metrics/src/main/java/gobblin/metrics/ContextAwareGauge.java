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
import java.util.List;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;


/**
 * A type of {@link com.codahale.metrics.Gauge}s that that are aware of their {@link MetricContext}
 * and can have associated {@link Tag}s.
 *
 * <p>
 *   This class wraps a {@link com.codahale.metrics.Gauge} and delegates calls to public methods of
 *   {@link com.codahale.metrics.Gauge} to the wrapped {@link com.codahale.metrics.Gauge}.
 * </p>
 *
 * @param <T> the type of the {@link ContextAwareGauge}'s value
 *
 * @author ynli
 */
public class ContextAwareGauge<T> implements Gauge<T>, ContextAwareMetric {

  private final String name;
  private final MetricContext context;
  private final Tagged tagged;
  private final Gauge<T> gauge;

  ContextAwareGauge(MetricContext context, String name, Gauge<T> gauge) {
    this.name = name;
    this.context = context;
    this.tagged = new Tagged();
    this.gauge = gauge;
  }

  @Override
  public T getValue() {
    return this.gauge.getValue();
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String getFullyQualifiedName(boolean includeTagKeys) {
    return MetricRegistry.name(metricNamePrefix(includeTagKeys), this.name);
  }

  @Override
  public MetricContext getContext() {
    return this.context;
  }

  @Override
  public void addTag(Tag<?> tag) {
    this.tagged.addTag(tag);
  }

  @Override
  public void addTags(Collection<Tag<?>> tags) {
    this.tagged.addTags(tags);
  }

  @Override
  public List<Tag<?>> getTags() {
    return this.tagged.getTags();
  }

  @Override
  public String metricNamePrefix(boolean includeTagKeys) {
    return this.tagged.metricNamePrefix(includeTagKeys);
  }
}
