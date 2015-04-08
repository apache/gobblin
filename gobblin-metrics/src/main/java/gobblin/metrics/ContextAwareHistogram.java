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

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;

import com.google.common.base.Optional;


/**
 * A type of {@link com.codahale.metrics.Histogram} that are aware of their {@link gobblin.metrics.MetricContext}
 * and can have associated {@link Tag}s.
 *
 * <p>
 *   Any updates to a {@link ContextAwareHistogram} will be applied automatically to the
 *   {@link ContextAwareHistogram} of the same name in the parent {@link MetricContext}.
 * </p>
 *
 * <p>
 *   This class wraps a {@link com.codahale.metrics.Histogram} and delegates calls to public methods of
 *   {@link com.codahale.metrics.Histogram} to the wrapped {@link com.codahale.metrics.Histogram}.
 * </p>
 *
 * @author ynli
 */
class ContextAwareHistogram extends Histogram implements ContextAwareMetric {

  private final String name;
  private final MetricContext context;
  private final Histogram histogram;
  private final Tagged tagged;
  private final Optional<ContextAwareHistogram> parentHistogram;

  ContextAwareHistogram(MetricContext context, String name, Histogram histogram) {
    super(new ExponentiallyDecayingReservoir());

    this.name = name;
    this.context = context;
    this.histogram = histogram;
    this.tagged = new Tagged();

    Optional<MetricContext> parentContext = context.getParent();
    if (parentContext.isPresent()) {
      this.parentHistogram = Optional.fromNullable(parentContext.get().contextAwareHistogram(name));
    } else {
      this.parentHistogram = Optional.absent();
    }
  }

  @Override
  public void update(int value) {
    this.histogram.update(value);
    if (this.parentHistogram.isPresent()) {
      this.parentHistogram.get().update(value);
    }
  }

  @Override
  public void update(long value) {
    this.histogram.update(value);
    if (this.parentHistogram.isPresent()) {
      this.parentHistogram.get().update(value);
    }
  }

  @Override
  public long getCount() {
    return this.histogram.getCount();
  }

  @Override
  public Snapshot getSnapshot() {
    return this.histogram.getSnapshot();
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String getFullyQualifiedName() {
    return MetricRegistry.name(metricNamePrefix(), this.name);
  }

  @Override
  public MetricContext getContext() {
    return this.context;
  }

  @Override
  public void addTag(Tag tag) {
    this.tagged.addTag(tag);
  }

  @Override
  public void addTags(Collection<Tag> tags) {
    this.tagged.addTags(tags);
  }

  @Override
  public List<Tag> getTags() {
    return this.tagged.getTags();
  }

  @Override
  public String metricNamePrefix() {
    return this.tagged.metricNamePrefix();
  }
}
