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
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import com.google.common.base.Optional;


/**
 * A type of {@link com.codahale.metrics.Timer}s that are aware of their {@link gobblin.metrics.MetricContext}
 * and can have associated {@link Tag}s.
 *
 * <p>
 *   Any updates to a {@link ContextAwareTimer} will be applied automatically to the
 *   {@link ContextAwareTimer} of the same name in the parent {@link MetricContext}.
 * </p>
 *
 * <p>
 *   This class wraps a {@link com.codahale.metrics.Timer} and delegates calls to public methods of
 *   {@link com.codahale.metrics.Timer} to the wrapped {@link com.codahale.metrics.Timer}.
 * </p>
 *
 * @author ynli
 */
class ContextAwareTimer extends Timer implements ContextAwareMetric {

  private final String name;
  private final MetricContext context;
  private final Tagged tagged;
  private final Optional<ContextAwareTimer> parentTimer;

  ContextAwareTimer(MetricContext context, String name) {
    this.name = name;
    this.context = context;
    this.tagged = new Tagged();

    Optional<MetricContext> parentContext = context.getParent();
    if (parentContext.isPresent()) {
      this.parentTimer = Optional.fromNullable(parentContext.get().contextAwareTimer(name));
    } else {
      this.parentTimer = Optional.absent();
    }
  }

  @Override
  public void update(long duration, TimeUnit unit) {
    super.update(duration, unit);
    if (this.parentTimer.isPresent()) {
      this.parentTimer.get().update(duration, unit);
    }
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
