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

package gobblin.metrics.reporter.util;

import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import gobblin.metrics.Event;
import gobblin.metrics.MetricContext;


/**
 * Wrapper around Avro {@link gobblin.metrics.Event.Builder} simplifying handling {@link gobblin.metrics.Event}s.
 *
 * <p>
 *   Instances of this class are immutable. Calling set* methods returns a copy of the calling instance.
 * </p>
 */
public class EventBuilder {

  private final Event.Builder builder;
  private final Optional<MetricContext> context;

  public EventBuilder(Optional<MetricContext> context) {
    this.builder = Event.newBuilder();
    this.context = context;
  }

  public EventBuilder(MetricContext context) {
    this(Optional.of(context));
  }

  public EventBuilder(EventBuilder other) {
    this.builder = other.builder;
    this.context = other.context;
  }

  /**
   * Set namespace of the {@link gobblin.metrics.Event}.
   * @return a copy of this instance with the new namespace.
   */
  public EventBuilder setNamespace(String namespace) {
    EventBuilder other = new EventBuilder(this);
    other.builder.setNamespace(namespace);
    return other;
  }

  /**
   * Set name of the {@link gobblin.metrics.Event}.
   * @return a copy of this instance with the new name.
   */
  public EventBuilder setName(String value) {
    EventBuilder other = new EventBuilder(this);
    other.builder.setName(value);
    return other;
  }

  /**
   * Adds metadata to the {@link gobblin.metrics.Event}.
   * @return a copy of this instance with the new metadata.
   */
  public EventBuilder addMetadata(String key, String value) {
    EventBuilder other = new EventBuilder(this);
    Map<String, String> metadata;
    if(other.builder.hasMetadata()) {
      metadata = other.builder.getMetadata();
    } else {
      metadata = Maps.newHashMap();
    }
    metadata.put(key, value);
    other.builder.setMetadata(metadata);
    return other;
  }

  /**
   * Builds and submits the {@link gobblin.metrics.Event} to the {@link gobblin.metrics.MetricContext}.
   */
  public void submit() {
    if(this.context.isPresent()) {
      this.context.get().sendEvent(this.builder.build());
    }
  }
}
