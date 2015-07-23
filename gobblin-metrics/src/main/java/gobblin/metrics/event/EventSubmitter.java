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

package gobblin.metrics.event;

import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import gobblin.metrics.GobblinTrackingEvent;
import gobblin.metrics.MetricContext;


/**
 * Wrapper around Avro {@link gobblin.metrics.GobblinTrackingEvent.Builder} simplifying handling {@link gobblin.metrics.GobblinTrackingEvent}s.
 *
 * <p>
 *   Instances of this class are immutable. Calling set* methods returns a copy of the calling instance.
 * </p>
 */
public class EventSubmitter {

  public static final String EVENT_TYPE = "eventType";

  private final Map<String, String> metadata;
  private final String namespace;
  private final Optional<MetricContext> metricContext;

  public static class Builder {
    private final Optional<MetricContext> metricContext;
    private final Map<String, String> metadata;
    private final String namespace;

    public Builder(MetricContext metricContext, String namespace) {
      this(Optional.of(metricContext), namespace);
    }

    public Builder(Optional<MetricContext> metricContext, String namespace) {
      this.metricContext = metricContext;
      this.namespace = namespace;
      this.metadata = Maps.newHashMap();
    }

    public Builder addMetadata(String key, String value) {
      this.metadata.put(key, value);
      return this;
    }

    public EventSubmitter build() {
      return new EventSubmitter(this);
    }
  }

  private EventSubmitter(Builder builder) {
    this.metadata = builder.metadata;
    this.namespace = builder.namespace;
    this.metricContext = builder.metricContext;
  }

  /**
   * Submits the {@link gobblin.metrics.GobblinTrackingEvent} to the {@link gobblin.metrics.MetricContext}.
   * @param name Name of the event.
   */
  public void submit(String name) {
    submit(name, ImmutableMap.<String, String>of());
  }

  /**
   * Submits the {@link gobblin.metrics.GobblinTrackingEvent} to the {@link gobblin.metrics.MetricContext}.
   * @param name Name of the event.
   * @param metadataEls List of keys and values for metadata of the form key1, value2, key2, value2, ...
   */
  public void submit(String name, String... metadataEls) {
    if(metadataEls.length % 2 != 0) {
      throw new IllegalArgumentException("Unmatched keys in metadata elements.");
    }

    Map<String, String> metadata = Maps.newHashMap();
    for(int i = 0; i < metadataEls.length/2; i++) {
      metadata.put(metadataEls[2 * i], metadataEls[2 * i + 1]);
    }
    submit(name, metadata);
  }

  /**
   * Submits the {@link gobblin.metrics.GobblinTrackingEvent} to the {@link gobblin.metrics.MetricContext}.
   * @param name Name of the event.
   * @param additionalMetadata Additional metadata to be added to the event.
   */
  public void submit(String name, Map<String, String> additionalMetadata) {
    if(this.metricContext.isPresent()) {
      Map<String, String> finalMetadata = Maps.newHashMap(this.metadata);
      if(!additionalMetadata.isEmpty()) {
        finalMetadata.putAll(additionalMetadata);
      }

      // Timestamp is set by metric context.
      this.metricContext.get().submitEvent(new GobblinTrackingEvent(0l, this.namespace, name, finalMetadata));
    }
  }

  /**
   * Get a {@link gobblin.metrics.event.TimingEvent} attached to this {@link gobblin.metrics.event.EventSubmitter}.
   * @param name Name of the {@link gobblin.metrics.GobblinTrackingEvent} that will be generated.
   * @return a {@link gobblin.metrics.event.TimingEvent}.
   */
  public TimingEvent getTimingEvent(String name) {
    return new TimingEvent(this, name);
  }
}
