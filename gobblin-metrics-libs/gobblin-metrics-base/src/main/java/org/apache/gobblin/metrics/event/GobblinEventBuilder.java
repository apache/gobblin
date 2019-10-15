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

package org.apache.gobblin.metrics.event;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;


/**
 * This class is to support semi-typed Gobblin event. Instead of all events represented as
 * instances of {@link GobblinTrackingEvent}. Different types of events can be defined from {@link GobblinEventBuilder},
 * where each can define its own attributes. In this way, one can inspect a Gobblin event with the corresponding event
 * builder instead of looking into the metadata maps, whose keys could be changed without control
 *
 * Note: a {@link GobblinEventBuilder} instance is not reusable
 */
public class GobblinEventBuilder {
  public static final String NAMESPACE = "gobblin.event";
  public static final String EVENT_TYPE = "eventType";

  @Getter
  protected final String name;
  @Getter
  @Setter
  protected String namespace;
  protected final Map<String, String> metadata;

  public GobblinEventBuilder(String name) {
    this(name, null);
  }

  public GobblinEventBuilder(String name, String namespace) {
    this.name = name;
    this.namespace = namespace;
    metadata = Maps.newHashMap();
  }

  public ImmutableMap<String, String> getMetadata() {
    return new ImmutableMap.Builder<String, String>().putAll(metadata).build();
  }

  /**
   * Add a metadata pair
   */
  public void addMetadata(String key, String value) {
    metadata.put(key, value);
  }

  /**
   * Add additional metadata
   */
  public void addAdditionalMetadata(Map<String, String> additionalMetadata) {
    metadata.putAll(additionalMetadata);
  }

  /**
   * Build as {@link GobblinTrackingEvent}
   */
  public GobblinTrackingEvent build() {
    return new GobblinTrackingEvent(0L, namespace, name, metadata);
  }
  /**
   * Submit the event
   * @deprecated Use {@link EventSubmitter#submit(MetricContext, GobblinEventBuilder)}
   */
  @Deprecated
  public void submit(MetricContext context) {
    if(namespace == null) {
      namespace = NAMESPACE;
    }
    context.submitEvent(build());
  }

}
