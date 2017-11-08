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

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;


/**
 * A general gobblin event builder which builds a {@link GobblinTrackingEvent}
 *
 * Note: a {@link GobblinEventBuilder} instance is not reusable
 */
public class GobblinEventBuilder {
  public static final String NAMESPACE = "gobblin.event";
  public static final String EVENT_TYPE = "eventType";

  @Getter
  protected final String name;
  @Getter
  protected final String namespace;
  protected final Map<String, String> metadata;

  public GobblinEventBuilder(String name) {
    this(name, NAMESPACE);
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
   */
  public void submit(MetricContext context) {
    context.submitEvent(build());
  }
}
