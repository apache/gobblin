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

package org.apache.gobblin.metrics.event.lineage;


import java.util.Map;

import org.apache.commons.lang.StringUtils;

import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * The builder builds a specific {@link GobblinTrackingEvent} whose metadata has {@value GobblinEventBuilder#EVENT_TYPE}
 * to be {@value LineageEventBuilder#LINEAGE_EVENT_TYPE}
 *
 * Note: A {@link LineageEventBuilder} instance is not reusable
 */

@Slf4j
public final class LineageEventBuilder extends GobblinEventBuilder {
  static final String LIENAGE_EVENT_NAMESPACE = getKey(NAMESPACE, "lineage");
  static final String SOURCE = "source";
  static final String DESTINATION = "destination";
  static final String LINEAGE_EVENT_TYPE = "LineageEvent";

  private static final Gson GSON = new Gson();

  @Getter @Setter
  private DatasetDescriptor source;
  @Getter @Setter
  private DatasetDescriptor destination;

  public LineageEventBuilder(String name) {
    super(name, LIENAGE_EVENT_NAMESPACE);
    addMetadata(EVENT_TYPE, LINEAGE_EVENT_TYPE);
  }

  @Override
  public GobblinTrackingEvent build() {
    source.toDataMap().forEach((key, value) -> metadata.put(getKey(SOURCE, key), value));
    destination.toDataMap().forEach((key, value) -> metadata.put(getKey(DESTINATION, key), value));
    return new GobblinTrackingEvent(0L, namespace, name, metadata);
  }

  @Override
  public String toString() {
    return GSON.toJson(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LineageEventBuilder event = (LineageEventBuilder) o;

    if (!namespace.equals(event.namespace) || !name.equals(event.name) || !metadata.equals(event.metadata)) {
      return false;
    }

    if (source != null ? !source.equals(event.source) : event.source != null) {
      return false;
    }

    return destination != null ? destination.equals(event.destination) : event.destination == null;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + namespace.hashCode();
    result = 31 * result + metadata.hashCode();
    result = 31 * result + (source != null ? source.hashCode() : 0);
    result = 31 * result + (destination != null ? destination.hashCode() : 0);
    return result;
  }

  /**
   * Check if the given {@link GobblinTrackingEvent} is a lineage event
   */
  public static boolean isLineageEvent(GobblinTrackingEvent event) {
    String eventType = event.getMetadata().get(EVENT_TYPE);
    return StringUtils.isNotEmpty(eventType) && eventType.equals(LINEAGE_EVENT_TYPE);
  }

  /**
   * Create a {@link LineageEventBuilder} from a {@link GobblinEventBuilder}. An inverse function
   * to {@link LineageEventBuilder#build()}
   */
  public static LineageEventBuilder fromEvent(GobblinTrackingEvent event) {
    Map<String, String> metadata = event.getMetadata();
    LineageEventBuilder lineageEvent = new LineageEventBuilder(event.getName());

    String sourcePrefix = getKey(SOURCE, "");
    Map<String, String> sourceDataMap = Maps.newHashMap();
    String destinationPrefix = getKey(DESTINATION, "");
    Map<String, String> destinationDataMap = Maps.newHashMap();

    metadata.forEach((key, value) -> {
      if (key.startsWith(sourcePrefix)) {
        sourceDataMap.put(key.substring(sourcePrefix.length()), value);
      } else if (key.startsWith(destinationPrefix)) {
        destinationDataMap.put(key.substring(destinationPrefix.length()), value);
      } else {
        lineageEvent.addMetadata(key, value);
      }
    });

    lineageEvent.setSource(DatasetDescriptor.fromDataMap(sourceDataMap));
    lineageEvent.setDestination(DatasetDescriptor.fromDataMap(destinationDataMap));
    return lineageEvent;
  }

  static String getKey(Object ... parts) {
    return Joiner.on(".").join(parts);
  }
}
