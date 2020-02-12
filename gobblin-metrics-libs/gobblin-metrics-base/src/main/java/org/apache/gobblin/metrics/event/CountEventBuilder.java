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

import org.apache.commons.lang.StringUtils;

import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.metrics.GobblinTrackingEvent;

/**
 * The builder builds builds a specific {@link GobblinTrackingEvent} whose metadata has
 * {@value GobblinEventBuilder#EVENT_TYPE} to be {@value #COUNT_EVENT_TYPE}
 *
 * <p>
 * Note: A {@link CountEventBuilder} instance is not reusable
 */
public class CountEventBuilder extends GobblinEventBuilder {

  public static final String COUNT_EVENT_TYPE = "CountEvent";
  public static final String COUNT_KEY = "count";
  @Setter
  @Getter
  private long count;

  public CountEventBuilder(String name, long count) {
    this(name, null, count);
  }

  public CountEventBuilder(String name, String namespace, long count) {
    super(name, namespace);
    this.metadata.put(EVENT_TYPE, COUNT_EVENT_TYPE);
    this.count = count;
  }

  /**
   *
   * @return {@link GobblinTrackingEvent}
   */
  @Override
  public GobblinTrackingEvent build() {
    this.metadata.put(COUNT_KEY, Long.toString(count));
    return super.build();
  }

  /**
   * Check if the given {@link GobblinTrackingEvent} is a {@link CountEventBuilder}
   */
  public static boolean isCountEvent(GobblinTrackingEvent event) {
    String eventType = (event.getMetadata() == null) ? "" : event.getMetadata().get(EVENT_TYPE);
    return StringUtils.isNotEmpty(eventType) && eventType.equals(COUNT_EVENT_TYPE);
  }
  /**
   * Create a {@link CountEventBuilder} from a {@link GobblinTrackingEvent}. An inverse function
   * to {@link CountEventBuilder#build()}
   */
  public static CountEventBuilder fromEvent(GobblinTrackingEvent event) {
    if(!isCountEvent(event)) {
      return null;
    }

    Map<String, String> metadata = event.getMetadata();
    long count = Long.parseLong(metadata.getOrDefault(COUNT_KEY, "0"));
    CountEventBuilder countEventBuilder = new CountEventBuilder(event.getName(), event.getNamespace(), count);
    metadata.forEach((key, value) -> {
      switch (key) {
        case COUNT_KEY:
          break;
        default:
          countEventBuilder.addMetadata(key, value);
          break;
      }
    });

    return countEventBuilder;
  }
}
