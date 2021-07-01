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
import org.apache.commons.lang.exception.ExceptionUtils;

import lombok.Getter;

import org.apache.gobblin.metrics.GobblinTrackingEvent;


/**
 * The builder builds builds a specific {@link GobblinTrackingEvent} whose metadata has
 * {@value GobblinEventBuilder#EVENT_TYPE} to be {@value #FAILURE_EVENT_TYPE}
 *
 * <p>
 * Note: A {@link FailureEventBuilder} instance is not reusable
 */
public class FailureEventBuilder extends GobblinEventBuilder {
  private static final String FAILURE_EVENT_TYPE = "FailureEvent";
  private static final String ROOT_CAUSE = "rootException";

  @Getter
  private String rootCause;

  public FailureEventBuilder(String name) {
    this(name, NAMESPACE);
  }

  public FailureEventBuilder(String name, String namespace) {
    super(name, namespace);
    metadata.put(EVENT_TYPE, FAILURE_EVENT_TYPE);
  }

  /**
   * Given an throwable, get its root cause and set as a metadata
   */
  public void setRootCause(Throwable t) {
    Throwable rootCause = getRootCause(t);
    if(rootCause != null) {
      this.rootCause = ExceptionUtils.getStackTrace(rootCause);
    }
  }

  /**
   * Build as {@link GobblinTrackingEvent}
   */
  public GobblinTrackingEvent build() {
    if (this.rootCause != null) {
      metadata.put(ROOT_CAUSE, this.rootCause);
    }
    return new GobblinTrackingEvent(0L, namespace, name, metadata);
  }

  /**
   * Check if the given {@link GobblinTrackingEvent} is a failure event
   */
  public static boolean isFailureEvent(GobblinTrackingEvent event) {
    String eventType = (event.getMetadata() == null) ? "" : event.getMetadata().get(EVENT_TYPE);
    return StringUtils.isNotEmpty(eventType) && eventType.equals(FAILURE_EVENT_TYPE);
  }

  private static Throwable getRootCause(Throwable t) {
    Throwable rootCause = ExceptionUtils.getRootCause(t);
    if (rootCause == null) {
      rootCause = t;
    }
    return rootCause;
  }

  /**
   * Create a {@link FailureEventBuilder} from a {@link GobblinTrackingEvent}. An inverse function
   * to {@link FailureEventBuilder#build()}
   */
  public static FailureEventBuilder fromEvent(GobblinTrackingEvent event) {
    if(!isFailureEvent(event)) {
      return null;
    }

    Map<String, String> metadata = event.getMetadata();
    FailureEventBuilder failureEvent = new FailureEventBuilder(event.getName(), event.getNamespace());

    metadata.forEach((key, value) -> {
      switch (key) {
        case ROOT_CAUSE:
          failureEvent.rootCause = value;
          break;
        default:
          failureEvent.addMetadata(key, value);
          break;
      }
    });

    return failureEvent;
  }
}
