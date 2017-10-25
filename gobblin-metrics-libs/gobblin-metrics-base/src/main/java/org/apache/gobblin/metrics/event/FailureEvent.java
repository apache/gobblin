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
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;

import com.google.common.collect.Maps;


/**
 * A failure event represents a specific {@link GobblinTrackingEvent} whose metadata has
 * {@value EventSubmitter#EVENT_TYPE} to be {@value #EVENT_TYPE}
 *
 * <p>
 * Note: A {@link FailureEvent} instance is not reusable
 */
public class FailureEvent {
  private static final String EVENT_TYPE = "FailureEvent";
  private static final String EVENT_NAMESPACE = "gobblin.event";
  private static final String ROOT_CAUSE = "rootException";

  private final EventSubmitter submitter;
  private final Map<String, String> metadata;

  public FailureEvent(MetricContext context) {
    submitter = new EventSubmitter.Builder(context, EVENT_NAMESPACE).build();
    metadata = Maps.newHashMap();
    metadata.put(EventSubmitter.EVENT_TYPE, EVENT_TYPE);
  }

  /**
   * Given an throwable, get its root cause and set to the metadata
   */
  public void setRootCause(Throwable t) {
    setMetadata(ROOT_CAUSE, getRootCause(t));
  }

  /**
   * Set a metadata pair
   */
  public void setMetadata(String key, String value) {
    metadata.put(key, value);
  }

  /**
   * Submit the event
   * @param name the name of the event
   * @param additionalMetadata additional meta data to be added to the event
   */
  public void submit(String name, Map<String, String> additionalMetadata) {
    metadata.putAll(additionalMetadata);
    submitter.submit(name, metadata);
  }

  /**
   * Check if the given {@link GobblinTrackingEvent} is a failiure event
   */
  public static boolean isFailureEvent(GobblinTrackingEvent event) {
    String eventType = event.getMetadata().get(EventSubmitter.EVENT_TYPE);
    return StringUtils.isNotEmpty(eventType) && eventType.equals(EVENT_TYPE);
  }

  private static String getRootCause(Throwable t) {
    Throwable rootCause = ExceptionUtils.getRootCause(t);
    if (rootCause == null) {
      rootCause = t;
    }
    return ExceptionUtils.getStackTrace(rootCause);
  }
}
