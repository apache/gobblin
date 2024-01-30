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

package org.apache.gobblin.temporal.workflows.metrics;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;

import static org.apache.gobblin.instrumented.GobblinMetricsKeys.CLASS_META;


/**
 * Wrapper for sending the core essence of an {@link EventSubmitter} over the wire (e.g. metadata tags, namespace)
 * This is in lieu of sending the entire {@link EventSubmitter} object over the wire, which is not serializable without
 * losing some information, such as the gauges
 */
@Getter
public class EventSubmitterContext {
  private final List<Tag<?>> tags;
  private final String namespace;
  private final Class callerClass;

  @JsonCreator
  private EventSubmitterContext(
      @JsonProperty("tags") List<Tag<?>> tags,
      @JsonProperty("namespace") String namespace,
      @JsonProperty("callerClass") Class callerClass) {
    this.tags = tags;
    this.namespace = namespace;
    this.callerClass = callerClass;
  }

  public EventSubmitterContext(List<Tag<?>> tags, String namespace) {
    // Explicitly send class over the wire to avoid any classloader issues
    this(tags, namespace, tags.stream()
        .filter(tag -> tag.getKey().equals(CLASS_META))
        .findAny()
        .map(tag -> (String) tag.getValue())
        .map(EventSubmitterContext::resolveClass)
        .orElse(EventSubmitterContext.class));
  }

  public EventSubmitterContext(EventSubmitter eventSubmitter) {
    this(eventSubmitter.getTags(), eventSubmitter.getNamespace());
  }

  public EventSubmitter create() {
    MetricContext metricContext = Instrumented.getMetricContext(new State(), callerClass, tags);
    return new EventSubmitter.Builder(metricContext, namespace).build();
  }

  private static Class resolveClass(String canonicalClassName) {
    try {
      return Class.forName(canonicalClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

  }
}
