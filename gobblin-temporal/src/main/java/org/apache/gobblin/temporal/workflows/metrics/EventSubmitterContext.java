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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;

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

  @AllArgsConstructor
  @NoArgsConstructor
  public static class Builder {
    private List<Tag<?>> tags = new ArrayList<>();
    private String namespace;
    public Builder(EventSubmitter eventSubmitter) {
      this.tags.addAll(eventSubmitter.getTags());
      this.namespace = eventSubmitter.getNamespace();
    }

    public Builder addTag(Tag<?> tag) {
      this.tags.add(tag);
      return this;
    }

    public Builder addTags(List<Tag<?>> tags) {
      this.tags.addAll(tags);
      return this;
    }

    public Builder setNamespace(String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder withGaaSJobProps(Properties jobProps) {
      // TODO: Add temporal specific metadata tags

      if (jobProps.containsKey(ConfigurationKeys.FLOW_GROUP_KEY)) {
        this.tags.add(new Tag<>(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, jobProps.getProperty(ConfigurationKeys.FLOW_GROUP_KEY)));
        this.tags.add(new Tag<>(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, jobProps.getProperty(ConfigurationKeys.FLOW_NAME_KEY)));
        this.tags.add(new Tag<>(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, jobProps.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)));
      }

      if (jobProps.containsKey(ConfigurationKeys.JOB_CURRENT_ATTEMPTS)) {
        this.tags.add(new Tag<>(TimingEvent.FlowEventConstants.CURRENT_ATTEMPTS_FIELD,
            jobProps.getProperty(ConfigurationKeys.JOB_CURRENT_ATTEMPTS, "1")));
        this.tags.add(new Tag<>(TimingEvent.FlowEventConstants.CURRENT_GENERATION_FIELD,
            jobProps.getProperty(ConfigurationKeys.JOB_CURRENT_GENERATION, "1")));
        this.tags.add(new Tag<>(TimingEvent.FlowEventConstants.SHOULD_RETRY_FIELD,
            "false"));
      }

      //Use azkaban.flow.execid as the jobExecutionId
      this.tags.add(new Tag<>(TimingEvent.FlowEventConstants.JOB_EXECUTION_ID_FIELD, "0"));

      this.tags.add(new Tag<>(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD,
          jobProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY, "")));
      this.tags.add(new Tag<>(TimingEvent.FlowEventConstants.JOB_NAME_FIELD,
          jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY, "")));

      this.tags.add(new Tag<>(Help.USER_TO_PROXY_KEY, jobProps.getProperty(Help.USER_TO_PROXY_KEY, "")));
      return this;
    }

    public Builder withEventSubmitter(EventSubmitter eventSubmitter) {
      this.tags.addAll(eventSubmitter.getTags());
      this.namespace = eventSubmitter.getNamespace();
      return this;
    }

    public EventSubmitterContext build() {
      return new EventSubmitterContext(ImmutableList.copyOf(tags), namespace, tags.stream()
          .filter(tag -> tag.getKey().equals(CLASS_META))
          .findAny()
          .map(tag -> (String) tag.getValue())
          .map(EventSubmitterContext::resolveClass)
          .orElse(EventSubmitterContext.class));
    }
  }
}
