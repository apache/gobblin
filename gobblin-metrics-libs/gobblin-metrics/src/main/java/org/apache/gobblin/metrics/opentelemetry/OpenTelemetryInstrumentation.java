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

package org.apache.gobblin.metrics.opentelemetry;

import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import com.google.common.base.Splitter;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.InMemoryOpenTelemetryMetrics;
import org.apache.gobblin.metrics.OpenTelemetryMetricsBase;
import org.apache.gobblin.service.ServiceConfigKeys;


/**
 * Provides OpenTelemetry instrumentation for metrics.
 *
 * <p>Maintains a singleton instance that holds common attributes {@link Attributes} and a Meter {@link Meter}.
 * Exposes methods to retrieve or create metric instruments defined in {@link GobblinOpenTelemetryMetrics}.
 */
@Slf4j
@Getter
public class OpenTelemetryInstrumentation {

  // Adding the gobblin-service.main (BaseFlowGraphHelper.FLOW_EDGE_LABEL_JOINER_CHAR) dependency is creating circular dependency issues
  private static final String FLOW_EDGE_LABEL_JOINER_CHAR = "_";
  private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
  private static volatile OpenTelemetryInstrumentation GLOBAL_INSTANCE;

  private final Attributes commonAttributes;
  private final Meter meter;
  private final ConcurrentHashMap<String, OpenTelemetryMetric> metrics;

  private OpenTelemetryInstrumentation(final State state) {
    this.commonAttributes = buildCommonAttributes(state);
    this.meter = getOpenTelemetryMetrics(state).getMeter(state.getProp(
        ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_GROUP_NAME,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_OPENTELEMETRY_GROUP_NAME));
    this.metrics = new ConcurrentHashMap<>();
  }

  private OpenTelemetryMetricsBase getOpenTelemetryMetrics(State state) {
    try {
      String openTelemetryClassName = state.getProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_CLASSNAME,
          ConfigurationKeys.DEFAULT_METRICS_REPORTING_OPENTELEMETRY_CLASSNAME);
      Class<?> metricsClass = Class.forName(openTelemetryClassName);
      Method getInstanceMethod = metricsClass.getMethod("getInstance", State.class);
      return (OpenTelemetryMetricsBase) getInstanceMethod.invoke(null, state);
    } catch (Exception e) {
      log.error("Failed to initialize OpenTelemetryMetrics through reflection, defaulting to direct instantiation of InMemoryOpenTelemetryMetrics", e);
    }
    return InMemoryOpenTelemetryMetrics.getInstance(state);
  }

  /**
   * Returns the singleton instance for the given configuration state.
   *
   * @param state the configuration containing metric reporting and dimension configs
   * @return the global {@link OpenTelemetryInstrumentation} instance
   */
  public static OpenTelemetryInstrumentation getInstance(final State state) {
    if (GLOBAL_INSTANCE == null) {
      synchronized (OpenTelemetryInstrumentation.class) {
        if (GLOBAL_INSTANCE == null) {
          log.info("Creating OpenTelemetryInstrumentation instance");
          GLOBAL_INSTANCE = new OpenTelemetryInstrumentation(state);
        }
      }
    }
    return GLOBAL_INSTANCE;
  }

  public static OpenTelemetryInstrumentation getInstance(final Properties props) {
    return getInstance(new State(props));
  }

  /**
   * Retrieves an existing metric by its enum definition or creates it if absent.
   *
   * @param metric the {@link GobblinOpenTelemetryMetrics} enum defining name, description, unit, and type {@link OpenTelemetryMetricType}
   * @return an {@link OpenTelemetryMetric} instance corresponding to the provided enum
   */
  public OpenTelemetryMetric getOrCreate(GobblinOpenTelemetryMetrics metric) {
    return this.metrics.computeIfAbsent(metric.getMetricName(), name -> createMetric(metric));
  }

  private OpenTelemetryMetric createMetric(GobblinOpenTelemetryMetrics metric) {
    String name = metric.getMetricName();
    String description = metric.getMetricDescription();
    String unit = metric.getMetricUnit();
    Attributes attrs = this.commonAttributes;

    switch (metric.getMetricType()) {
      case LONG_COUNTER:
        return new OpenTelemetryLongCounter(
            name,
            attrs,
            this.meter.counterBuilder(name)
                .setDescription(description)
                .setUnit(unit)
                .build()
        );
      case DOUBLE_HISTOGRAM:
        return new OpenTelemetryDoubleHistogram(
            name,
            attrs,
            this.meter.histogramBuilder(name)
                .setDescription(metric.getMetricDescription())
                .setUnit(metric.getMetricUnit())
                .build()
        );
      default:
        throw new IllegalArgumentException("Unsupported metric type: " + metric.getMetricType());
    }
  }

  private Attributes buildCommonAttributes(final State state) {
    AttributesBuilder attributesBuilder = Attributes.builder();
    String commonDimensions = state.getProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_DIMENSIONS, "");
    if (StringUtils.isNotEmpty(commonDimensions)) {
      for (String dimension : COMMA_SPLITTER.split(commonDimensions)) {
        String dimensionKey = dimension.trim();
        String dimensionValue = state.getProp(dimensionKey, "");
        if (ConfigurationKeys.FLOW_EDGE_ID_KEY.equals(dimensionKey)) {
          dimensionValue = getFlowEdgeId(state, dimensionValue);
        }
        if (StringUtils.isNotEmpty(dimensionValue)) {
          attributesBuilder.put(dimensionKey, OpenTelemetryHelper.getOrDefaultOpenTelemetryAttrValue(dimensionValue));
        }
      }
    }
    return attributesBuilder.build();
  }

  private static String getFlowEdgeId(final State state, String fullFlowEdgeId) {
    // Parse the flowEdgeId from fullFlowEdgeId that is stored in format sourceNode_destinationNode_flowEdgeId
    return StringUtils.substringAfter(
        StringUtils.substringAfter(fullFlowEdgeId, state.getProp(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, "")),
        FLOW_EDGE_LABEL_JOINER_CHAR);
  }

}
