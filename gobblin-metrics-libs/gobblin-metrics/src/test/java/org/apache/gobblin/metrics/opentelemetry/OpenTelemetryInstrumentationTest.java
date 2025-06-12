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

import java.lang.reflect.Field;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.service.ServiceConfigKeys;


/**
 * Unit tests for {@link OpenTelemetryInstrumentation}.
 * These tests ensure that the singleton instance is created correctly,
 * common attributes are built from dimensions, and metrics are created and cached properly.
 */
public class OpenTelemetryInstrumentationTest {

  private OpenTelemetryInstrumentation instrumentation;
  private State state;

  @BeforeMethod
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    // Reset singleton instance before each test
    Field instanceField = OpenTelemetryInstrumentation.class.getDeclaredField("GLOBAL_INSTANCE");
    instanceField.setAccessible(true);
    instanceField.set(null, null);

    state = new State();
    state.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_CLASSNAME, "org.apache.gobblin.metrics.InMemoryOpenTelemetryMetrics");
  }

  @Test
  public void singletonInstanceCreatedOnce() {
    OpenTelemetryInstrumentation instance1 = OpenTelemetryInstrumentation.getInstance(state);
    OpenTelemetryInstrumentation instance2 = OpenTelemetryInstrumentation.getInstance(state);

    Assert.assertSame(instance1, instance2, "getInstance should return the same instance");
  }

  @Test
  public void emptyDimensionsCreateEmptyAttributes() {
    state.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_DIMENSIONS, "");

    instrumentation = OpenTelemetryInstrumentation.getInstance(state);
    Attributes attributes = instrumentation.getCommonAttributes();

    Assert.assertEquals(attributes.size(), 0);
  }

  @Test
  public void commonAttributesBuiltFromDimensions() {
    state.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_DIMENSIONS, "dim1,dim2");
    state.setProp("dim1", "value1");
    state.setProp("dim2", "value2");

    instrumentation = OpenTelemetryInstrumentation.getInstance(state);
    Attributes attributes = instrumentation.getCommonAttributes();

    Assert.assertEquals(attributes.get(AttributeKey.stringKey("dim1")), "value1");
    Assert.assertEquals(attributes.get(AttributeKey.stringKey("dim2")), "value2");
  }

  @Test
  public void flowEdgeIdParsedCorrectly() {
    state.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_DIMENSIONS,
        ConfigurationKeys.FLOW_EDGE_ID_KEY);
    state.setProp(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, "destNode");
    state.setProp(ConfigurationKeys.FLOW_EDGE_ID_KEY, "sourceNode_destNode_edgeId");

    instrumentation = OpenTelemetryInstrumentation.getInstance(state);
    Attributes attributes = instrumentation.getCommonAttributes();

    Assert.assertEquals(attributes.get(AttributeKey.stringKey(ConfigurationKeys.FLOW_EDGE_ID_KEY)),
        "edgeId");
  }

  @Test
  public void metricsAreCreatedAndCached() {
    instrumentation = OpenTelemetryInstrumentation.getInstance(state);
    Assert.assertEquals(instrumentation.getMetrics().size(), 0, "Metrics map should be empty initially");
    instrumentation.getOrCreate(GaaSOpenTelemetryMetrics.GAAS_JOB_STATUS);
    Assert.assertEquals(instrumentation.getMetrics().size(), 1, "Metrics map should contain one metric after creation");
    instrumentation.getOrCreate(GaaSOpenTelemetryMetrics.GAAS_JOB_STATUS);
    Assert.assertEquals(instrumentation.getMetrics().size(), 1, "Metrics map should still contain one metric after duplicate creation");
    instrumentation.getOrCreate(GaaSOpenTelemetryMetrics.GAAS_JOB_STATE_LATENCY);
    Assert.assertEquals(instrumentation.getMetrics().size(), 2, "Metrics map should contain two metrics after creating another");
  }

}
