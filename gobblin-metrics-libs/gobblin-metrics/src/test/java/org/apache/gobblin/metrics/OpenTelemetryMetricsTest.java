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

package org.apache.gobblin.metrics;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;


public class OpenTelemetryMetricsTest  {

  @Test
  public void testInitializeOpenTelemetryFailsWithoutEndpoint() {
    State opentelemetryState = new State();
    opentelemetryState.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENABLED, "true");
    Assert.assertThrows(IllegalArgumentException.class, () -> {
      OpenTelemetryMetrics.getInstance(opentelemetryState);
    });
  }

  @Test
  public void testInitializeOpenTelemetrySucceedsWithEndpoint() {
    State opentelemetryState = new State();
    opentelemetryState.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENABLED, "true");
    opentelemetryState.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENDPOINT, "http://localhost:4317");
    // Should not throw an exception
    OpenTelemetryMetrics.getInstance(opentelemetryState);
    Assert.assertTrue(true);
  }

  @Test
  public void testHeadersParseCorrectly() {
    Map<String, String> headers = OpenTelemetryMetrics.parseHttpHeaders(
        "{\"Content-Type\":\"application/x-protobuf\",\"headerTag\":\"tag1:value1,tag2:value2\"}");
    Assert.assertEquals(headers.size(), 2);
    Assert.assertEquals(headers.get("Content-Type"), "application/x-protobuf");
    Assert.assertEquals(headers.get("headerTag"), "tag1:value1,tag2:value2");
  }

  @Test
  void testHeadersParseNull() {
    Map<String, String> headers = OpenTelemetryMetrics.parseHttpHeaders("{}");
    Assert.assertEquals(headers.size(), 0);
  }
}
