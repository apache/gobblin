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


import io.micrometer.registry.otlp.OtlpConfig;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;


/** Test {@link TemporalMetricsHelper} */
public class TemporalMetricsHelperTest {

  private Config config;

  @BeforeClass
  public void setup() {
    config = ConfigFactory.empty()
        .withValue("prefix", ConfigValueFactory.fromAnyRef("gobblin.temporal.metrics.otlp"))
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_METRICS_OTLP_PREFIX_WITHOUT_DOT + ".headers",
            ConfigValueFactory.fromAnyRef("{\"abc\":\"123\", \"pqr\":\"456\"}"))
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_METRICS_OTLP_PREFIX_WITHOUT_DOT + ".resourceAttributes",
            ConfigValueFactory.fromAnyRef("service.name=gobblin-service"))
        .withValue("dim1", ConfigValueFactory.fromAnyRef("val1"))
        .withValue("dim2", ConfigValueFactory.fromAnyRef("val2"))
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_METRICS_OTLP_PREFIX_WITHOUT_DOT + ".dimensions",
            ConfigValueFactory.fromAnyRef("dim1,dim2,missingDimension"));
  }

  @Test
  public void testGetDimensions() {
    Map<String, String> dimensions = TemporalMetricsHelper.getDimensions(config);

    Assert.assertNotNull(dimensions);
    Assert.assertEquals(3, dimensions.size());
    Assert.assertEquals("val1", dimensions.get("dim1"));
    Assert.assertEquals("val2", dimensions.get("dim2"));
    Assert.assertEquals("", dimensions.get("missingDimension"));
  }

  @Test
  public void testGetDimensionsEmptyConfig() {
    Map<String, String> dimensions = TemporalMetricsHelper.getDimensions(ConfigFactory.empty());

    Assert.assertNotNull(dimensions);
    Assert.assertEquals(0, dimensions.size());
  }

  @Test
  public void testGetOtlpConfig() {
    OtlpConfig otlpConfig = TemporalMetricsHelper.getOtlpConfig(config);

    Map<String, String> headers = otlpConfig.headers();
    Assert.assertNotNull(headers);
    Assert.assertEquals(2, headers.size());
    Assert.assertEquals("123", headers.get("abc"));
    Assert.assertEquals("456", headers.get("pqr"));

    Assert.assertEquals("gobblin-service", otlpConfig.resourceAttributes().get("service.name"));
  }
}
