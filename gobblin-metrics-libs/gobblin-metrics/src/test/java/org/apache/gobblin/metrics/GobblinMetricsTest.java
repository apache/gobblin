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

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;


@Test
public class GobblinMetricsTest {
  /**
   * Test the {@link GobblinMetrics} instance is removed from {@link GobblinMetricsRegistry} when
   * it stops metrics reporting
   */
  public void testStopReportingMetrics()
      throws MultiReporterException {
    String id = getClass().getSimpleName() + "-" + System.currentTimeMillis();
    GobblinMetrics gobblinMetrics = GobblinMetrics.get(id);

    Properties properties = new Properties();
    properties.put(ConfigurationKeys.FAILURE_REPORTING_FILE_ENABLED_KEY, "false");

    gobblinMetrics.startMetricReporting(properties);
    Assert.assertEquals(GobblinMetricsRegistry.getInstance().get(id).get(), gobblinMetrics);

    gobblinMetrics.stopMetricsReporting();
    Assert.assertFalse(GobblinMetricsRegistry.getInstance().get(id).isPresent());
  }

  public void testMetricFileReporterThrowsException() {
    String id = getClass().getSimpleName() + "-" + System.currentTimeMillis();
    GobblinMetrics gobblinMetrics = GobblinMetrics.get(id);

    //Enable file reporter without specifying metrics.log.dir.
    Config config = ConfigFactory.empty()
        .withValue(ConfigurationKeys.METRICS_REPORTING_FILE_ENABLED_KEY, ConfigValueFactory.fromAnyRef(true));

    Properties properties = ConfigUtils.configToProperties(config);
    //Ensure MultiReporterException is thrown
    try {
      gobblinMetrics.startMetricReporting(properties);
      Assert.fail("Metric reporting unexpectedly succeeded.");
    } catch (MultiReporterException e) {
      //Do nothing. Expected to be here.
    }
  }

  public void testMetricFileReporterSuccessful() {
    String id = getClass().getSimpleName() + "-" + System.currentTimeMillis();
    GobblinMetrics gobblinMetrics = GobblinMetrics.get(id);

    //Enable file reporter without specifying metrics.log.dir.
    Config config = ConfigFactory.empty()
        .withValue(ConfigurationKeys.METRICS_REPORTING_FILE_ENABLED_KEY, ConfigValueFactory.fromAnyRef(true))
        .withValue(ConfigurationKeys.METRICS_LOG_DIR_KEY, ConfigValueFactory.fromAnyRef("/tmp"))
        .withValue(ConfigurationKeys.FAILURE_LOG_DIR_KEY, ConfigValueFactory.fromAnyRef("/tmp"));

    Properties properties = ConfigUtils.configToProperties(config);
    //Ensure MultiReporterException is thrown
    try {
      gobblinMetrics.startMetricReporting(properties);
    } catch (MultiReporterException e) {
      Assert.fail("Unexpected exception " + e.getMessage());
    }
  }

}
