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

package org.apache.gobblin.runtime.services;

import java.util.Properties;

import com.google.common.util.concurrent.AbstractIdleService;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MultiReporterException;
import org.apache.gobblin.metrics.reporter.util.MetricReportUtils;
import org.apache.gobblin.util.PropertiesUtils;


/**
 * A {@link com.google.common.util.concurrent.Service} for handling life cycle events around {@link GobblinMetrics}.
 */
@Slf4j
public class MetricsReportingService extends AbstractIdleService {
  public static final String METRICS_REPORTING_FAILURE_FATAL_KEY = "metrics.reporting.failure.fatal";
  public static final String EVENT_REPORTING_FAILURE_FATAL_KEY = "event.reporting.failure.fatal";

  public static final String DEFAULT_METRICS_REPORTING_FAILURE_FATAL = "false";
  public static final String DEFAULT_EVENT_REPORTING_FAILURE_FATAL = "false";

  private final Properties properties;
  private final String appId;
  private final boolean isMetricReportingFailureFatal;
  private final boolean isEventReportingFailureFatal;

  public MetricsReportingService(Properties properties, String appId) {
    this.properties = properties;
    this.appId = appId;
    this.isMetricReportingFailureFatal = PropertiesUtils.getPropAsBoolean(properties, METRICS_REPORTING_FAILURE_FATAL_KEY, DEFAULT_METRICS_REPORTING_FAILURE_FATAL);
    this.isEventReportingFailureFatal = PropertiesUtils.getPropAsBoolean(properties, EVENT_REPORTING_FAILURE_FATAL_KEY, DEFAULT_EVENT_REPORTING_FAILURE_FATAL);
  }

  @Override
  protected void startUp() throws Exception {
    try {
      GobblinMetrics.get(this.appId).startMetricReporting(this.properties);
    } catch (MultiReporterException ex) {
      if (MetricReportUtils.shouldThrowException(log, ex, this.isMetricReportingFailureFatal, this.isEventReportingFailureFatal)) {
        throw ex;
      }
    }
  }

  @Override
  protected void shutDown() throws Exception {
    GobblinMetrics.get(this.appId).stopMetricsReporting();
  }
}
