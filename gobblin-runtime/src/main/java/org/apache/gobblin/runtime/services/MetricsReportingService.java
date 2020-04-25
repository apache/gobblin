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

import org.apache.gobblin.metrics.EventReporterException;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricReporterException;


/**
 * A {@link com.google.common.util.concurrent.Service} for handling life cycle events around {@link GobblinMetrics}.
 */
@Slf4j
public class MetricsReportingService extends AbstractIdleService {

  private final Properties properties;
  private final String appId;

  public MetricsReportingService(Properties properties, String appId) {
    this.properties = properties;
    this.appId = appId;
  }

  @Override
  protected void startUp() throws Exception {
    try {
      GobblinMetrics.get(this.appId).startMetricReporting(this.properties);
    } catch (MetricReporterException e) {
      log.error("Failed to start {} metric reporter", e.getType().name(), e);
    } catch (EventReporterException e) {
      log.error("Failed to start {} event reporter", e.getType().name(), e);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    GobblinMetrics.get(this.appId).stopMetricsReporting();
  }
}
