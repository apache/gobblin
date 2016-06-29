/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.services;

import java.util.Properties;

import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.metrics.GobblinMetrics;


/**
 * A {@link com.google.common.util.concurrent.Service} for handling life cycle events around {@link GobblinMetrics}.
 */
public class MetricsReportingService extends AbstractIdleService {

  private final Properties properties;
  private final String appId;

  public MetricsReportingService(Properties properties, String appId) {
    this.properties = properties;
    this.appId = appId;
  }

  @Override
  protected void startUp() throws Exception {
    GobblinMetrics.get(this.appId).startMetricReporting(this.properties);
  }

  @Override
  protected void shutDown() throws Exception {
    GobblinMetrics.get(this.appId).stopMetricsReporting();
  }
}
