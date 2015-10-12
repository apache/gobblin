/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics;

import java.io.IOException;
import java.util.Properties;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;


/**
 * Factory for custom {@link com.codahale.metrics.ScheduledReporter}.
 *
 * Implementations should have a parameter-less constructor.
 */
public interface CustomReporterFactory {
  /**
   * Builds and returns a new {@link com.codahale.metrics.ScheduledReporter}.
   * @param registry {@link com.codahale.metrics.MetricRegistry} for which metrics should be reported.
   * @param properties {@link java.util.Properties} used to build the reporter.
   * @return new {@link com.codahale.metrics.ScheduledReporter}.
   */
  public ScheduledReporter newScheduledReporter(MetricRegistry registry, Properties properties) throws IOException;
}
