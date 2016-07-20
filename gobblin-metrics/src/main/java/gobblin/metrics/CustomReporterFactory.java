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

package gobblin.metrics;

import java.io.IOException;
import java.util.Properties;

import gobblin.metrics.reporter.ScheduledReporter;


/**
 * BuilderFactory for custom {@link ScheduledReporter}. Implementations should have a parameter-less constructor.
 */
public interface CustomReporterFactory {

  /**
   * Builds and returns a new {@link com.codahale.metrics.ScheduledReporter}.
   *
   * @param properties {@link Properties} used to build the reporter.
   * @return new {@link ScheduledReporter}.
   */
  public ScheduledReporter newScheduledReporter(Properties properties) throws IOException;
}
