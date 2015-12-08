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

import gobblin.metrics.reporter.OutputStreamReporter;

import java.io.IOException;
import java.util.Properties;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;


/**
 * A reporter factory to report metrics to console.
 * <p>Set
 * metrics.reporting.custom.builders=gobblin.metrics.ConsoleReporterFactory to report event to console</p>
 */
public class ConsoleReporterFactory implements CustomReporterFactory {

  @Override
  public ScheduledReporter newScheduledReporter(MetricRegistry registry, Properties properties) throws IOException {
    return OutputStreamReporter.forRegistry(registry).build();
  }
}
