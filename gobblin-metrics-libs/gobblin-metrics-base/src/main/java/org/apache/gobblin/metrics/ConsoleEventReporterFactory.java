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
package gobblin.metrics;

import java.io.IOException;
import java.util.Properties;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

import gobblin.metrics.reporter.OutputStreamEventReporter;


/**
 * A reporter factory to report event to console.
 *
 * <p>
 *   Set metrics.reporting.custom.builders=gobblin.metrics.ConsoleEventReporterFactory to report event to console
 * </p>
 */
public class ConsoleEventReporterFactory implements CustomCodahaleReporterFactory {

  @Override
  public ScheduledReporter newScheduledReporter(MetricRegistry registry, Properties properties) throws IOException {
    try {
      return OutputStreamEventReporter.forContext(MetricContext.class.cast(registry)).build();
    } catch (ClassCastException cce) {
      throw new IOException(cce);
    }
  }
}
