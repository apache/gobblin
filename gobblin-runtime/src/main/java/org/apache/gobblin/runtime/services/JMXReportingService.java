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

package gobblin.runtime.services;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

import com.google.common.util.concurrent.AbstractIdleService;


/**
 * A {@link com.google.common.util.concurrent.Service} for collecting various JVM metrics and reporting them via JMX.
 *
 * <p>
 *   The class uses Codahale to collect the various JVM metrics which includes:
 *
 *   <ul>
 *     <li>GC activity using a {@link GarbageCollectorMetricSet}</li>
 *     <li>Memory usage using a {@link MemoryUsageGaugeSet}</li>
 *     <li>Thread usage and state using a {@link ThreadStatesGaugeSet}</li>
 *     <li>Used file descriptors using a {@link FileDescriptorRatioGauge}</li>
 *   </ul>
 *
 *   All metrics are collected via a {@link JmxReporter}.
 * </p>
 */
public class JMXReportingService extends AbstractIdleService {

  private final MetricRegistry metricRegistry = new MetricRegistry();
  private final JmxReporter jmxReporter = JmxReporter.forRegistry(this.metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build();

  @Override
  protected void startUp() throws Exception {
    registerJvmMetrics();
    this.jmxReporter.start();
  }

  @Override
  protected void shutDown() throws Exception {
    this.jmxReporter.stop();
  }

  private void registerJvmMetrics() {
    registerMetricSetWithPrefix("jvm.gc", new GarbageCollectorMetricSet());
    registerMetricSetWithPrefix("jvm.memory", new MemoryUsageGaugeSet());
    registerMetricSetWithPrefix("jvm.threads", new ThreadStatesGaugeSet());
    this.metricRegistry.register("jvm.fileDescriptorRatio", new FileDescriptorRatioGauge());
  }

  private void registerMetricSetWithPrefix(String prefix, MetricSet metricSet) {
    for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      this.metricRegistry.register(MetricRegistry.name(prefix, entry.getKey()), entry.getValue());
    }
  }
}
