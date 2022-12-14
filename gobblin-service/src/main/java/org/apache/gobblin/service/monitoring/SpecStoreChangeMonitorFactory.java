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

package org.apache.gobblin.service.monitoring;

import java.util.Objects;

import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.inject.Provider;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A factory implementation that returns a {@link SpecStoreChangeMonitor} instance.
 */
@Slf4j
public class SpecStoreChangeMonitorFactory implements Provider<SpecStoreChangeMonitor> {
  static final String SPEC_STORE_CHANGE_MONITOR_NUM_THREADS_KEY = "numThreads";

  private final Config config;
  private FlowCatalog flowCatalog;
  private GobblinServiceJobScheduler scheduler;

  @Inject
  public SpecStoreChangeMonitorFactory(Config config,FlowCatalog flowCatalog, GobblinServiceJobScheduler scheduler) {
    this.config = Objects.requireNonNull(config);
    this.flowCatalog = flowCatalog;
    this.scheduler = scheduler;
  }

  private SpecStoreChangeMonitor createSpecStoreChangeMonitor()
      throws ReflectiveOperationException {
    Config specStoreChangeConfig = this.config.getConfig(SpecStoreChangeMonitor.SPEC_STORE_CHANGE_MONITOR_PREFIX);
    log.info("SpecStoreChangeMonitor will be initialized with config {}", specStoreChangeConfig);

    String topic = ""; // Pass empty string because we expect underlying client to dynamically determine the Kafka topic
    int numThreads = ConfigUtils.getInt(specStoreChangeConfig, SPEC_STORE_CHANGE_MONITOR_NUM_THREADS_KEY, 5);

    return new SpecStoreChangeMonitor(topic, specStoreChangeConfig, this.flowCatalog, this.scheduler, numThreads);
  }

  @Override
  public SpecStoreChangeMonitor get() {
    try {
      return createSpecStoreChangeMonitor();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to initialize SpecStoreChangeMonitor due to ", e);
    }
  }
}
