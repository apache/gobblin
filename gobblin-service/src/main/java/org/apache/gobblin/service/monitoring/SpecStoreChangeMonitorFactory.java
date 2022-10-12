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

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A factory implementation that returns a {@link SpecStoreChangeMonitor} instance.
 */
@Slf4j
public class SpecStoreChangeMonitorFactory implements Provider<SpecStoreChangeMonitor> {
  static final String SPEC_STORE_CHANGE_MONITOR_CLASS_NAME = "org.apache.gobblin.service.monitoring.SpecStoreChangeMonitor";
  static final String SPEC_STORE_CHANGE_MONITOR_NUM_THREADS_KEY = "numThreads";

  private final Config config;

  @Inject
  public SpecStoreChangeMonitorFactory(Config config) {
    this.config = Objects.requireNonNull(config);
  }

  private SpecStoreChangeMonitor createSpecStoreChangeMonitor()
      throws ReflectiveOperationException {
    Config specStoreChangeConfig = config.getConfig(SpecStoreChangeMonitor.SPEC_STORE_CHANGE_MONITOR_PREFIX);
    String topic = ""; // Pass empty string because we expect underlying client to dynamically determine the Kafka topic
    int numThreads = ConfigUtils.getInt(specStoreChangeConfig, SPEC_STORE_CHANGE_MONITOR_NUM_THREADS_KEY, 5);

    return (SpecStoreChangeMonitor) GobblinConstructorUtils.invokeConstructor(
        Class.forName(SPEC_STORE_CHANGE_MONITOR_CLASS_NAME), topic, specStoreChangeConfig, numThreads);
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
