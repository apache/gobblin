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

import org.apache.gobblin.service.modules.orchestration.DagActionReminderScheduler;
import org.apache.gobblin.service.modules.orchestration.DagManagement;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A factory implementation that returns a {@link DagManagementDagActionStoreChangeMonitor} instance.
 */
@Slf4j
public class DagManagementDagActionStoreChangeMonitorFactory implements Provider<DagManagementDagActionStoreChangeMonitor> {
  static final String DAG_ACTION_STORE_CHANGE_MONITOR_NUM_THREADS_KEY = "numThreads";

  private final Config config;
  private final DagManagementStateStore dagManagementStateStore;
  private final DagManagement dagManagement;
  private final DagActionReminderScheduler dagActionReminderScheduler;
  private final DagProcessingEngineMetrics dagProcEngineMetrics;

  @Inject
  public DagManagementDagActionStoreChangeMonitorFactory(Config config, DagManagementStateStore dagManagementStateStore,
      DagManagement dagManagement, DagActionReminderScheduler dagActionReminderScheduler, DagProcessingEngineMetrics dagProcEngineMetrics) {
    this.config = Objects.requireNonNull(config);
    this.dagManagementStateStore = dagManagementStateStore;
    this.dagManagement = dagManagement;
    this.dagActionReminderScheduler = dagActionReminderScheduler;
    this.dagProcEngineMetrics = dagProcEngineMetrics;
  }

  private DagManagementDagActionStoreChangeMonitor createDagActionStoreMonitor() {
    Config dagActionStoreChangeConfig = this.config.getConfig(DagManagementDagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX);
    log.info("DagManagementDagActionStoreChangeMonitor will be initialized with config {}", dagActionStoreChangeConfig);

    int numThreads = ConfigUtils.getInt(dagActionStoreChangeConfig, DAG_ACTION_STORE_CHANGE_MONITOR_NUM_THREADS_KEY, 5);

    return new DagManagementDagActionStoreChangeMonitor(dagActionStoreChangeConfig, numThreads, dagManagementStateStore,
        dagManagement, dagActionReminderScheduler, dagProcEngineMetrics);
  }

  @Override
  public DagManagementDagActionStoreChangeMonitor get() {
    return createDagActionStoreMonitor();
  }
}
