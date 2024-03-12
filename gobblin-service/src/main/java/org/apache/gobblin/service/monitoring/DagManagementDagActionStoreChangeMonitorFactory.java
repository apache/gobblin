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
import javax.inject.Named;
import javax.inject.Provider;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.util.InjectionNames;
import org.apache.gobblin.service.modules.orchestration.DagManagement;
import org.apache.gobblin.service.modules.orchestration.DagManager;
import org.apache.gobblin.service.modules.orchestration.Orchestrator;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A factory implementation that returns a {@link DagManagementDagActionStoreChangeMonitor} instance.
 */
@Slf4j
public class DagManagementDagActionStoreChangeMonitorFactory implements Provider<DagActionStoreChangeMonitor> {
  static final String DAG_ACTION_STORE_CHANGE_MONITOR_NUM_THREADS_KEY = "numThreads";

  private final Config config;
  private final FlowCatalog flowCatalog;
  private final Orchestrator orchestrator;
  private final DagActionStore dagActionStore;
  private final boolean isMultiActiveSchedulerEnabled;
  private final DagManagement dagManagement;

  @Inject
  public DagManagementDagActionStoreChangeMonitorFactory(Config config, DagManager dagManager, FlowCatalog flowCatalog,
      Orchestrator orchestrator, DagActionStore dagActionStore, DagManagement dagManagement,
      @Named(InjectionNames.MULTI_ACTIVE_SCHEDULER_ENABLED) boolean isMultiActiveSchedulerEnabled) {
    this.config = Objects.requireNonNull(config);
    this.flowCatalog = flowCatalog;
    this.orchestrator = orchestrator;
    this.dagActionStore = dagActionStore;
    this.isMultiActiveSchedulerEnabled = isMultiActiveSchedulerEnabled;
    this.dagManagement = dagManagement;
  }

  private DagManagementDagActionStoreChangeMonitor createDagActionStoreMonitor()
    throws ReflectiveOperationException {
    Config dagActionStoreChangeConfig = this.config.getConfig(DagActionStoreChangeMonitor.DAG_ACTION_CHANGE_MONITOR_PREFIX);
    log.info("DagActionStore will be initialized with config {}", dagActionStoreChangeConfig);

    int numThreads = ConfigUtils.getInt(dagActionStoreChangeConfig, DAG_ACTION_STORE_CHANGE_MONITOR_NUM_THREADS_KEY, 5);

    return new DagManagementDagActionStoreChangeMonitor(dagActionStoreChangeConfig,
        numThreads, flowCatalog, orchestrator, dagActionStore, isMultiActiveSchedulerEnabled, this.dagManagement);
  }

  @Override
  public DagActionStoreChangeMonitor get() {
    try {
      DagActionStoreChangeMonitor changeMonitor = createDagActionStoreMonitor();
      changeMonitor.initializeMonitor();
      return changeMonitor;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to initialize DagActionStoreMonitor due to ", e);
    }
  }
}
