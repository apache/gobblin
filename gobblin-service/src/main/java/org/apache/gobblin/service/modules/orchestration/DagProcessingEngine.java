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

package org.apache.gobblin.service.modules.orchestration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.linkedin.r2.util.NamedThreadFactory;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Responsible for polling {@link DagTask}s from {@link DagManagement} and processing the
 * {@link org.apache.gobblin.service.modules.flowgraph.Dag} based on the type of {@link DagTask}.
 * Each {@link DagTask} acquires a lease for the {@link org.apache.gobblin.runtime.api.DagActionStore.DagAction}.
 * The {@link DagProcFactory} then provides the appropriate {@link DagProc} associated with the {@link DagTask}.
 * The actual work or processing is done by the {@link DagProc#process(DagManagementStateStore)}
 */

@Alpha
@Slf4j
@Singleton
public class DagProcessingEngine {
  public static final String NUM_THREADS_KEY = ConfigurationKeys.GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_PREFIX + "numThreads";
  private static final Integer DEFAULT_NUM_THREADS = 3;

  @Getter private final DagManagement dagManager;
  @Getter DagManagementStateStore dagManagementStateStore;

  @Inject
  public DagProcessingEngine(Config config, DagManagement dagManager, DagProcFactory dagProcFactory,
      DagManagementStateStore dagManagementStateStore, Optional<MultiActiveLeaseArbiter> multiActiveLeaseArbiter) {
    Integer numThreads = ConfigUtils.getInt(config, NUM_THREADS_KEY, DEFAULT_NUM_THREADS);
    ScheduledExecutorService scheduledExecutorPool =
        Executors.newScheduledThreadPool(numThreads, new NamedThreadFactory("DagProcessingEngineThread"));
    this.dagManager = dagManager;
    this.dagManagementStateStore = dagManagementStateStore;

    for (int i=0; i < numThreads; i++) {
      DagProcEngineThread dagProcEngineThread = new DagProcEngineThread(dagManager, dagProcFactory, dagManagementStateStore);
      scheduledExecutorPool.submit(dagProcEngineThread);
    }
  }

  @AllArgsConstructor
  private static class DagProcEngineThread implements Runnable {

    private DagManagement dagManager;
    private DagProcFactory dagProcFactory;
    private DagManagementStateStore dagManagementStateStore;

    @Override
    public void run() {
      while (true) {
        DagTask<DagProc> dagTask = dagManager.next(); // blocking call
        if (dagTask == null) {
          continue;
        }
        DagProc dagProc = dagTask.host(dagProcFactory);
        try {
          // todo - add retries
          dagProc.process(dagManagementStateStore);
        } catch (Throwable t) {
          log.error("DagProcEngineThread encountered error ", t);
        }
        // todo mark lease success and releases it
        //dagTaskStream.complete(dagTask);
      }
    }
  }
}
