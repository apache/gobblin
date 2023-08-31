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

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Responsible for polling {@link DagTask}s from {@link DagTaskStream} and processing the {@link org.apache.gobblin.service.modules.flowgraph.Dag}
 * based on the type of {@link DagTask} which is determined by the {@link org.apache.gobblin.runtime.api.DagActionStore.DagAction}.
 * Each {@link DagTask} acquires a lease for the {@link org.apache.gobblin.runtime.api.DagActionStore.DagAction}.
 * The {@link DagProcFactory} then provides the appropriate {@link DagProc} associated with the {@link DagTask}.
 * The actual work or processing is done by the {@link DagProc#process(DagManagementStateStore, int, long)}
 */

@Alpha
@Slf4j
@Singleton
public class DagProcessingEngine {
  public static final String GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_PREFIX = ServiceConfigKeys.GOBBLIN_SERVICE_PREFIX + "dagProcessingEngine.";
  public static final String DAG_PROCESSING_ENGINE_ENABLED = GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_PREFIX + "enabled";
  public static final String NUM_THREADS_KEY = GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_PREFIX + "numThreads";
  private static final Integer DEFAULT_NUM_THREADS = 3;

  @Getter private final DagTaskStream dagTaskStream;
  Optional<EventSubmitter> eventSubmitter;
  @Getter DagManagementStateStore dagManagementStateStore;

  public DagProcessingEngine(Config config, DagTaskStream dagTaskStream, DagProcFactory dagProcFactory,
      DagManagementStateStore dagManagementStateStore, MultiActiveLeaseArbiter multiActiveLeaseArbiter) {
    MetricContext metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.eventSubmitter = Optional.of(new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build());
    Integer numThreads = ConfigUtils.getInt(config, NUM_THREADS_KEY, DEFAULT_NUM_THREADS);
    ScheduledExecutorService scheduledExecutorPool = Executors.newScheduledThreadPool(numThreads);
    this.dagTaskStream = dagTaskStream;
    this.dagManagementStateStore = dagManagementStateStore;

    for (int i=0; i < numThreads; i++) {
      DagProcEngineThread dagProcEngineThread = new DagProcEngineThread(dagTaskStream, dagProcFactory, this, dagManagementStateStore);
      scheduledExecutorPool.submit(dagProcEngineThread);
    }
  }

  public void addDagNodeToRetry(Dag.DagNode<JobExecutionPlan> dagNode) {
    // todo - how to add dag action for for a dag node? should we create a dag node action? right now dag action is synonymous to flow action
    // this.dagTaskStream.addDagTask(new RetryDagTask(dagNode));
  }

  @AllArgsConstructor
  private static class DagProcEngineThread implements Runnable {

    private DagTaskStream dagTaskStream;
    private DagProcFactory dagProcFactory;
    private DagProcessingEngine dagProcessingEngine;
    private DagManagementStateStore dagManagementStateStore;

    @Override
    public void run() {
      while (dagTaskStream.hasNext()) {
        DagTask dagTask = dagTaskStream.next();
        DagProc dagProc = dagTask.host(dagProcFactory, dagProcessingEngine);
//          dagProc.process(eventSubmitter.get(), maxRetryAttempts, delayRetryMillis);
        try {
          // todo - add retries
          dagProc.process(dagManagementStateStore);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        // todo mark lease success and releases it
        //dagTaskStream.complete(dagTask);
      }
    }
  }
}
