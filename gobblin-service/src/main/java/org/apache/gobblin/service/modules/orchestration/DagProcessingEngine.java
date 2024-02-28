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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.linkedin.r2.util.NamedThreadFactory;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Responsible for polling {@link DagTask}s from {@link DagTaskStream} and processing the
 * {@link org.apache.gobblin.service.modules.flowgraph.Dag} based on the type of {@link DagTask}.
 * Each {@link DagTask} returned from the {@link DagTaskStream} comes with a time-limited lease conferring the exclusive
 * right to perform the work of the task.
 * The {@link DagProcFactory} transforms each {@link DagTask} into a specific, concrete {@link DagProc}, which
 * encapsulates all processing inside {@link DagProc#process(DagManagementStateStore)}
 */

@Alpha
@Slf4j
@Singleton
public class DagProcessingEngine {

  @Getter private final DagTaskStream dagTaskStream;
  @Getter DagManagementStateStore dagManagementStateStore;

  @Inject
  public DagProcessingEngine(Config config, DagTaskStream dagTaskStream, DagProcFactory dagProcFactory,
      DagManagementStateStore dagManagementStateStore) {
    Integer numThreads = ConfigUtils.getInt
        (config, ServiceConfigKeys.NUM_DAG_PROC_THREADS_KEY, ServiceConfigKeys.DEFAULT_NUM_DAG_PROC_THREADS);
    ScheduledExecutorService scheduledExecutorPool =
        Executors.newScheduledThreadPool(numThreads, new NamedThreadFactory("DagProcessingEngineThread"));
    this.dagTaskStream = dagTaskStream;
    this.dagManagementStateStore = dagManagementStateStore;

    for (int i=0; i < numThreads; i++) {
      DagProcEngineThread dagProcEngineThread = new DagProcEngineThread(dagTaskStream, dagProcFactory, dagManagementStateStore);
      scheduledExecutorPool.submit(dagProcEngineThread);
    }
  }

  @AllArgsConstructor
  @VisibleForTesting
  static class DagProcEngineThread implements Runnable {
    private DagTaskStream dagTaskStream;
    private DagProcFactory dagProcFactory;
    private DagManagementStateStore dagManagementStateStore;

    @Override
    public void run() {
      while (true) {
        DagTask<DagProc> dagTask = dagTaskStream.next(); // blocking call
        if (dagTask == null) {
          log.warn("Received a null dag task, ignoring.");
          continue;
        }
        DagProc dagProc = dagTask.host(dagProcFactory);
        try {
          // todo - add retries
          dagProc.process(dagManagementStateStore);
          dagTask.conclude();
        } catch (Throwable t) {
          log.error("DagProcEngineThread encountered error ", t);
        }
        // todo mark lease success and releases it
        //dagTaskStream.complete(dagTask);
      }
    }
  }
}
