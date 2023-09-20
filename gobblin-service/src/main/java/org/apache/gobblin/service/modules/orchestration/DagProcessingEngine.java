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

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.orchestration.processor.DagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.util.ConfigUtils;


@Alpha
@Slf4j
public class DagProcessingEngine {

  public static final String DAG_PROCESSING_ENGINE_PREFIX = "gobblin.service.dagProcessingEngine.";
  public static final String NUM_THREADS_KEY = DAG_PROCESSING_ENGINE_PREFIX + "numThreads";
  public static final String JOB_STATUS_POLLING_INTERVAL_KEY = DAG_PROCESSING_ENGINE_PREFIX + "pollingInterval";

  private static final Integer DEFAULT_NUM_THREADS = 3;
  private static final Integer DEFAULT_JOB_STATUS_POLLING_INTERVAL = 10;

  private DagTaskStream dagTaskStream;
  private DagProcFactory dagProcFactory;
  private DagManagementStateStore dagManagementStateStore;
  private ScheduledExecutorService scheduledExecutorPool;
  private Config config;
  private Integer numThreads;
  private Integer pollingInterval;
  private DagProcessingEngine.Thread [] threads;
  private MultiActiveLeaseArbiter multiActiveLeaseArbiter;


  public DagProcessingEngine(Config config, DagTaskStream dagTaskStream, DagProcFactory dagProcFactory, DagManagementStateStore dagManagementStateStore, MultiActiveLeaseArbiter multiActiveLeaseArbiter) {
    this.config = config;
    this.dagTaskStream = dagTaskStream;
    this.dagProcFactory = dagProcFactory;
    this.dagManagementStateStore = dagManagementStateStore;
    this.multiActiveLeaseArbiter = multiActiveLeaseArbiter;
    this.numThreads = ConfigUtils.getInt(config, NUM_THREADS_KEY, DEFAULT_NUM_THREADS);
    this.scheduledExecutorPool = Executors.newScheduledThreadPool(numThreads);
    this.pollingInterval = ConfigUtils.getInt(config, JOB_STATUS_POLLING_INTERVAL_KEY, DEFAULT_JOB_STATUS_POLLING_INTERVAL);
    this.threads = new DagProcessingEngine.Thread[this.numThreads];
    for(int i=0; i < this.numThreads; i++) {
      Thread thread = new Thread(dagTaskStream, dagProcFactory, dagManagementStateStore, multiActiveLeaseArbiter);
      this.threads[i] = thread;
    }
  }


  @AllArgsConstructor
  private static class Thread implements Runnable {
    private DagTaskStream dagTaskStream;
    private DagProcFactory dagProcFactory;
    private DagManagementStateStore dagManagementStateStore;
    private MultiActiveLeaseArbiter multiActiveLeaseArbiter;

    @Override
    public void run() {
      try {
        while (dagTaskStream.hasNext()) {
          Optional<DagTask> dagTask = getNextTask();
          if (dagTask.isPresent()) {
            DagProc dagProc = (DagProc) dagTask.get().host(dagProcFactory);
            dagProc.process(dagManagementStateStore);
            //marks lease success and releases it
            dagTask.get().conclude(multiActiveLeaseArbiter);
          }
        }
      } catch (Exception ex) {
        //TODO: need to handle exceptions gracefully
        log.error(String.format("Exception encountered in %s", getClass().getName()), ex);
        throw new RuntimeException(ex);
      }
    }
    private Optional<DagTask> getNextTask() {
      return dagTaskStream.next();
    }
  }
}
