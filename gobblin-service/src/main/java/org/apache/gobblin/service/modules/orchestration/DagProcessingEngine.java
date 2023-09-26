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
import com.typesafe.config.ConfigFactory;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.service.ServiceConfigKeys.GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_ENABLED_KEY;


/**
 * Responsible for polling {@link DagTask}s from {@link DagTaskStream} and processing the {@link org.apache.gobblin.service.modules.flowgraph.Dag}
 * based on the type of {@link DagTask} which is determined by the {@link org.apache.gobblin.runtime.api.DagActionStore.DagAction}.
 * Each {@link DagTask} acquires a lease for the {@link org.apache.gobblin.runtime.api.DagActionStore.DagAction}.
 * The {@link DagProcFactory} then provides the appropriate {@link DagProc} associated with the {@link DagTask}.
 * The actual work or processing is done by the {@link DagProc#process(DagManagementStateStore, int, long)}
 */

@Alpha
@Slf4j
public class DagProcessingEngine {

  public static final String NUM_THREADS_KEY = GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_ENABLED_KEY + "numThreads";
  public static final String JOB_STATUS_POLLING_INTERVAL_KEY = GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_ENABLED_KEY + "pollingInterval";
  public static final String MAX_RETRY_ATTEMPTS = GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_ENABLED_KEY + "maxRetryAttempts";
  public static final String RETRY_DELAY_MS = GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_ENABLED_KEY + "retryDelayMillis";


  private static final Integer DEFAULT_NUM_THREADS = 3;
  private static final Integer DEFAULT_JOB_STATUS_POLLING_INTERVAL = 10;
  private static final Integer DEFAULT_MAX_RETRY_ATTEMPTS = 3;
  private static final long DEFAULT_RETRY_DELAY_MS = 1000;

  private final DagTaskStream dagTaskStream;
  private final DagProcFactory dagProcFactory;
  private final DagManagementStateStore dagManagementStateStore;
  private final ScheduledExecutorService scheduledExecutorPool;
  private final MetricContext metricContext;
  private final Optional<EventSubmitter> eventSubmitter;
  private final Config config;
  private final Integer numThreads;
  private final Integer pollingInterval;
  private final Integer maxRetryAttempts;
  private final long delayRetryMillis;
  private final DagProcessingEngine.Thread [] threads;


  public DagProcessingEngine(Config config, DagTaskStream dagTaskStream, DagProcFactory dagProcFactory, DagManagementStateStore dagManagementStateStore, MultiActiveLeaseArbiter multiActiveLeaseArbiter) {
    this.config = config;
    this.dagTaskStream = dagTaskStream;
    this.dagProcFactory = dagProcFactory;
    this.dagManagementStateStore = dagManagementStateStore;
    metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.eventSubmitter = Optional.of(new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build());

    this.numThreads = ConfigUtils.getInt(config, NUM_THREADS_KEY, DEFAULT_NUM_THREADS);
    this.scheduledExecutorPool = Executors.newScheduledThreadPool(numThreads);
    this.pollingInterval = ConfigUtils.getInt(config, JOB_STATUS_POLLING_INTERVAL_KEY, DEFAULT_JOB_STATUS_POLLING_INTERVAL);
    this.maxRetryAttempts = ConfigUtils.getInt(config, MAX_RETRY_ATTEMPTS, DEFAULT_MAX_RETRY_ATTEMPTS);
    this.delayRetryMillis = ConfigUtils.getLong(config, RETRY_DELAY_MS, DEFAULT_RETRY_DELAY_MS);
    this.threads = new DagProcessingEngine.Thread[this.numThreads];
    for(int i=0; i < this.numThreads; i++) {
      Thread thread = new Thread(dagTaskStream, dagProcFactory, dagManagementStateStore, eventSubmitter, this.maxRetryAttempts, this.delayRetryMillis);
      this.threads[i] = thread;
    }
  }


  @AllArgsConstructor
  private static class Thread implements Runnable {

    private DagTaskStream dagTaskStream;
    private DagProcFactory dagProcFactory;
    private DagManagementStateStore dagManagementStateStore;
    private Optional<EventSubmitter> eventSubmitter;
    private Integer maxRetryAttempts;
    private long delayRetryMillis;

    @Override
    public void run() {
      try {

        for(DagTask dagTask : dagTaskStream) {
            DagProc dagProc = dagTask.host(dagProcFactory);
            dagProc.process(dagManagementStateStore, eventSubmitter.get(), maxRetryAttempts, delayRetryMillis);
            //marks lease success and releases it
            dagTaskStream.complete(dagTask);
        }
      } catch (Exception ex) {
        //TODO: need to handle exceptions gracefully
        log.error(String.format("Exception encountered in %s", getClass().getName()), ex);
        throw new RuntimeException(ex);
      }
    }
  }
}
