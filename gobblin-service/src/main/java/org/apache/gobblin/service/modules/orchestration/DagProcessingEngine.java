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
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;


/**
 * Responsible for polling {@link DagTask}s from {@link DagTaskStream} and processing the
 * {@link org.apache.gobblin.service.modules.flowgraph.Dag} based on the type of {@link DagTask}.
 * Each {@link DagTask} returned from the {@link DagTaskStream} comes with a time-limited lease conferring the exclusive
 * right to perform the work of the task.
 * The {@link DagProcFactory} transforms each {@link DagTask} into a specific, concrete {@link DagProc}, which
 * encapsulates all processing inside {@link DagProc#process(DagManagementStateStore, DagProcessingEngineMetrics)}
 */

@AllArgsConstructor
@Alpha
@Slf4j
@Singleton
public class DagProcessingEngine extends AbstractIdleService {

  @Getter private final DagTaskStream dagTaskStream;
  @Getter DagManagementStateStore dagManagementStateStore;
  private final Config config;
  private final DagProcFactory dagProcFactory;
  private ScheduledExecutorService scheduledExecutorPool;
  private final DagProcessingEngineMetrics dagProcEngineMetrics;
  private static final Integer TERMINATION_TIMEOUT = 30;
  public static final String DEFAULT_JOB_START_DEADLINE_TIME_MS = "defaultJobStartDeadlineTimeMillis";
  @Getter static long defaultJobStartDeadlineTimeMillis;
  public static final String DEFAULT_FLOW_FAILURE_OPTION = FailureOption.FINISH_ALL_POSSIBLE.name();

  @Inject
  public DagProcessingEngine(Config config, DagTaskStream dagTaskStream, DagProcFactory dagProcFactory,
      DagManagementStateStore dagManagementStateStore,
      @Named(DEFAULT_JOB_START_DEADLINE_TIME_MS) long deadlineTimeMs, DagProcessingEngineMetrics dagProcEngineMetrics) {
    this.config = config;
    this.dagProcFactory = dagProcFactory;
    this.dagTaskStream = dagTaskStream;
    this.dagManagementStateStore = dagManagementStateStore;
    this.dagProcEngineMetrics = dagProcEngineMetrics;
    log.info("DagProcessingEngine initialized.");
    setDefaultJobStartDeadlineTimeMs(deadlineTimeMs);
  }

  private static void setDefaultJobStartDeadlineTimeMs(long deadlineTimeMs) {
    defaultJobStartDeadlineTimeMillis = deadlineTimeMs;
  }

  @Override
  protected void startUp() {
    Integer numThreads = ConfigUtils.getInt
        (config, ServiceConfigKeys.NUM_DAG_PROC_THREADS_KEY, ServiceConfigKeys.DEFAULT_NUM_DAG_PROC_THREADS);
    this.scheduledExecutorPool =
        Executors.newScheduledThreadPool(numThreads,
            ExecutorsUtils.newThreadFactory(com.google.common.base.Optional.of(log),
                com.google.common.base.Optional.of("DagProcessingEngineThread")));
    for (int i=0; i < numThreads; i++) {
      // todo - set metrics for count of active DagProcEngineThread
      DagProcEngineThread dagProcEngineThread = new DagProcEngineThread(dagTaskStream, dagProcFactory,
          dagManagementStateStore, dagProcEngineMetrics, i);
      this.scheduledExecutorPool.submit(dagProcEngineThread);
    }
  }

  @Override
  protected void shutDown()
      throws Exception {
    log.info("DagProcessingEngine shutting down.");
    this.scheduledExecutorPool.shutdown();
    this.scheduledExecutorPool.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.SECONDS);
  }

  /**
   * Action to be performed on a {@link Dag}, in case of a job failure. Currently, we allow 2 modes:
   * <ul>
   *   <li> FINISH_RUNNING, which allows currently running jobs to finish.</li>
   *   <li> FINISH_ALL_POSSIBLE, which allows every possible job in the Dag to finish, as long as all the dependencies
   *   of the job are successful.</li>
   * </ul>
   */
  public enum FailureOption {
    FINISH_RUNNING,
    CANCEL,
    FINISH_ALL_POSSIBLE
  }

  @AllArgsConstructor
  @VisibleForTesting
  static class DagProcEngineThread implements Runnable {
    private final DagTaskStream dagTaskStream;
    private final DagProcFactory dagProcFactory;
    private final DagManagementStateStore dagManagementStateStore;
    private final DagProcessingEngineMetrics dagProcEngineMetrics;
    private final int threadID;

    @Override
    public void run() {
      log.info("Starting DagProcEngineThread to process dag tasks. Thread id: {}", threadID);

      while (true) {
        DagTask dagTask = dagTaskStream.next(); // blocking call
        if (dagTask == null) {
          //todo - add a metrics to count the times dagTask was null
          log.warn("Received a null dag task, ignoring.");
          continue;
        }
        DagProc<?> dagProc = dagTask.host(dagProcFactory);
        try {
          dagProc.process(dagManagementStateStore, dagProcEngineMetrics);
          dagTask.conclude();
          log.info("Concluded dagTask : {}", dagTask);
        } catch (Exception e) {
          log.error("DagProcEngineThread encountered exception while processing dag " + dagProc.getDagId(), e);
          dagManagementStateStore.getDagManagerMetrics().dagProcessingExceptionMeter.mark();
        }
      }
    }
  }
}
