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

package gobblin.runtime;

import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.MDC;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.GobblinMetrics;
import gobblin.runtime.fork.Fork;
import gobblin.util.ExecutorsUtils;


/**
 * An abstract implementation of {@link TaskStateTracker} that provides basic common functionality for
 * platform-specific implementations.
 *
 * @author Yinan Li
 */
public abstract class AbstractTaskStateTracker extends AbstractIdleService implements TaskStateTracker {

  // This is used to schedule and run task metrics updaters
  private final ListeningScheduledExecutorService taskMetricsUpdaterExecutor;

  private final Logger logger;

  public AbstractTaskStateTracker(int coreThreadPoolSize, Logger logger) {
    Preconditions.checkArgument(coreThreadPoolSize > 0, "Thread pool size should be positive");
    this.taskMetricsUpdaterExecutor = ExecutorsUtils.loggingDecorator(
            new ScheduledThreadPoolExecutor(coreThreadPoolSize,
        ExecutorsUtils.newThreadFactory(Optional.of(logger), Optional.of("TaskStateTracker-%d"))));
    this.logger = logger;
  }

  public AbstractTaskStateTracker(Properties properties, Logger logger) {
    this(Integer.parseInt(properties.getProperty(ConfigurationKeys.TASK_STATE_TRACKER_THREAD_POOL_CORE_SIZE_KEY,
        Integer.toString(ConfigurationKeys.DEFAULT_TASK_STATE_TRACKER_THREAD_POOL_CORE_SIZE))), logger);
  }

  public AbstractTaskStateTracker(Configuration configuration, Logger logger) {
    this(Integer.parseInt(configuration.get(ConfigurationKeys.TASK_STATE_TRACKER_THREAD_POOL_CORE_SIZE_KEY,
        Integer.toString(ConfigurationKeys.DEFAULT_TASK_STATE_TRACKER_THREAD_POOL_CORE_SIZE))), logger);
  }

  @Override
  protected void startUp() throws Exception {
    this.logger.info("Starting the task state tracker");
  }

  @Override
  protected void shutDown() throws Exception {
    this.logger.info("Stopping the task state tracker");
    ExecutorsUtils.shutdownExecutorService(this.taskMetricsUpdaterExecutor, Optional.of(this.logger));
  }

  /**
   * Schedule a {@link TaskMetricsUpdater}.
   *
   * @param taskMetricsUpdater the {@link TaskMetricsUpdater} to schedule
   * @param task the {@link Task} that the {@link TaskMetricsUpdater} is associated to
   * @return a {@link java.util.concurrent.ScheduledFuture} corresponding to the scheduled {@link TaskMetricsUpdater}
   */
  protected ScheduledFuture<?> scheduleTaskMetricsUpdater(Runnable taskMetricsUpdater, Task task) {
    return this.taskMetricsUpdaterExecutor.scheduleAtFixedRate(taskMetricsUpdater,
        task.getTaskContext().getStatusReportingInterval(), task.getTaskContext().getStatusReportingInterval(),
        TimeUnit.MILLISECONDS);
  }

  /**
   * A base class providing a default implementation for updating task metrics.
   *
   * @deprecated see {@link gobblin.instrumented.writer.InstrumentedDataWriterBase}.
   */
  @Deprecated
  protected class TaskMetricsUpdater implements Runnable {

    protected final Task task;

    public TaskMetricsUpdater(Task task) {
      this.task = task;
    }

    @Override
    public void run() {
      MDC.put(ConfigurationKeys.TASK_KEY_KEY, task.getTaskKey());
      updateTaskMetrics();
    }

    protected void updateTaskMetrics() {
      if (GobblinMetrics.isEnabled(this.task.getTaskState().getWorkunit())) {
        this.task.updateRecordMetrics();
        this.task.updateByteMetrics();
      }
    }
  }
}
