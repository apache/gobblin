/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.JobMetrics;
import gobblin.util.ExecutorsUtils;


/**
 * A class for executing new {@link Task}s and retrying failed ones.
 *
 * @author ynli
 */
public class TaskExecutor extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);

  // Thread pool executor for running tasks
  private final ExecutorService executor;

  // Scheduled thread pool executor for scheduling task retries
  private final ScheduledThreadPoolExecutor retryExecutor;

  // Task retry interval
  private final long retryIntervalInSeconds;

  /**
   * Constructor used internally.
   */
  private TaskExecutor(int taskExecutorThreadPoolSize, int coreRetryThreadPoolSize, int maxRetryThreadPoolSize,
      long retryIntervalInSeconds) {

    // Currently a fixed-size thread pool is used to execute tasks.
    // We probably need to revisit this later.
    this.executor =
        Executors.newFixedThreadPool(taskExecutorThreadPoolSize, ExecutorsUtils.newThreadFactory(Optional.of(LOG)));

    // Using a separate thread pool for task retries to achieve isolation
    // between normal task execution and task retries
    this.retryExecutor = new ScheduledThreadPoolExecutor(coreRetryThreadPoolSize);
    this.retryExecutor.setMaximumPoolSize(maxRetryThreadPoolSize);

    this.retryIntervalInSeconds = retryIntervalInSeconds;
  }

  /**
   * Constructor to work with {@link java.util.Properties}.
   */
  public TaskExecutor(Properties properties) {
    this(Integer.parseInt(properties.getProperty(ConfigurationKeys.TASK_EXECUTOR_THREADPOOL_SIZE_KEY,
            ConfigurationKeys.DEFAULT_TASK_EXECUTOR_THREADPOOL_SIZE)), Integer.parseInt(properties
            .getProperty(ConfigurationKeys.TASK_RETRY_THREAD_POOL_CORE_SIZE_KEY,
                ConfigurationKeys.DEFAULT_TASK_RETRY_THREAD_POOL_CORE_SIZE)), Integer.parseInt(properties
            .getProperty(ConfigurationKeys.TASK_RETRY_THREAD_POOL_MAX_SIZE_KEY,
                ConfigurationKeys.DEFAULT_TASK_RETRY_THREAD_POOL_MAX_SIZE)), Long.parseLong(properties
            .getProperty(ConfigurationKeys.TASK_RETRY_INTERVAL_IN_SEC_KEY,
                ConfigurationKeys.DEFAULT_TASK_RETRY_INTERVAL_IN_SEC)));
  }

  /**
   * Constructor to work with Hadoop {@link org.apache.hadoop.conf.Configuration}.
   */
  public TaskExecutor(Configuration conf) {
    this(Integer.parseInt(conf.get(ConfigurationKeys.TASK_EXECUTOR_THREADPOOL_SIZE_KEY,
            ConfigurationKeys.DEFAULT_TASK_EXECUTOR_THREADPOOL_SIZE)), Integer.parseInt(
            conf.get(ConfigurationKeys.TASK_RETRY_THREAD_POOL_CORE_SIZE_KEY,
                ConfigurationKeys.DEFAULT_TASK_RETRY_THREAD_POOL_CORE_SIZE)), Integer.parseInt(
            conf.get(ConfigurationKeys.TASK_RETRY_THREAD_POOL_MAX_SIZE_KEY,
                ConfigurationKeys.DEFAULT_TASK_RETRY_THREAD_POOL_MAX_SIZE)), Long.parseLong(
            conf.get(ConfigurationKeys.TASK_RETRY_INTERVAL_IN_SEC_KEY,
                ConfigurationKeys.DEFAULT_TASK_RETRY_INTERVAL_IN_SEC)));
  }

  @Override
  protected void startUp()
      throws Exception {
    LOG.info("Starting the task executor");
    if (this.executor.isShutdown() || this.executor.isTerminated()) {
      throw new IllegalStateException();
    }
    if (this.retryExecutor.isShutdown() || this.retryExecutor.isTerminated()) {
      throw new IllegalStateException();
    }
  }

  @Override
  protected void shutDown()
      throws Exception {
    LOG.info("Stopping the task executor ");
    this.executor.shutdown();
    this.retryExecutor.shutdown();
  }

  /**
   * Execute a {@link Task}.
   *
   * @param task {@link Task} to be executed
   */
  public void execute(Task task) {
    LOG.info(String.format("Executing task %s", task.getTaskId()));
    this.executor.execute(task);
  }

  /**
   * Submit a {@link Task} to run.
   *
   * @param task {@link Task} to be submitted
   * @return A {@link java.util.concurrent.Future} for the submitted {@link Task}
   */
  public Future<?> submit(Task task) {
    LOG.info(String.format("Submitting task %s", task.getTaskId()));
    return this.executor.submit(task);
  }

  /**
   * Retry a failed {@link Task}.
   *
   * @param task failed {@link Task} to be retried
   */
  public void retry(Task task) {
    if (JobMetrics.isEnabled(task.getTaskState().getWorkunit())) {
      // Adjust metrics to clean up numbers from the failed task
      task.getTaskState()
          .adjustJobMetricsOnRetry(task.getTaskState().getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY));
      // Remove task-level metrics associated with this task so
      // the retry will use fresh metrics
      task.getTaskState().removeMetrics();
    }

    // Task retry interval increases linearly with number of retries
    long interval = task.getRetryCount() * this.retryIntervalInSeconds;
    // Schedule the retry of the failed task
    this.retryExecutor.schedule(task, interval, TimeUnit.SECONDS);
    LOG.info(String.format("Scheduled retry of failed task %s to run in %d seconds", task.getTaskId(), interval));
    task.incrementRetryCount();
  }
}
