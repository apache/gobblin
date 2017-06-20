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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import gobblin.runtime.fork.Fork;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.GobblinMetrics;
import gobblin.util.ExecutorsUtils;

import lombok.Getter;


/**
 * A class for executing {@link Task}s and retrying failed ones as well as for executing {@link Fork}s.
 *
 * @author Yinan Li
 */
public class TaskExecutor extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutor.class);

  // Thread pool executor for running tasks
  private final ScheduledExecutorService taskExecutor;

  // A separate thread pool executor for running forks of tasks
  @Getter
  private final ExecutorService forkExecutor;

  // Task retry interval
  private final long retryIntervalInSeconds;

  /**
   * Constructor used internally.
   */
  private TaskExecutor(int taskExecutorThreadPoolSize, int coreRetryThreadPoolSize, long retryIntervalInSeconds) {
    Preconditions.checkArgument(taskExecutorThreadPoolSize > 0, "Task executor thread pool size should be positive");
    Preconditions.checkArgument(retryIntervalInSeconds > 0, "Task retry interval should be positive");

    // Currently a fixed-size thread pool is used to execute tasks. We probably need to revisit this later.
    this.taskExecutor = Executors.newScheduledThreadPool(
        taskExecutorThreadPoolSize,
        ExecutorsUtils.newThreadFactory(Optional.of(LOG), Optional.of("TaskExecutor-%d")));

    this.retryIntervalInSeconds = retryIntervalInSeconds;

    this.forkExecutor = new ThreadPoolExecutor(
        // The core thread pool size is equal to that of the task executor as there's at least one fork per task
        taskExecutorThreadPoolSize,
        // The fork executor thread pool size is essentially unbounded. This is to make sure all forks of
        // a task get a thread to run so all forks of the task are making progress. This is necessary since
        // otherwise the parent task will be blocked if the record queue (bounded) of some fork is full and
        // that fork has not yet started to run because of no available thread. The task cannot proceed in
        // this case because it has to make sure every records go to every forks.
        Integer.MAX_VALUE,
        0L,
        TimeUnit.MILLISECONDS,
        // The work queue is a SynchronousQueue. This essentially forces a new thread to be created for each fork.
        new SynchronousQueue<Runnable>(),
        ExecutorsUtils.newThreadFactory(Optional.of(LOG), Optional.of("ForkExecutor-%d")));
  }

  /**
   * Constructor to work with {@link java.util.Properties}.
   */
  public TaskExecutor(Properties properties) {
    this(Integer.parseInt(properties.getProperty(ConfigurationKeys.TASK_EXECUTOR_THREADPOOL_SIZE_KEY,
            Integer.toString(ConfigurationKeys.DEFAULT_TASK_EXECUTOR_THREADPOOL_SIZE))),
        Integer.parseInt(properties.getProperty(ConfigurationKeys.TASK_RETRY_THREAD_POOL_CORE_SIZE_KEY,
            Integer.toString(ConfigurationKeys.DEFAULT_TASK_RETRY_THREAD_POOL_CORE_SIZE))),
        Long.parseLong(properties.getProperty(ConfigurationKeys.TASK_RETRY_INTERVAL_IN_SEC_KEY,
            Long.toString(ConfigurationKeys.DEFAULT_TASK_RETRY_INTERVAL_IN_SEC))));
  }

  /**
   * Constructor to work with Hadoop {@link org.apache.hadoop.conf.Configuration}.
   */
  public TaskExecutor(Configuration conf) {
    this(conf.getInt(ConfigurationKeys.TASK_EXECUTOR_THREADPOOL_SIZE_KEY,
            ConfigurationKeys.DEFAULT_TASK_EXECUTOR_THREADPOOL_SIZE),
        conf.getInt(ConfigurationKeys.TASK_RETRY_THREAD_POOL_CORE_SIZE_KEY,
            ConfigurationKeys.DEFAULT_TASK_RETRY_THREAD_POOL_CORE_SIZE),
        conf.getLong(ConfigurationKeys.TASK_RETRY_INTERVAL_IN_SEC_KEY,
            ConfigurationKeys.DEFAULT_TASK_RETRY_INTERVAL_IN_SEC));
  }

  @Override
  protected void startUp()
      throws Exception {
    LOG.info("Starting the task executor");
    if (this.taskExecutor.isShutdown() || this.taskExecutor.isTerminated()) {
      throw new IllegalStateException("Task thread pool executor is shutdown or terminated");
    }
    if (this.forkExecutor.isShutdown() || this.forkExecutor.isTerminated()) {
      throw new IllegalStateException("Fork thread pool executor is shutdown or terminated");
    }
  }

  @Override
  protected void shutDown()
      throws Exception {
    LOG.info("Stopping the task executor");
    try {
      ExecutorsUtils.shutdownExecutorService(this.taskExecutor, Optional.of(LOG));
    } finally {
      ExecutorsUtils.shutdownExecutorService(this.forkExecutor, Optional.of(LOG));
    }
  }

  /**
   * Execute a {@link Task}.
   *
   * @param task {@link Task} to be executed
   */
  public void execute(Task task) {
    LOG.info(String.format("Executing task %s", task.getTaskId()));
    this.taskExecutor.execute(task);
  }

  /**
   * Submit a {@link Task} to run.
   *
   * @param task {@link Task} to be submitted
   * @return a {@link java.util.concurrent.Future} for the submitted {@link Task}
   */
  public Future<?> submit(Task task) {
    LOG.info(String.format("Submitting task %s", task.getTaskId()));
    return this.taskExecutor.submit(task);
  }

  /**
   * Execute a {@link Fork}.
   *
   * @param fork {@link Fork} to be executed
   */
  public void execute(Fork fork) {
    LOG.info(String.format("Executing fork %d of task %s", fork.getIndex(), fork.getTaskId()));
    this.forkExecutor.execute(fork);
  }

  /**
   * Submit a {@link Fork} to run.
   *
   * @param fork {@link Fork} to be submitted
   * @return a {@link java.util.concurrent.Future} for the submitted {@link Fork}
   */
  public Future<?> submit(Fork fork) {
    LOG.info(String.format("Submitting fork %d of task %s", fork.getIndex(), fork.getTaskId()));
    return this.forkExecutor.submit(fork);
  }

  /**
   * Retry a failed {@link Task}.
   *
   * @param task failed {@link Task} to be retried
   */
  public void retry(Task task) {
    if (GobblinMetrics.isEnabled(task.getTaskState().getWorkunit()) &&
        task.getTaskState().contains(ConfigurationKeys.FORK_BRANCHES_KEY)) {
      // Adjust metrics to clean up numbers from the failed task
      task.getTaskState()
          .adjustJobMetricsOnRetry(task.getTaskState().getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY));
    }

    // Task retry interval increases linearly with number of retries
    long interval = task.getRetryCount() * this.retryIntervalInSeconds;
    // Schedule the retry of the failed task
    this.taskExecutor.schedule(task, interval, TimeUnit.SECONDS);
    LOG.info(String.format("Scheduled retry of failed task %s to run in %d seconds", task.getTaskId(), interval));
    task.incrementRetryCount();
  }
}
