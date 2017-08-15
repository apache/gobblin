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

package org.apache.gobblin.runtime;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.runtime.fork.Fork;
import org.apache.gobblin.util.ExecutorsUtils;

import lombok.Getter;

import static com.codahale.metrics.MetricRegistry.name;


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

  // The maximum number of items in the queued task time map.
  private final int queuedTaskTimeMaxSize;

  // The maximum age of the items in the queued task time map.
  private final long queuedTaskTimeMaxAge ;

  // Map of queued task ids to queue times.  The key is the task id, the value is the time the task was queued.  If the
  // task is being retried, the time may be in the future.  Entries with time in the future will not be counted as
  // queued until the time is in the past.
  private final Map<String, Long> queuedTasks = Maps.newConcurrentMap();

  // Set of historical queued task times. The key is the UTC epoch time the task started, the value is the milliseconds
  // the task waited to start.
  private final ConcurrentSkipListMap<Long, Long> queuedTaskTimeHistorical = new ConcurrentSkipListMap<>();

  // The timestamp for the last time the metric source data was pruned.
  private long lastCleanupTime = 0;

  // The total number of tasks currently queued and queued over the historical lookback period.
  private AtomicInteger queuedTaskCount = new AtomicInteger();

  // The total number of tasks currently queued.
  private AtomicInteger currentQueuedTaskCount = new AtomicInteger();

  // The total number of tasks queued over the historical lookback period.
  private AtomicInteger historicalQueuedTaskCount = new AtomicInteger();

  // The total time tasks have currently been in the queue and were in the queue during the historical lookback period.
  private AtomicLong queuedTaskTotalTime = new AtomicLong();

  // The total time tasks have currently been in the queue.
  private AtomicLong currentQueuedTaskTotalTime = new AtomicLong();

  // The total time tasks have been in the queue during the historical lookback period.
  private AtomicLong historicalQueuedTaskTotalTime = new AtomicLong();

  // Count of running tasks.
  private final Counter runningTaskCount = new Counter();

  // Count of failed tasks.
  private final Meter successfulTaskCount = new Meter();

  // Count of failed tasks.
  private final Meter failedTaskCount = new Meter();

  // The metric set exposed from the task executor.
  private final TaskExecutorQueueMetricSet metricSet = new TaskExecutorQueueMetricSet();

  /**
   * Constructor used internally.
   */
  private TaskExecutor(int taskExecutorThreadPoolSize, int coreRetryThreadPoolSize, long retryIntervalInSeconds,
                       int queuedTaskTimeMaxSize, long queuedTaskTimeMaxAge) {
    Preconditions.checkArgument(taskExecutorThreadPoolSize > 0, "Task executor thread pool size should be positive");
    Preconditions.checkArgument(retryIntervalInSeconds > 0, "Task retry interval should be positive");
    Preconditions.checkArgument(queuedTaskTimeMaxSize > 0, "Queued task time max size should be positive");
    Preconditions.checkArgument(queuedTaskTimeMaxAge > 0, "Queued task time max age should be positive");

    // Currently a fixed-size thread pool is used to execute tasks. We probably need to revisit this later.
    this.taskExecutor = Executors.newScheduledThreadPool(
        taskExecutorThreadPoolSize,
        ExecutorsUtils.newThreadFactory(Optional.of(LOG), Optional.of("TaskExecutor-%d")));

    this.retryIntervalInSeconds = retryIntervalInSeconds;
    this.queuedTaskTimeMaxSize = queuedTaskTimeMaxSize;
    this.queuedTaskTimeMaxAge = queuedTaskTimeMaxAge;

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
            Long.toString(ConfigurationKeys.DEFAULT_TASK_RETRY_INTERVAL_IN_SEC))),
        Integer.parseInt(properties.getProperty(ConfigurationKeys.QUEUED_TASK_TIME_MAX_SIZE,
            Integer.toString(ConfigurationKeys.DEFAULT_QUEUED_TASK_TIME_MAX_SIZE))),
        Long.parseLong(properties.getProperty(ConfigurationKeys.QUEUED_TASK_TIME_MAX_AGE,
            Long.toString(ConfigurationKeys.DEFAULT_QUEUED_TASK_TIME_MAX_AGE))));
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
            ConfigurationKeys.DEFAULT_TASK_RETRY_INTERVAL_IN_SEC),
        conf.getInt(ConfigurationKeys.QUEUED_TASK_TIME_MAX_SIZE,
            ConfigurationKeys.DEFAULT_QUEUED_TASK_TIME_MAX_SIZE),
        conf.getLong(ConfigurationKeys.QUEUED_TASK_TIME_MAX_AGE,
            ConfigurationKeys.DEFAULT_QUEUED_TASK_TIME_MAX_AGE));
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
    this.taskExecutor.execute(new TrackingTask(task));
  }

  /**
   * Submit a {@link Task} to run.
   *
   * @param task {@link Task} to be submitted
   * @return a {@link java.util.concurrent.Future} for the submitted {@link Task}
   */
  public Future<?> submit(Task task) {
    LOG.info(String.format("Submitting task %s", task.getTaskId()));
    return this.taskExecutor.submit(new TrackingTask(task));
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
    this.taskExecutor.schedule(new TrackingTask(task, interval, TimeUnit.SECONDS), interval, TimeUnit.SECONDS);
    LOG.info(String.format("Scheduled retry of failed task %s to run in %d seconds", task.getTaskId(), interval));
    task.incrementRetryCount();
  }

  public MetricSet getTaskExecutorQueueMetricSet() {
    return this.metricSet;
  }

  private synchronized void cleanupMetricSources() {
    long currentTimeMillis = System.currentTimeMillis();
    if (lastCleanupTime < currentTimeMillis - TimeUnit.SECONDS.toMillis(10)) {
      int currentQueuedTaskCount = 0;
      long currentQueuedTaskTotalTime = 0;
      for (Map.Entry<String, Long> queuedTask : this.queuedTasks.entrySet()) {
        if (queuedTask.getValue() <= currentTimeMillis) {
          currentQueuedTaskCount++;
          currentQueuedTaskTotalTime += queuedTask.getValue();
        }
      }
      this.currentQueuedTaskCount.set(currentQueuedTaskCount);
      this.currentQueuedTaskTotalTime.set(currentQueuedTaskTotalTime);

      int historicalQueuedTaskCount = 0;
      long historicalQueuedTaskTotalTime = 0;
      long cutoff = currentTimeMillis - queuedTaskTimeMaxAge;
      Iterator<Map.Entry<Long, Long>> iterator = queuedTaskTimeHistorical.descendingMap().entrySet().iterator();
      while (iterator.hasNext()) {
        try {
          Map.Entry<Long, Long> historicalQueuedTask = iterator.next();
          if (historicalQueuedTask.getKey() < cutoff || historicalQueuedTaskCount >= queuedTaskTimeMaxSize) {
            iterator.remove();
          } else {
            historicalQueuedTaskCount++;
            historicalQueuedTaskTotalTime += historicalQueuedTask.getValue();
          }
        } catch (NoSuchElementException e) {
          LOG.warn("Ran out of items in historical task queue time set.");
        }
      }
      this.historicalQueuedTaskCount.set(historicalQueuedTaskCount);
      this.historicalQueuedTaskTotalTime.set(historicalQueuedTaskTotalTime);

      this.queuedTaskCount.set(currentQueuedTaskCount + historicalQueuedTaskCount);
      this.queuedTaskTotalTime.set(currentQueuedTaskTotalTime + historicalQueuedTaskTotalTime);

      this.lastCleanupTime = currentTimeMillis;
    } else {
      LOG.debug("Skipped cleanup of metrics sources because not enough time has passed since last cleanup.");
    }
  }

  private class TaskExecutorQueueMetricSet implements MetricSet {
    @Override
    public Map<String, Metric> getMetrics() {
      final Map<String, Metric> metrics = new HashMap<>();
      metrics.put(name("queued", "current", "count"), new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          cleanupMetricSources();
          return currentQueuedTaskCount.intValue();
        }
      });
      metrics.put(name("queued", "historical", "count"), new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          cleanupMetricSources();
          return historicalQueuedTaskCount.intValue();
        }
      });
      metrics.put(name("queued", "count"), new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          cleanupMetricSources();
          return queuedTaskCount.intValue();
        }
      });
      metrics.put(name("queued", "current", "time", "total"), new Gauge<Long>() {
        @Override
        public Long getValue() {
          cleanupMetricSources();
          return currentQueuedTaskTotalTime.longValue();
        }
      });
      metrics.put(name("queued", "historical", "time", "total"), new Gauge<Long>() {
        @Override
        public Long getValue() {
          cleanupMetricSources();
          return historicalQueuedTaskTotalTime.longValue();
        }
      });
      metrics.put(name("queued", "time", "total"), new Gauge<Long>() {
        @Override
        public Long getValue() {
          cleanupMetricSources();
          return queuedTaskTotalTime.longValue();
        }
      });
      metrics.put(name("running", "count"), runningTaskCount);
      metrics.put(name("successful", "count"), successfulTaskCount);
      metrics.put(name("failed", "count"), failedTaskCount);
      return Collections.unmodifiableMap(metrics);
    }
  }

  private class TrackingTask implements Runnable {
    private Task underlyingTask;

    public TrackingTask(Task task) {
      this(task, 0, TimeUnit.SECONDS);
    }

    public TrackingTask(Task task, long interval, TimeUnit timeUnit) {
      queuedTasks.putIfAbsent(task.getTaskId(), System.currentTimeMillis() + timeUnit.toMillis(interval));
      this.underlyingTask = task;
    }

    @Override
    public void run() {
      long startTime = System.currentTimeMillis();
      onStart(startTime);
      try {
        this.underlyingTask.run();
        successfulTaskCount.mark();;
      } catch (Exception e) {
        failedTaskCount.mark();
        throw e;
      } finally {
        runningTaskCount.dec();
      }
    }

    private void onStart(long startTime) {
      Long queueTime = queuedTasks.remove(this.underlyingTask.getTaskId());
      queuedTaskTimeHistorical.putIfAbsent(System.currentTimeMillis(), startTime - queueTime);
      runningTaskCount.inc();
    }
  }
}
