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

package gobblin.util.concurrent;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import gobblin.util.ExecutorsUtils;


/**
 * An implementation of {@link TaskScheduler} which schedules @{link ScheduledTask}s on an instance
 * of {@link ScheduledExecutorService}.
 *
 * @param <K> the type of the key for the {@link ScheduledTask}s
 * @param <T> the type of the {@link ScheduledTask}s
 * @author joelbaranick
 */
class ScheduledExecutorServiceTaskScheduler<K, T extends ScheduledTask<K>> extends TaskScheduler<K, T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledExecutorServiceTaskScheduler.class);

  private ScheduledExecutorService executorService;

  /**
   * Instantiates a new instance of {@link ScheduledExecutorServiceTaskScheduler}.
   */
  ScheduledExecutorServiceTaskScheduler() {}

  /**
   * Start the {@link TaskScheduler}.
   *
   * @param name the name of the {@link TaskScheduler}
   */
  @Override
  final void startImpl(Optional<String> name) {
    this.executorService =
        Executors.newScheduledThreadPool(0, ExecutorsUtils.newDaemonThreadFactory(Optional.of(LOGGER), name));
  }

  /**
   * Schedules a subclass of {@link ScheduledTask} to run periodically.
   *
   * @param task the subclass of {@link ScheduledTask} to run every period
   * @param period the period between successive executions of the task
   * @param unit the time unit of the period parameter
   */
  @Override
  final CancellableTask<K, T> scheduleImpl(final T task, long period, TimeUnit unit) {
    final ScheduledFuture<?> future = this.executorService.scheduleAtFixedRate(new RunnableTask(task), 0, period, unit);
    return new CancellableScheduledFuture<>(task, future);
  }

  /**
   * Closes this {@link TaskScheduler}, ensuring that new tasks cannot be created
   * and cancelling existing tasks.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  final void closeImpl() throws IOException {
    ExecutorsUtils.shutdownExecutorService(this.executorService, Optional.of(LOGGER));
  }

  /**
   * An implementation of {@link Runnable} which will run the specified {@link ScheduledTask}.
   */
  private class RunnableTask implements Runnable {
    private final T task;

    /**
     * Instantiates a new {@link RunnableTask}.
     *
     * @param task the {@link ScheduledTask} to run
     */
    public RunnableTask(T task) {
      this.task = task;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see     java.lang.Thread#run()
     */
    @Override
    public void run() {
      this.task.runOneIteration();
    }
  }

  /**
   * An implementation of {@link CancellableTask} which can cancel the underlying {@link ScheduledFuture}.
   *
   * @param <K2> the type of the key of the {@link ScheduledTask}
   * @param <T2> the type of the {@link ScheduledTask}
   */
  private class CancellableScheduledFuture<K2, T2 extends ScheduledTask<K2>> extends CancellableTask<K2, T2> {
    private final ScheduledFuture<?> future;

    /**
     * Instantiates a new {@link CancellableScheduledFuture}.
     *
     * @param task the underlying {@link ScheduledTask}
     * @param future the underlying {@link ScheduledFuture}
     */
    public CancellableScheduledFuture(T2 task, ScheduledFuture<?> future) {
      super(task);
      this.future = future;
    }

    /**
     * Attempts to cancel execution of this task. If the task
     * has been executed or cancelled already, it will return
     * with no side effect.
     *
     * @return true if the task was cancelled; otherwise, false
     */
    @Override
    public boolean cancel() {
      this.future.cancel(true);
      return true;
    }
  }
}
