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

package org.apache.gobblin.util.concurrent;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.common.base.Optional;

import org.apache.gobblin.util.ExecutorsUtils;

import lombok.Synchronized;


/**
 * An implementation of {@link TaskScheduler} which schedules @{link ScheduledTask}s on an instance
 * of {@link HashedWheelTimer}.
 *
 * @param <K> the type of the key for the {@link ScheduledTask}s
 * @param <T> the type of the {@link ScheduledTask}s
 * @author joelbaranick
 */
class HashedWheelTimerTaskScheduler<K, T extends ScheduledTask<K>> extends TaskScheduler<K, T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(HashedWheelTimerTaskScheduler.class);
  private static HashedWheelTimer executor = new HashedWheelTimer(
      ExecutorsUtils.newDaemonThreadFactory(Optional.of(LOGGER), Optional.of("HashedWheelTimerTaskScheduler")));

  /**
   * Instantiates a new instance of {@link HashedWheelTimerTaskScheduler}.
   */
  HashedWheelTimerTaskScheduler() {}

  /**
   * Start the {@link TaskScheduler}.
   *
   * @param name the name of the {@link TaskScheduler}
   */
  @Override
  final void startImpl(Optional<String> name) {}

  /**
   * Schedules a subclass of {@link ScheduledTask} to run periodically.
   *
   * @param task the subclass of {@link ScheduledTask} to run every period
   * @param period the period between successive executions of the task
   * @param unit the time unit of the period parameter
   */
  @Override
  final CancellableTask<K, T> scheduleImpl(T task, long period, TimeUnit unit) {
    return new HashedWheelTimerTask<>(executor, task, period, unit);
  }

  /**
   * Closes this {@link TaskScheduler}, ensuring that new tasks cannot be created
   * and cancelling existing tasks.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  final void closeImpl() throws IOException {
    for (T scheduledTask : this.getScheduledTasks()) {
      this.cancel(scheduledTask);
    }
  }

  /**
   * The concrete implementation of {@link TimerTask} which is used to schedule a {@link ScheduledTask}
   * on a {@link HashedWheelTimer}.
   *
   * @param <K2> the type of the key of the {@link ScheduledTask}
   * @param <T2> the type of the {@link ScheduledTask}
   * @author joelbaranick
   */
  private class HashedWheelTimerTask<K2, T2 extends ScheduledTask<K2>> extends CancellableTask<K2, T2>
      implements TimerTask {
    private final HashedWheelTimer timer;
    private final T2 task;
    private final long period;
    private final TimeUnit unit;
    private final Map<String, String> context;

    private volatile Timeout future;

    /**
     * Instantiates a new instance of {@link HashedWheelTimerTask}.
     *
     * @param timer the {@link HashedWheelTimer} that the {@link HashedWheelTimerTask} is associated to.
     * @param task the {@link ScheduledTask} to run.
     * @param period the period between successive executions of the task
     * @param unit the time unit of the period parameter
     */
    HashedWheelTimerTask(HashedWheelTimer timer, T2 task, long period, TimeUnit unit) {
      super(task);
      this.timer = timer;
      this.task = task;
      this.period = period;
      this.unit = unit;
      this.context = MDC.getCopyOfContextMap();
      this.future = this.timer.newTimeout(this, this.period, this.unit);
    }

    /**
     * Executed after the delay specified with
     * {@link Timer#newTimeout(TimerTask, long, TimeUnit)}.
     *
     * @param timeout a handle which is associated with this task
     */
    @Override
    @Synchronized
    public void run(Timeout timeout) throws Exception {
      Map<String, String> originalContext = MDC.getCopyOfContextMap();
      if (this.context != null) {
        MDC.setContextMap(context);
      }
      try {
        this.task.runOneIteration();
      } finally {
        if (this.future != null) {
          this.future = this.timer.newTimeout(this, this.period, this.unit);
        }
        if (originalContext != null) {
          MDC.setContextMap(originalContext);
        } else {
          MDC.clear();
        }
      }
    }

    /**
     * Attempts to cancel execution of this task. If the task
     * has been executed or cancelled already, it will return
     * with no side effect.
     *
     * @return true if the task was cancelled; otherwise, false
     */
    @Override
    @Synchronized
    public boolean cancel() {
      if (this.future != null) {
        this.future.cancel();
        this.future = null;
      }
      return true;
    }
  }
}
