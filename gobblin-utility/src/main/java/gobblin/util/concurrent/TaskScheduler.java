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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Iterables;

import lombok.Synchronized;


/**
 * Base class for a task scheduler that can run {@link ScheduledTask}s periodically. Subclasses can implement
 * {@link #startImpl(Optional)} and {@link #closeImpl()}.
 *
 * <p>Implementations of this interface are expected to be thread-safe, and can be safely accessed
 * by multiple concurrent threads.</p>
 *
 * @author joelbaranick
 */
public abstract class TaskScheduler<K, T extends ScheduledTask<K>> implements Closeable {
  private final Cache<K, CancellableTask<K, T>> cancellableTaskMap = CacheBuilder.newBuilder().build();
  private volatile boolean isStarted = false;

  protected TaskScheduler() {}

  /**
   * Start the {@link TaskScheduler}.
   *
   * @param name the name of the {@link TaskScheduler}
   */
  @Synchronized
  final void start(Optional<String> name) {
    if (!this.isStarted) {
      this.startImpl(name);
      this.isStarted = true;
    }
  }

  /**
   * Start the {@link TaskScheduler}.
   *
   * @param name the name of the {@link TaskScheduler}
   */
  abstract void startImpl(Optional<String> name);

  /**
   * Schedules a subclass of {@link ScheduledTask} to run periodically.
   *
   * @param task the subclass of {@link ScheduledTask} to run every period
   * @param period the period between successive executions of the task
   * @param unit the time unit of the initialDelay and period parameters
   */
  public final void schedule(final T task, final long period, final TimeUnit unit) throws IOException {
    Preconditions.checkArgument(this.isStarted, "TaskScheduler is not started");
    try {
      CancellableTask<K, T> cancellableTask =
          this.cancellableTaskMap.get(task.getKey(), new Callable<CancellableTask<K, T>>() {
            @Override
            public CancellableTask<K, T> call() {
              return scheduleImpl(task, period, unit);
            }
          });
      if (cancellableTask.getScheduledTask() != task) {
        throw new IOException("Failed to schedule task with duplicate key");
      }
    } catch (ExecutionException e) {
      throw new IOException("Failed to schedule task", e);
    }
  }

  /**
   * Schedules a subclass of {@link ScheduledTask} to run periodically.
   *
   * @param task the subclass of {@link ScheduledTask} to run every period
   * @param period the period between successive executions of the task
   * @param unit the time unit of the period parameter
   */
  abstract CancellableTask<K, T> scheduleImpl(T task, long period, TimeUnit unit);

  /**
   * Gets all {@link ScheduledTask}s.
   *
   * @return the {@link ScheduledTask}s
   */
  public final Iterable<T> getScheduledTasks() {
    return Iterables.transform(this.cancellableTaskMap.asMap().values(), new Function<CancellableTask<K, T>, T>() {
      @Override
      public T apply(CancellableTask<K, T> cancellableTask) {
        return cancellableTask.getScheduledTask();
      }
    });
  }

  /**
   * Gets the {@link ScheduledTask} with the specified key.
   *
   * @return the {@link ScheduledTask}
   */
  public final Optional<T> getScheduledTask(K key) {
    CancellableTask<K, T> cancellableTask = this.cancellableTaskMap.getIfPresent(key);
    if (cancellableTask != null) {
      return Optional.of(cancellableTask.getScheduledTask());
    }
    return Optional.absent();
  }

  /**
   * Attempts to cancel the specified {@link ScheduledTask}.
   *
   * @param task the {@link ScheduledTask} to cancel
   * @return true if the {@link ScheduledTask} was canceled; otherwise, false
   */
  public boolean cancel(T task) {
    CancellableTask<K, T> cancellableTask = this.cancellableTaskMap.getIfPresent(task.getKey());
    if (cancellableTask != null && cancellableTask.getScheduledTask() == task && cancellableTask.cancel()) {
      this.cancellableTaskMap.invalidate(task.getKey());
      return true;
    }
    return false;
  }

  /**
   * Closes this {@link TaskScheduler}, ensuring that new tasks cannot be created
   * and cancelling existing tasks.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  @Synchronized
  public final void close() throws IOException {
    if (this.isStarted) {
      this.isStarted = false;
      this.closeImpl();
    }
  }

  /**
   * Closes this {@link TaskScheduler}, ensuring that new tasks cannot be created
   * and cancelling existing tasks.
   *
   * @throws IOException if an I/O error occurs
   */
  abstract void closeImpl() throws IOException;
}
