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
package org.apache.gobblin.util.executors;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class MDCPropagatingScheduledExecutorService extends ForwardingListeningExecutorService implements ListeningScheduledExecutorService {
  private final ListeningScheduledExecutorService executorService;

  public MDCPropagatingScheduledExecutorService(ScheduledExecutorService executorService) {
    if (executorService instanceof ListeningScheduledExecutorService) {
      this.executorService = (ListeningScheduledExecutorService)executorService;
    } else {
      this.executorService = MoreExecutors.listeningDecorator(executorService);
    }
  }

  @Override
  protected ListeningExecutorService delegate() {
    return executorService;
  }

  @Override
  public void execute(Runnable command) {
    super.execute(new MDCPropagatingRunnable(command));
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    return super.submit(new MDCPropagatingCallable<T>(task));
  }

  @Override
  public ListenableFuture<?> submit(Runnable task) {
    return super.submit(new MDCPropagatingRunnable(task));
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    return super.submit(new MDCPropagatingRunnable(task), result);
  }

  @Override
  public ListenableScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    ListenableFutureTask<Void> task = ListenableFutureTask.create(new MDCPropagatingRunnable(command), null);
    ScheduledFuture<?> scheduled = executorService.schedule(task, delay, unit);
    return new ListenableScheduledTask<>(task, scheduled);
  }

  @Override
  public <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    ListenableFutureTask<V> task = ListenableFutureTask.create(new MDCPropagatingCallable<>(callable));
    ScheduledFuture<?> scheduled = executorService.schedule(task, delay, unit);
    return new ListenableScheduledTask<>(task, scheduled);
  }

  @Override
  public ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    NeverSuccessfulListenableFutureTask task = new NeverSuccessfulListenableFutureTask(new MDCPropagatingRunnable(command));
    ScheduledFuture<?> scheduled = executorService.scheduleAtFixedRate(task, initialDelay, period, unit);
    return new ListenableScheduledTask<>(task, scheduled);
  }

  @Override
  public ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    NeverSuccessfulListenableFutureTask task = new NeverSuccessfulListenableFutureTask(new MDCPropagatingRunnable(command));
    ScheduledFuture<?> scheduled = executorService.scheduleWithFixedDelay(task, initialDelay, delay, unit);
    return new ListenableScheduledTask<>(task, scheduled);
  }

  @SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
  private static final class ListenableScheduledTask<V> extends ForwardingListenableFuture.SimpleForwardingListenableFuture<V> implements ListenableScheduledFuture<V> {
    private final ScheduledFuture<?> scheduledDelegate;

    public ListenableScheduledTask(ListenableFuture<V> listenableDelegate, ScheduledFuture<?> scheduledDelegate) {
      super(listenableDelegate);
      this.scheduledDelegate = scheduledDelegate;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (cancelled) {
        scheduledDelegate.cancel(mayInterruptIfRunning);
      }
      return cancelled;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return scheduledDelegate.getDelay(unit);
    }

    @Override
    public int compareTo(Delayed other) {
      return scheduledDelegate.compareTo(other);
    }
  }

  private static final class NeverSuccessfulListenableFutureTask extends AbstractFuture<Void> implements Runnable {
    private final Runnable delegate;

    public NeverSuccessfulListenableFutureTask(Runnable delegate) {
      this.delegate = checkNotNull(delegate);
    }

    @Override public void run() {
      try {
        delegate.run();
      } catch (Throwable t) {
        setException(t);
        throw Throwables.propagate(t);
      }
    }
  }
}