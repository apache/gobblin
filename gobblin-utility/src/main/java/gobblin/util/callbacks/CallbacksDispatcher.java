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
package gobblin.util.callbacks;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.util.ExecutorsUtils;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * A helper to dispatch callbacks to a set of listeners. The CallbacksDispatcher is responsible for
 * managing the list if listeners which implement a common interface L. Invocation happens through
 * the {@link #execCallbacks(CallbackFactory) method.
 *
 * @param L     the listener type; it is strongly advised that the class implements toString() to
 *              provide useful logging
 */
public class CallbacksDispatcher<L> implements Closeable {
  private final Logger _log;
  private final List<L> _listeners = new ArrayList<>();
  private final WeakHashMap<L, Void> _autoListeners = new WeakHashMap<>();
  private final ExecutorService _execService;

  /**
   * Constructor
   * @param execService   optional executor services for the callbacks; if none is specified, a
   *                      single-thread executor will be used
   * @param log           optional logger; if none is specified, a default one will be created
   */
  public CallbacksDispatcher(Optional<ExecutorService> execService, Optional<Logger> log) {
    Preconditions.checkNotNull(execService);
    Preconditions.checkNotNull(log);

    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    _execService = execService.isPresent() ? execService.get() : getDefaultExecutor(_log);
  }

  public static ExecutorService getDefaultExecutor(Logger log) {
    return Executors.newSingleThreadExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of(log.getName() + "-%d")));
  }

  public CallbacksDispatcher() {
    this(Optional.<ExecutorService>absent(), Optional.<Logger>absent());
  }

  public CallbacksDispatcher(Logger log) {
    this(Optional.<ExecutorService>absent(), Optional.of(log));
  }

  public CallbacksDispatcher(ExecutorService execService, Logger log) {
    this(Optional.of(execService), Optional.of(log));
  }

  @Override
  public void close()
      throws IOException {
    ExecutorsUtils.shutdownExecutorService(_execService, Optional.of(_log), 5, TimeUnit.SECONDS);
  }

  public synchronized List<L> getListeners() {
    // Clone to protect against adding/removing listeners while running callbacks
    ArrayList<L> res = new ArrayList<>(_listeners);

    // Scan any auto listeners
    for (Map.Entry<L, Void> entry: _autoListeners.entrySet()) {
      res.add(entry.getKey());
    }

    return res;
  }

  public synchronized void addListener(L listener) {
    Preconditions.checkNotNull(listener);

    _log.info("Adding listener:" + listener);
    _listeners.add(listener);
  }

  /**
   * Only weak references are stored for weak listeners. They will be removed from the dispatcher
   * automatically, once the listener objects are GCed. Note that weak listeners cannot be removed
   * explicitly. */
  public synchronized void addWeakListener(L listener) {
    Preconditions.checkNotNull(listener);

    _log.info("Adding a weak listener " + listener);
    _autoListeners.put(listener, null);
  }

  public synchronized void removeListener(L listener) {
    Preconditions.checkNotNull(listener);

    _log.info("Removing listener:" + listener);
    _listeners.remove(listener);
  }

  public Logger getLog() {
    return _log;
  }
  public <R> CallbackResults<L, R> execCallbacks(Function<? super L, R> callback, L listener)
         throws InterruptedException {
    Preconditions.checkNotNull(listener);
    List<L> listenerList = new ArrayList<>(1);
    listenerList.add(listener);

    return execCallbacks(callback, listenerList);
  }

  public <R> CallbackResults<L, R> execCallbacks(Function<? super L, R> callback)
         throws InterruptedException {
    Preconditions.checkNotNull(callback);
    List<L> listeners = getListeners();

    return execCallbacks(callback, listeners);
  }

  private <R> CallbackResults<L, R> execCallbacks(Function<? super L, R> callback,
                                                          List<L> listeners)
      throws InterruptedException {
    CallbackResults<L, R> res = new CallbackResults<L, R>();
    if (0 == listeners.size()) {
      return res;
    }

    List<Callable<R>> callbacks = new ArrayList<>(listeners.size());
    for (L listener: listeners) {
      callbacks.add(new CallbackCallable<>(callback, listener));
    }

    List<Future<R>> futures = _execService.invokeAll(callbacks);
    for (int i = 0; i < listeners.size(); ++i) {
      CallbackResult<R> cr = CallbackResult.createFromFuture(futures.get(i));
      L listener = listeners.get(i);
      if (cr.isCanceled()) {
        _log.warn("Callback cancelled: " + callbacks.get(i) + " on " + listener);
        res.cancellations.put(listener, cr);
      }
      else if (cr.hasFailed()) {
        _log.error("Callback error: " + callbacks.get(i) + " on " + listener + ":" + cr.getError());
        res.failures.put(listener, cr);
      }
      else {
        res.successes.put(listener, cr);
      }
    }

    return res;
  }

  @AllArgsConstructor
  public class CallbackCallable<R> implements Callable<R> {
    final Function<? super L, R> _callback;
    final L _listener;

    @Override public R call() throws Exception {
      _log.info("Calling " + _callback + " on " + _listener);
      return _callback.apply(_listener);
    }

  }

  @Data
  public static class CallbackResults<L, R> {
    private final Map<L, CallbackResult<R>> successes = new IdentityHashMap<>();
    private final Map<L, CallbackResult<R>> failures = new IdentityHashMap<>();
    private final Map<L, CallbackResult<R>> cancellations = new IdentityHashMap<>();
  }

}
