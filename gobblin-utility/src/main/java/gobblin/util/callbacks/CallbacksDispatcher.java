/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.util.callbacks;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
public class CallbacksDispatcher<L> {
  private final Logger _log;
  private final List<L> _listeners = new ArrayList<>();
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
    _execService = execService.isPresent() ? execService.get() :
        Executors.newSingleThreadExecutor(
            ExecutorsUtils.newThreadFactory(Optional.of(_log), Optional.of(_log.getName() + "-%d")));
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

  public synchronized List<L> getListeners() {
    // Clone to protect against adding/removing listeners while running callbacks
    return new ArrayList<>(_listeners);
  }

  public synchronized void addListener(L listener) {
    Preconditions.checkNotNull(listener);

    _log.info("Adding listener:" + listener);
    _listeners.add(listener);
  }

  public synchronized void removeListener(L listener) {
    Preconditions.checkNotNull(listener);

    _log.info("Removing listener:" + listener);
    _listeners.remove(listener);
  }

  public Logger getLog() {
    return _log;
  }
  public <R> CallbackResults<L, R> execCallbacks(Function<L, R> callback, L listener)
         throws InterruptedException {
    Preconditions.checkNotNull(listener);
    List<L> listenerList = new ArrayList<>(1);
    listenerList.add(listener);

    return execCallbacks(callback, listenerList);
  }

  public <R> CallbackResults<L, R> execCallbacks(Function<L, R> callback)
         throws InterruptedException {
    Preconditions.checkNotNull(callback);
    List<L> listeners = getListeners();

    return execCallbacks(callback, listeners);
  }

  private <R> CallbackResults<L, R> execCallbacks(Function<L, R> callback, List<L> listeners)
      throws InterruptedException {
    List<Callable<R>> callbacks = new ArrayList<>(listeners.size());
    for (L listener: listeners) {
      callbacks.add(new CallbackCallable<>(callback, listener));
    }

    List<Future<R>> futures = _execService.invokeAll(callbacks);
    CallbackResults<L, R> res = new CallbackResults<L, R>();
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
    final Function<L, R> _callback;
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
