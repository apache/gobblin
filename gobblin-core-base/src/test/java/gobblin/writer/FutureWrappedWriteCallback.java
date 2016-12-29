/*
 *
 *  * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  * this file except in compliance with the License. You may obtain a copy of the
 *  * License at  http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed
 *  * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  * CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package gobblin.writer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;


/**
 * A helper class that makes it easy to implement a future that is updated by a callback.
 * TODO: Figure out what this is really useful for... write tests.
 *
 */
public class FutureWrappedWriteCallback implements WriteCallback, Future<WriteResponse> {
  private WriteCallback _innerCallback;
  private WriteResponse _writeResponse;
  private Throwable _throwable;
  private Condition _callback;
  private final Object _lock;
  private volatile boolean _callbackFired;


  public FutureWrappedWriteCallback(WriteCallback innerCallback) {
    _writeResponse = null;
    _throwable = null;
    _lock = new Object();
    _innerCallback = innerCallback;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return _callbackFired;
  }

  @Override
  public WriteResponse get()
      throws InterruptedException, ExecutionException {
    try {
      return get(-1, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new ExecutionException(e);
    }
  }

  @Override
  public WriteResponse get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    synchronized (_lock)
    {
      while (!_callbackFired)
      {
        _lock.wait(timeout);
      }
    }
    if (_callbackFired)
    {
      if (_throwable != null)
      {
        throw new ExecutionException(_throwable);
      }
      else
      {
        return _writeResponse;
      }
    }
    else
    {
      //TODO: umm...
      return null;
    }
  }


  @Override
  public void onSuccess(WriteResponse writeResponse) {
    synchronized (_lock) {
      _writeResponse = writeResponse;
      _callbackFired = true;
      if (_innerCallback != null)
      {
        _innerCallback.onSuccess(writeResponse);
      }
      _lock.notify();
    }
  }

  @Override
  public void onFailure(Throwable throwable) {
    synchronized (_lock) {
      _throwable = throwable;
      _callbackFired = true;
      if (_innerCallback!= null)
      {
        _innerCallback.onFailure(throwable);
      }
      _lock.notify();
    }
  }
}
