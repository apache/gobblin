/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.writer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.extern.slf4j.Slf4j;


/**
 * A helper class that makes it easy to implement a future that is updated by a callback.
 *
 */
@Slf4j
public class FutureWrappedWriteCallback implements WriteCallback<Object>, Future<WriteResponse> {
  private WriteCallback _innerCallback;
  private WriteResponse _writeResponse;
  private Throwable _throwable;
  private volatile boolean _callbackFired;

  public FutureWrappedWriteCallback(WriteCallback innerCallback) {
    _writeResponse = null;
    _throwable = null;
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
      return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new ExecutionException(e);
    }
  }

  @Override
  public WriteResponse get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    synchronized (this) {
      while (!_callbackFired) {
        wait(timeout);
      }
    }
    if (_callbackFired) {
      if (_throwable != null) {
        throw new ExecutionException(_throwable);
      } else {
        return _writeResponse;
      }
    } else {
      throw new AssertionError("Should not be here if _callbackFired is behaving well");
    }
  }

  @Override
  public void onSuccess(WriteResponse writeResponse) {
    _writeResponse = writeResponse;
    synchronized (this) {
      _callbackFired = true;
      if (_innerCallback != null) {
        try {
          _innerCallback.onSuccess(writeResponse);
        } catch (Exception e) {
          log.error("Ignoring error thrown in callback", e);
        }
      }
      notifyAll();
    }
  }

  @Override
  public void onFailure(Throwable throwable) {
    synchronized (this) {
      _throwable = throwable;
      _callbackFired = true;
      if (_innerCallback != null) {
        _innerCallback.onFailure(throwable);
      }
      notifyAll();
    }
  }
}
