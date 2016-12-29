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


/**
 * Wraps a Future<InnerType> and implements a Future<WriteResponse> when provided with a wrapper class
 * @param <InnerType>
 */
public class WriteResponseFuture<InnerType> implements Future<WriteResponse> {

  private final Future<InnerType> _innerFuture;
  private final WriteResponseMapper<InnerType> _writeResponseMapper;

  public WriteResponseFuture(Future<InnerType> innerFuture, WriteResponseMapper<InnerType> writeResponseMapper)
  {
    _innerFuture = innerFuture;
    _writeResponseMapper = writeResponseMapper;
  }


  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return _innerFuture.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled() {
    return _innerFuture.isCancelled();
  }

  @Override
  public boolean isDone() {
    return _innerFuture.isDone();
  }

  @Override
  public WriteResponse get()
      throws InterruptedException, ExecutionException {
    return _writeResponseMapper.wrap(_innerFuture.get());
  }

  @Override
  public WriteResponse get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return _writeResponseMapper.wrap(_innerFuture.get(timeout, unit));
  }
}
