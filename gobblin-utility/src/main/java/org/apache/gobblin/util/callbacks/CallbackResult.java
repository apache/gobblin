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
package org.apache.gobblin.util.callbacks;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * A helper class to simplify the access the result of a callback (if any)
 */
@Data
@AllArgsConstructor
public class CallbackResult<R> {
  private final R result;
  private final Throwable error;
  private final boolean canceled;

  public boolean hasFailed() {
    return null != this.error;
  }

  public boolean isSuccessful() {
    return !hasFailed() && !isCanceled();
  }

  public static <R> CallbackResult<R> createCancelled() {
    return new CallbackResult<R>(null, null, true);
  }

  public static <R> CallbackResult<R> createFailed(Throwable t) {
    return new CallbackResult<R>(null, t, false);
  }

  public static <R> CallbackResult<R> createSuccessful(R result) {
    return new CallbackResult<R>(result, null, false);
  }

  public static <R> CallbackResult<R> createFromFuture(Future<R> execFuture)
          throws InterruptedException {
    Preconditions.checkNotNull(execFuture);
    if (execFuture.isCancelled()) {
      return createCancelled();
    }
    try {
      R res = execFuture.get();
      return createSuccessful(res);
    }
    catch (ExecutionException e) {
      if (execFuture.isCancelled()) {
        return createCancelled();
      }
      else {
        return createFailed(e.getCause());
      }
    }
  }
}
