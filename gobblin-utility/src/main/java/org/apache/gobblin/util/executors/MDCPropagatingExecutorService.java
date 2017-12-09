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

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

public class MDCPropagatingExecutorService extends ForwardingListeningExecutorService {
  private ListeningExecutorService executorService;

  public MDCPropagatingExecutorService(ExecutorService executorService) {
    if (executorService instanceof ListeningExecutorService) {
      this.executorService = (ListeningExecutorService)executorService;
    } else {
      this.executorService = MoreExecutors.listeningDecorator(executorService);
    }
  }

  @Override
  protected ListeningExecutorService delegate() {
    return this.executorService;
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
}