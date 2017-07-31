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

package org.apache.gobblin.util.limiter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;

import com.google.common.collect.Queues;
import com.linkedin.common.callback.Callback;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.RestLiServiceException;

import org.apache.gobblin.restli.throttling.LimiterServerResource;
import org.apache.gobblin.restli.throttling.PermitAllocation;
import org.apache.gobblin.restli.throttling.PermitRequest;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;


/**
 * A mock {@link RestClientRequestSender} that satisfies requests using an embedded
 * {@link LimiterServerResource}.
 */
@Slf4j
public class MockRequester implements RequestSender {

  private final BlockingQueue<RequestAndCallback> requestAndCallbackQueue;

  private final LimiterServerResource limiterServer;
  private final long latencyMillis;
  private final int requestHandlerThreads;

  private boolean started;
  private ExecutorService handlerExecutorService;

  public MockRequester(LimiterServerResource limiterServer, long latencyMillis, int requestHandlerThreads) {
    this.limiterServer = limiterServer;
    this.latencyMillis = latencyMillis;
    this.requestHandlerThreads = requestHandlerThreads;
    this.requestAndCallbackQueue = Queues.newLinkedBlockingQueue();
  }

  public synchronized void start() {
    if (this.started) {
      return;
    }
    this.started = true;
    this.handlerExecutorService = Executors.newFixedThreadPool(this.requestHandlerThreads);
    for (int i = 0; i < this.requestHandlerThreads; i++) {
      this.handlerExecutorService.submit(new RequestHandler());
    }
  }

  public synchronized void stop() {
    if (!this.started) {
      return;
    }
    this.handlerExecutorService.shutdownNow();
    this.started = false;
  }

  @Override
  public void sendRequest(PermitRequest request, Callback<Response<PermitAllocation>> callback) {
    if (!this.started) {
      throw new IllegalStateException(MockRequester.class.getSimpleName() + " has not been started.");
    }
    long nanoTime = System.nanoTime();
    long satisfyAt = nanoTime + TimeUnit.MILLISECONDS.toNanos(this.latencyMillis);
    this.requestAndCallbackQueue.add(new RequestAndCallback(request, callback, satisfyAt));
  }

  @Data
  public static class RequestAndCallback {
    private final PermitRequest request;
    private final Callback<Response<PermitAllocation>> callback;
    private final long processAfterNanos;
  }

  private class RequestHandler implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          RequestAndCallback requestAndCallback = MockRequester.this.requestAndCallbackQueue.take();

          long nanoTime = System.nanoTime();
          long delayNanos = requestAndCallback.getProcessAfterNanos() - nanoTime;
          if (delayNanos > 0) {
            Thread.sleep(TimeUnit.NANOSECONDS.toMillis(delayNanos));
          }

          try {
            PermitAllocation allocation =
                MockRequester.this.limiterServer.getSync(new ComplexResourceKey<>(requestAndCallback.getRequest(), new EmptyRecord()));

            Response<PermitAllocation> response = Mockito.mock(Response.class);
            Mockito.when(response.getEntity()).thenReturn(allocation);

            requestAndCallback.getCallback().onSuccess(response);

          } catch (RestLiServiceException rexc) {
            RestLiResponseException returnException = Mockito.mock(RestLiResponseException.class);
            Mockito.when(returnException.getStatus()).thenReturn(rexc.getStatus().getCode());
            requestAndCallback.getCallback().onError(returnException);
          }
        }
      } catch (Throwable t) {
        log.error("Error", t);
        throw new RuntimeException(t);
      }
    }
  }

}
