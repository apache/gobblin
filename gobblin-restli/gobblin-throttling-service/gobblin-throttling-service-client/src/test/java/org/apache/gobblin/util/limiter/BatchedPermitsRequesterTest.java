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

import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Queues;
import com.linkedin.common.callback.Callback;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.HttpStatus;

import org.apache.gobblin.restli.throttling.PermitAllocation;
import org.apache.gobblin.restli.throttling.PermitRequest;
import org.apache.gobblin.util.ExecutorsUtils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

public class BatchedPermitsRequesterTest {

  @Test
  public void testForwardingOfRequests() throws Exception {

    Queue<RequestAndCallback> queue = Queues.newArrayDeque();

    BatchedPermitsRequester container = BatchedPermitsRequester.builder().resourceId("resource")
        .requestorIdentifier("requestor").requestSender(new TestRequestSender(queue, false)).build();
    try (ParallelRequester requester = new ParallelRequester(container)) {

      Future<Boolean> future = requester.request(10);

      await(new QueueSize(queue, 1), 1000);
      Assert.assertEquals(queue.size(), 1);
      satisfyRequestBuilder().requestAndCallback(queue.poll()).satisfy();

      future.get(1, TimeUnit.SECONDS);
      Assert.assertTrue(future.isDone());
      Assert.assertTrue(future.get());
    }
  }

  @Test
  public void testNoMoreThanOneRequestAtATime() throws Exception {
    Queue<RequestAndCallback> queue = Queues.newArrayDeque();

    BatchedPermitsRequester container = BatchedPermitsRequester.builder().resourceId("resource")
        .requestorIdentifier("requestor").requestSender(new TestRequestSender(queue, false)).build();
    try (ParallelRequester requester = new ParallelRequester(container)) {

      Future<Boolean> future = requester.request(1);

      await(new QueueSize(queue, 1), 1000);
      Assert.assertEquals(queue.size(), 1);

      Future<Boolean> future2 = requester.request(2);
      Future<Boolean> future3 = requester.request(3);
      Future<Boolean> future4 = requester.request(4);
      Future<Boolean> future5 = requester.request(5);

      Thread.sleep(100);

      Assert.assertEquals(queue.size(), 1);
      satisfyRequestBuilder().requestAndCallback(queue.poll()).satisfy();

      future.get(1, TimeUnit.SECONDS);
      Assert.assertTrue(future.isDone());
      Assert.assertTrue(future.get());

      await(new QueueSize(queue, 1), 1000);
      Assert.assertEquals(queue.size(), 1);
      satisfyRequestBuilder().requestAndCallback(queue.poll()).satisfy();

      future2.get(1, TimeUnit.SECONDS);
      future3.get(1, TimeUnit.SECONDS);
      future4.get(1, TimeUnit.SECONDS);
      future5.get(1, TimeUnit.SECONDS);

      Assert.assertTrue(future2.get());
      Assert.assertTrue(future3.get());
      Assert.assertTrue(future4.get());
      Assert.assertTrue(future5.get());
    }

  }

  @Test
  public void testRetriableFail() throws Exception {
    Queue<RequestAndCallback> queue = Queues.newArrayDeque();

    BatchedPermitsRequester container = BatchedPermitsRequester.builder().resourceId("resource")
        .requestorIdentifier("requestor").requestSender(new TestRequestSender(queue, false)).build();
    try (ParallelRequester requester = new ParallelRequester(container)) {

      Future<Boolean> future = requester.request(10);

      for (int i = 0; i < BatchedPermitsRequester.MAX_RETRIES; i++) {
        // container will fail 5 times
        await(new QueueSize(queue, 1), 1000);
        Assert.assertFalse(future.isDone());
        failRequestBuilder().requestAndCallback(queue.poll()).fail();
      }

      // should return a failure
      Assert.assertFalse(future.get());
      // should not make any more request
      Assert.assertEquals(queue.size(), 0);
    }
  }

  @Test
  public void testNonRetriableFail() throws Exception {
    Queue<RequestAndCallback> queue = Queues.newArrayDeque();

    BatchedPermitsRequester container = BatchedPermitsRequester.builder().resourceId("resource")
        .requestorIdentifier("requestor").requestSender(new TestRequestSender(queue, false)).build();
    try (ParallelRequester requester = new ParallelRequester(container)) {

      Future<Boolean> future = requester.request(10);

      // container should only try request once
      await(new QueueSize(queue, 1), 1000);
      Assert.assertFalse(future.isDone());
      failRequestBuilder().requestAndCallback(queue.poll()).errorStatus(HttpStatus.S_422_UNPROCESSABLE_ENTITY).fail();

      Assert.assertFalse(future.get());
      Assert.assertEquals(queue.size(), 0);
    }
  }

  public static class TestRequestSender implements RequestSender {
    private final Queue<RequestAndCallback> requestAndCallbacks;
    private final boolean autoSatisfyRequests;

    public TestRequestSender(Queue<RequestAndCallback> requestAndCallbacks, boolean autoSatisfyRequests) {
      this.requestAndCallbacks = requestAndCallbacks;
      this.autoSatisfyRequests = autoSatisfyRequests;
    }

    @Override
    public void sendRequest(PermitRequest request, Callback<Response<PermitAllocation>> callback) {
      if (this.autoSatisfyRequests) {
        satisfyRequestBuilder().requestAndCallback(new RequestAndCallback(request, callback)).satisfy();
      } else {
        this.requestAndCallbacks.add(new RequestAndCallback(request, callback));
      }
    }
  }

  @Builder(builderMethodName = "satisfyRequestBuilder", buildMethodName = "satisfy")
  public static void satisfyRequest(RequestAndCallback requestAndCallback, long expiration) {
    PermitAllocation allocation = new PermitAllocation();
    allocation.setPermits(requestAndCallback.getRequest().getPermits());
    allocation.setExpiration(expiration > 0 ? expiration : Long.MAX_VALUE);

    Response<PermitAllocation> response = Mockito.mock(Response.class);
    Mockito.when(response.getEntity()).thenReturn(allocation);
    requestAndCallback.getCallback().onSuccess(response);
  }

  @Builder(builderMethodName = "failRequestBuilder", buildMethodName = "fail")
  public static void failRequest(RequestAndCallback requestAndCallback, Throwable exception, HttpStatus errorStatus) {
    Throwable actualException;
    if (errorStatus != null) {
      RestLiResponseException restException = Mockito.mock(RestLiResponseException.class);
      Mockito.when(restException.getStatus()).thenReturn(errorStatus.getCode());
      actualException = restException;
    } else if (exception != null) {
      actualException = exception;
    } else {
      actualException = new RuntimeException();
    }

    requestAndCallback.callback.onError(actualException);
  }

  @Data
  public static class RequestAndCallback {
    private final PermitRequest request;
    private final Callback<Response<PermitAllocation>> callback;
  }

  private static class ParallelRequester implements Closeable {
    private final BatchedPermitsRequester container;
    private final ExecutorService executorService;

    public ParallelRequester(BatchedPermitsRequester container) {
      this.container = container;
      this.executorService = Executors.newCachedThreadPool(ExecutorsUtils.newThreadFactory(Optional.<Logger>absent(), Optional.of("parallel-requester-%d")));
    }

    public Future<Boolean> request(final long permits) {
      return this.executorService.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return container.getPermits(permits);
        }
      });
    }

    @Override
    public void close() throws IOException {
      if (this.executorService != null) {
        this.executorService.shutdownNow();
      }
    }
  }

  private void await(Callable<Boolean> condition, int millis) throws Exception {
    while (!condition.call()) {
      millis -= 50;
      if (millis < 0) {
        throw new RuntimeException("Await failed");
      }
      Thread.sleep(50);
    }
  }

  @AllArgsConstructor
  private class QueueSize implements Callable<Boolean> {
    private final Queue queue;
    private final int size;

    @Override
    public Boolean call() throws Exception {
      return queue.size() == size;
    }
  }

}
