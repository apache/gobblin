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

package org.apache.gobblin.restli.throttling;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import lombok.AllArgsConstructor;


public class TokenBucketTest {

  @Test
  public void testSmallQps() throws Exception {
    testForQps(100);
  }

  @Test
  public void testLargeQps() throws Exception {
    testForQps((long) 1e10);
  }

  @Test
  public void testTimeout() throws Exception {
    TokenBucket tokenBucket = new TokenBucket(100, 0);

    // If it cannot satisfy the request within the timeout, return false immediately
    Assert.assertFalse(tokenBucket.getTokens(100, 1, TimeUnit.MILLISECONDS));
    Assert.assertFalse(tokenBucket.getTokens(100, 10, TimeUnit.MILLISECONDS));
    Assert.assertFalse(tokenBucket.getTokens(100, 100, TimeUnit.MILLISECONDS));
    Assert.assertTrue(tokenBucket.getTokens(10, 101, TimeUnit.MILLISECONDS));

    // Can use stored tokens to satisfy request
    tokenBucket = new TokenBucket(100, 100);
    Thread.sleep(200); // fill up bucket
    Assert.assertTrue(tokenBucket.getTokens(20, 101, TimeUnit.MILLISECONDS));
  }

  private void testForQps(long qps) throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(10);

    TokenBucket tokenBucket = new TokenBucket(qps, 1000);

    List<Future<Boolean>> futures = Lists.newArrayList();

    long permitsPerRequest = qps / 10;

    long start = System.currentTimeMillis();
    futures.add(executorService.submit(new MyRunnable(tokenBucket, permitsPerRequest, 1000)));
    futures.add(executorService.submit(new MyRunnable(tokenBucket, permitsPerRequest, 1000)));
    futures.add(executorService.submit(new MyRunnable(tokenBucket, permitsPerRequest, 1000)));
    futures.add(executorService.submit(new MyRunnable(tokenBucket, permitsPerRequest, 1000)));
    futures.add(executorService.submit(new MyRunnable(tokenBucket, permitsPerRequest, 1000)));
    futures.add(executorService.submit(new MyRunnable(tokenBucket, permitsPerRequest, 1000)));
    futures.add(executorService.submit(new MyRunnable(tokenBucket, permitsPerRequest, 1000)));
    futures.add(executorService.submit(new MyRunnable(tokenBucket, permitsPerRequest, 1000)));
    futures.add(executorService.submit(new MyRunnable(tokenBucket, permitsPerRequest, 1000)));
    futures.add(executorService.submit(new MyRunnable(tokenBucket, permitsPerRequest, 1000)));

    for (Future<Boolean> future : futures) {
      Assert.assertTrue(future.get());
    }
    long end = System.currentTimeMillis();

    double averageRate = 1000 * (double) (permitsPerRequest * futures.size()) / (end - start);

    Assert.assertTrue(Math.abs(averageRate - qps) / qps < 0.2, "Average rate: " + averageRate + " expected: 100");
  }

  @AllArgsConstructor
  public static class MyRunnable implements Callable<Boolean> {
    private final TokenBucket tokenBucket;
    private final long tokens;
    private final long timeoutMillis;

    @Override
    public Boolean call() {
      try {
        return this.tokenBucket.getTokens(this.tokens, this.timeoutMillis, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
  }
}
