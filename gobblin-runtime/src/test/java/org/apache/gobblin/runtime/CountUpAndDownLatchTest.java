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

package org.apache.gobblin.runtime;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.Test;

import lombok.AllArgsConstructor;


public class CountUpAndDownLatchTest {

  @Test
  public void test() throws Exception {

    ExecutorService executorService = null;
    try {
      executorService = Executors.newFixedThreadPool(1);

      CountUpAndDownLatch countUpAndDownLatch = new CountUpAndDownLatch(1);
      Future future = executorService.submit(new Waiter(countUpAndDownLatch));

      try {
        future.get(50, TimeUnit.MILLISECONDS);
        Assert.fail();
      } catch (TimeoutException te) {
        // Expected
      }

      countUpAndDownLatch.countUp();
      try {
        future.get(50, TimeUnit.MILLISECONDS);
        Assert.fail();
      } catch (TimeoutException te) {
        // Expected
      }

      countUpAndDownLatch.countDown();
      try {
        future.get(50, TimeUnit.MILLISECONDS);
        Assert.fail();
      } catch (TimeoutException te) {
        // Expected
      }

      countUpAndDownLatch.countDown();
      future.get(1, TimeUnit.SECONDS);
    } finally {
      if (executorService != null) {
        executorService.shutdownNow();
      }
    }
  }

  @AllArgsConstructor
  public static class Waiter implements Runnable {
    private final CountDownLatch countDownLatch;

    @Override
    public void run() {
      try {
        this.countDownLatch.await();
      } catch (InterruptedException ie) {
        throw new RuntimeException();
      }
    }
  }

}
