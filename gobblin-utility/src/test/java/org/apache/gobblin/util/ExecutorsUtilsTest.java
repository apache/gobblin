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

package org.apache.gobblin.util;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;


/**
 * Unit tests for {@link ExecutorsUtils}.
 *
 * @author Yinan Li
 */
@Test(groups = {"gobblin.util"})
public class ExecutorsUtilsTest {

  @Test(expectedExceptions = RuntimeException.class)
  public void testNewThreadFactory() throws InterruptedException {
    Logger logger = Mockito.mock(Logger.class);

    ThreadFactory threadFactory = ExecutorsUtils.newThreadFactory(Optional.of(logger));
    final RuntimeException runtimeException = new RuntimeException();
    Thread thread = threadFactory.newThread(new Runnable() {
      @Override
      public void run() {
        throw runtimeException;
      }
    });
    thread.setName("foo");
    String errorMessage = String.format("Thread %s threw an uncaught exception: %s", thread, runtimeException);
    Mockito.doThrow(runtimeException).when(logger).error(errorMessage, runtimeException);

    thread.run();
  }

  /**
   * Test to verify that {@link ExecutorsUtils#parallelize(List, Function, int, int, Optional)} returns the result in
   * the same order as the input
   *
   */
  @Test
  public void testParallelize() throws Exception {
    List<Integer> nums = ImmutableList.of(3, 5, 10, 5, 20);
    final int factor = 5;

    Function<Integer, String> multiply = new Function<Integer, String>() {
      @Override
      public String apply(Integer input) {
        return Integer.toString(input * factor);
      }
    };

    List<String> result = ExecutorsUtils.parallelize(nums, multiply, 2, 60, Optional.<Logger> absent());
    Assert.assertEquals(Arrays.asList("15", "25", "50", "25", "100"), result);
  }

  /**
   * Test to verify that {@link ExecutorsUtils#parallelize(List, Function, int, int, Optional)} throws
   * {@link ExecutionException} when any of the threads throw and exception
   */
  @Test(expectedExceptions = ExecutionException.class)
  public void testParallelizeException() throws Exception {
    List<Integer> nums = ImmutableList.of(3, 5);
    final int factor = 5;

    Function<Integer, String> exceptionFunction = new Function<Integer, String>() {
      @Override
      public String apply(Integer input) {
        if (input == 3) {
          throw new RuntimeException("testParallelizeException thrown for testing");
        }
        return Integer.toString(input * factor);
      }
    };

    ExecutorsUtils.parallelize(nums, exceptionFunction, 2, 1, Optional.<Logger> absent());

  }

  /**
   * Test to verify that {@link ExecutorsUtils#parallelize(List, Function, int, int, Optional)} throws
   * {@link ExecutionException} when any of the threads timesout.
   */
  @Test(expectedExceptions = ExecutionException.class)
  public void testParallelizeTimeout() throws Exception {
    List<Integer> nums = ImmutableList.of(3, 5);
    final int factor = 5;

    Function<Integer, String> sleepAndMultiply = new Function<Integer, String>() {
      @Override
      public String apply(Integer input) {
        try {
          if (input == 5) {
            TimeUnit.SECONDS.sleep(2);
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return Integer.toString(input * factor);
      }
    };

    ExecutorsUtils.parallelize(nums, sleepAndMultiply, 2, 1, Optional.<Logger> absent());
  }
}
