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
package org.apache.gobblin.configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StateTest {
  private LinkedBlockingQueue<Throwable> exceptions = new LinkedBlockingQueue<>();

  /**
   * This test checks that state object is thread safe. We run 2 threads, one of them continuously adds and removes key/values
   * to the state and other thread calls getProperties.
   */
  @Test
  public void testGetPropertiesThreadSafety() {
    try {
      final State state = new State();
      for (int i = 0; i < 1000; i++) {
        state.setProp(Integer.toString(i), Integer.toString(i));
      }
      ExecutorService executorService = Executors.newFixedThreadPool(2);

      executorService.submit(new Runnable() {
        @Override
        public void run() {
          for (int j = 0; j < 1000; j++) {
            for (int i = 0; i < 1000; i++) {
              try {
                state.removeProp(Integer.toString(i));
                state.setProp(Integer.toString(i), Integer.toString(i));
              } catch (Throwable t) {
                exceptions.add(t);
              }
            }
          }
        }
      });

      executorService.submit(new Runnable() {
        @Override
        public void run() {
          for (int i = 0; i < 1000; i++) {
            try {
              state.getProperties().get(Integer.toString(i));
            } catch (Throwable t) {
              exceptions.add(t);
            }
          }
        }
      });

      executorService.shutdown();
      if (!executorService.awaitTermination(100, TimeUnit.SECONDS)) {
        throw new RuntimeException("Executor service still running");
      }
    } catch (Throwable t) {
      Assert.fail("Concurrency test failed", t);
    }

    if (!this.exceptions.isEmpty()) {
      Assert.fail("Concurrency test failed with first exception: " + ExceptionUtils.getFullStackTrace(this.exceptions.poll()));
    }
  }

  @Test
  public void testRemovePropsWithPrefix() {
    final State state = new State();
    final String prefix = "prefix";
    for (int i = 0; i < 10; i++) {
      state.setProp("prefix." + i, i);
    }
    Assert.assertTrue(state.getPropertyNames().size() == 10);
    state.removePropsWithPrefix(prefix);
    Assert.assertTrue(state.getPropertyNames().size() == 0);
  }
}