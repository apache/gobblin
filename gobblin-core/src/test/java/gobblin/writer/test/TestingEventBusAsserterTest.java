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
package gobblin.writer.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;

/**
 * Unit tests for {@link TestingEventBusAsserter}
 */
public class TestingEventBusAsserterTest {

  @Test
  public void testAssertNext() throws InterruptedException, TimeoutException, IOException {
    EventBus testBus = TestingEventBuses.getEventBus("TestingEventBusAsserterTest.testHappyPath");

    try(final TestingEventBusAsserter asserter =
        new TestingEventBusAsserter("TestingEventBusAsserterTest.testHappyPath")) {
      testBus.post(new TestingEventBuses.Event("event1"));
      testBus.post(new TestingEventBuses.Event("event2"));

      asserter.assertNextValueEq("event1");
      Assert.assertThrows(new ThrowingRunnable() {
        @Override public void run() throws Throwable {
          asserter.assertNextValueEq("event3");
        }
      });

      testBus.post(new TestingEventBuses.Event("event13"));
      testBus.post(new TestingEventBuses.Event("event11"));
      testBus.post(new TestingEventBuses.Event("event12"));
      testBus.post(new TestingEventBuses.Event("event10"));

      asserter.assertNextValuesEq(Arrays.asList("event10", "event11", "event12", "event13"));

      testBus.post(new TestingEventBuses.Event("event22"));
      testBus.post(new TestingEventBuses.Event("event20"));

      Assert.assertThrows(new ThrowingRunnable() {
        @Override public void run() throws Throwable {
          asserter.assertNextValuesEq(Arrays.asList("event22", "event21"));
        }
      });
    }
  }

  @Test
  public void testTimeout() throws InterruptedException, TimeoutException, IOException {
    try(final TestingEventBusAsserter asserter =
        new TestingEventBusAsserter("TestingEventBusAsserterTest.testTimeout")) {
      final CountDownLatch timeoutSeen  = new CountDownLatch(1);
      Thread assertThread = new Thread(new Runnable() {
        @Override public void run() {
          try {
            asserter.withTimeout(300, TimeUnit.MILLISECONDS).assertNextValueEq("event1");
          } catch (TimeoutException e) {
            timeoutSeen.countDown();
          }
          catch (InterruptedException e ) {
            // Unexpected
          }
        }
      }, "TestingEventBusAsserterTest.testTimeout.assertThread");
      assertThread.start();
      Thread.sleep(100);
      Assert.assertTrue(assertThread.isAlive());
      Assert.assertTrue(timeoutSeen.await(300, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  public void testAssertNextValuesEqTimeout() throws InterruptedException, TimeoutException, IOException {
    EventBus testBus = TestingEventBuses.getEventBus("TestingEventBusAsserterTest.testAssertNextValuesEqTimeout");

    try(final TestingEventBusAsserter asserter =
        new TestingEventBusAsserter("TestingEventBusAsserterTest.testAssertNextValuesEqTimeout")) {
      testBus.post(new TestingEventBuses.Event("event1"));

      final CountDownLatch timeoutSeen  = new CountDownLatch(1);
      Thread assertThread = new Thread(new Runnable() {
        @Override public void run() {
          try {
            asserter.withTimeout(300, TimeUnit.MILLISECONDS)
                    .assertNextValuesEq(Arrays.asList("event1", "event2"));
          } catch (TimeoutException e) {
            timeoutSeen.countDown();
          }
          catch (InterruptedException e ) {
            // Unexpected
          }
        }
      }, "TestingEventBusAsserterTest.testTimeout.assertThread");
      assertThread.start();
      Thread.sleep(100);
      Assert.assertTrue(assertThread.isAlive());
      Assert.assertTrue(timeoutSeen.await(300, TimeUnit.MILLISECONDS));
    }
  }

}
