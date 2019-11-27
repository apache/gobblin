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
package org.apache.gobblin.testing;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

/** Unit tests for {@link AssertWithBackoff} */
public class AssertWithBackoffTest {

  @BeforeClass
  public void setUp() {
    BasicConfigurator.configure();
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR);
  }

  @Test
  public void testComputeRetrySleep() {
    final long infiniteFutureMs = Long.MAX_VALUE;
    Assert.assertEquals(AssertWithBackoff.computeRetrySleep(0, 2, 100, infiniteFutureMs), 1);
    Assert.assertEquals(AssertWithBackoff.computeRetrySleep(10, 2, 100, infiniteFutureMs), 20);
    Assert.assertEquals(AssertWithBackoff.computeRetrySleep(50, 1, 100, infiniteFutureMs), 51);
    Assert.assertEquals(AssertWithBackoff.computeRetrySleep(50, 3, 100, infiniteFutureMs), 100);
    Assert.assertEquals(AssertWithBackoff.computeRetrySleep(50, 3, 60, System.currentTimeMillis() + 100), 60);
    long sleepMs = AssertWithBackoff.computeRetrySleep(50, 3, 1000, System.currentTimeMillis() + 100);
    Assert.assertTrue(sleepMs <= 100);
  }

  @Test
  public void testAssertWithBackoff_conditionTrue() throws Exception {
    Logger log = LoggerFactory.getLogger("testAssertWithBackoff_conditionTrue");
    AssertWithBackoff.create().logger(log).timeoutMs(1000)
        .assertTrue(Predicates.<Void>alwaysTrue(), "should always succeed");
  }

  @Test
  public void testAssertWithBackoff_conditionEventuallyTrue() throws Exception {
    Logger log = LoggerFactory.getLogger("testAssertWithBackoff_conditionEventuallyTrue");
    setLogjLevelForLogger(log, Level.ERROR);
    final AtomicInteger cnt = new AtomicInteger();
    AssertWithBackoff.create().logger(log).timeoutMs(100000).backoffFactor(2.0)
        .assertEquals(new Function<Void, Integer>() {
          @Override public Integer apply(Void input) { return cnt.incrementAndGet(); }
        }, 5, "should eventually succeed");
  }

  @Test
  public void testAssertWithBackoff_conditionFalse() throws Exception {
    Logger log = LoggerFactory.getLogger("testAssertWithBackoff_conditionFalse");
    setLogjLevelForLogger(log, Level.ERROR);
    long startTimeMs = System.currentTimeMillis();
    try {
      AssertWithBackoff.create().logger(log).timeoutMs(50)
         .assertTrue(Predicates.<Void>alwaysFalse(), "should timeout");
      Assert.fail("TimeoutException expected");
    } catch (TimeoutException e) {
      //Expected
    }
    long durationMs = System.currentTimeMillis() - startTimeMs;
    log.debug("assert took " + durationMs + "ms");
    Assert.assertTrue(durationMs >= 50L, Long.toString(durationMs) + ">= 50ms");
  }

  @Test
  public void testAssertWithBackoff_RuntimeException() throws Exception {
    Logger log = LoggerFactory.getLogger("testAssertWithBackoff_RuntimeException");
    setLogjLevelForLogger(log, Level.ERROR);
    try {
      AssertWithBackoff.create().logger(log).timeoutMs(50)
        .assertTrue(new Predicate<Void>() {
          @Override public boolean apply(Void input) { throw new RuntimeException("BLAH"); }
        }, "should throw RuntimeException");
      Assert.fail("should throw RuntimeException");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().indexOf("BLAH") > 0, e.getMessage());
    }
  }

  public static void setLogjLevelForLogger(Logger log, Level logLevel) {
    org.apache.log4j.Logger log4jLogger = org.apache.log4j.Logger.getLogger(log.getName());
    log4jLogger.setLevel(logLevel);
  }

}
