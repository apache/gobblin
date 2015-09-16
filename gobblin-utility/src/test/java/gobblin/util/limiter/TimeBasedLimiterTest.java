/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.limiter;

import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.util.limiter.Limiter;
import gobblin.util.limiter.TimeBasedLimiter;


/**
 * Unit tests for {@link TimeBasedLimiter}.
 *
 * @author ynli
 */
@Test(groups = { "gobblin.util.limiter" })
public class TimeBasedLimiterTest {

  private Limiter limiter;

  @BeforeClass
  public void setUp() {
    this.limiter = new TimeBasedLimiter(3l, TimeUnit.SECONDS);
    this.limiter.start();
  }

  @Test
  public void testThrottling() throws InterruptedException {
    Assert.assertTrue(this.limiter.acquirePermits(1) != null);
    Thread.sleep(1000);
    Assert.assertTrue(this.limiter.acquirePermits(1) != null);
    Thread.sleep(1000);
    Assert.assertTrue(this.limiter.acquirePermits(1) != null);
    Thread.sleep(1100);
    Assert.assertTrue(this.limiter.acquirePermits(1) == null);
  }

  @AfterClass
  public void tearDown() {
    this.limiter.stop();
  }
}
