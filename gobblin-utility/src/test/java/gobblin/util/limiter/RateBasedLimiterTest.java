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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.Meter;
import com.google.common.math.DoubleMath;

import gobblin.util.limiter.Limiter;
import gobblin.util.limiter.RateBasedLimiter;


/**
 * Unit tests for {@link RateBasedLimiter}.
 *
 * @author ynli
 */
@Test(groups = { "gobblin.util.limiter" })
public class RateBasedLimiterTest {

  private static final Random RANDOM = new Random();

  private Limiter limiter;

  @BeforeClass
  public void setUp() {
    this.limiter = new RateBasedLimiter(20, TimeUnit.SECONDS);
    this.limiter.start();
  }

  @Test
  public void testThrottling() throws InterruptedException {
    Meter meter = new Meter();
    for (int i = 0; i < 1000; i++) {
      Assert.assertTrue(this.limiter.acquirePermits(1) != null);
      meter.mark();
      Thread.sleep((RANDOM.nextInt() & Integer.MAX_VALUE) % 10);
    }

    // Assert a fuzzy equal with 5% of tolerance
    Assert.assertTrue(DoubleMath.fuzzyEquals(meter.getMeanRate(), 20d, 20d * 0.05));
  }

  @AfterClass
  public void tearDown() {
    this.limiter.stop();
  }
}
