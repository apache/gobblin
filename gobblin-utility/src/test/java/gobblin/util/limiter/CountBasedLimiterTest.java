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

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.util.limiter.CountBasedLimiter;
import gobblin.util.limiter.Limiter;


/**
 * Unit tests for {@link CountBasedLimiter}.
 *
 * @author ynli
 */
@Test(groups = { "gobblin.util.limiter" })
public class CountBasedLimiterTest {

  @Test
  public void testThrottling() throws InterruptedException {
    Limiter limiter = new CountBasedLimiter(10);
    limiter.start();

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(limiter.acquirePermits(1) != null);
    }
    Assert.assertTrue(limiter.acquirePermits(1) == null);

    limiter.stop();
  }
}
