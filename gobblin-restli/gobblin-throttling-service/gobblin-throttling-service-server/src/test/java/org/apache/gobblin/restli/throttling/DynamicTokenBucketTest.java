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

import org.testng.Assert;
import org.testng.annotations.Test;


public class DynamicTokenBucketTest {

  @Test
  public void test() throws Exception {
    int qps = 10;
    DynamicTokenBucket limiter = new DynamicTokenBucket(qps, 10, 0);

    // Requesting 10 seconds worth of permits with 1 second timeout fails
    Assert.assertEquals(limiter.getPermits(10 * qps, 10 * qps, 1000), 0);
    // Requesting 0.2 seconds worth of permits with 300 millis timeout succeeds
    long permits = qps / 5;
    Assert.assertEquals(limiter.getPermits(permits, permits, 300), permits);
    // Requesting 1 seconds worth of permits, with min of 0.1 seconds, and with 200 millis timeout will return at least
    // min permits
    permits = qps;
    Assert.assertTrue(limiter.getPermits(permits, qps / 10, 200) >= qps / 10);
  }

  @Test
  public void testEagerGrantingIfUnderused() throws Exception {
    int qps = 100;
    DynamicTokenBucket limiter = new DynamicTokenBucket(qps, 10, 100);

    Thread.sleep(100); // fill bucket
    // Grant 4 permits even though only 1 requested
    Assert.assertTrue(limiter.getPermits(1, 0, 100) > 4);
  }

}
