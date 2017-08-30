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

package org.apache.gobblin.util.limiter;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


public class MultiLimiterTest {

  @Test
  public void test() throws Exception {

    CountBasedLimiter countLimiter1 = new CountBasedLimiter(3);
    CountBasedLimiter countLimiter2 = new CountBasedLimiter(1);

    MultiLimiter multiLimiter = new MultiLimiter(countLimiter1, countLimiter2);

    // Can only take 1 permit (limiter2 has only 1 permit available)
    Assert.assertNotNull(multiLimiter.acquirePermits(1));
    Assert.assertNull(multiLimiter.acquirePermits(1));

    // limiter1 has 1 leftover permit (one consumed in the failed second permit above)
    Assert.assertNotNull(countLimiter1.acquirePermits(1));
    Assert.assertNull(countLimiter1.acquirePermits(1));

    // limiter2 has not leftover permits
    Assert.assertNull(countLimiter2.acquirePermits(1));
  }

  public void testConstructor() throws Exception {

    CountBasedLimiter countLimiter1 = new CountBasedLimiter(3);
    CountBasedLimiter countLimiter2 = new CountBasedLimiter(3);
    CountBasedLimiter countLimiter3 = new CountBasedLimiter(3);
    NoopLimiter noopLimiter = new NoopLimiter();

    MultiLimiter multiLimiter1 = new MultiLimiter(countLimiter1, countLimiter2);
    Assert.assertEquals(multiLimiter1.getUnderlyingLimiters(), Lists.newArrayList(countLimiter1, countLimiter2));

    // Noop limiters get filtered
    MultiLimiter multiLimiter2 = new MultiLimiter(countLimiter1, noopLimiter);
    Assert.assertEquals(multiLimiter2.getUnderlyingLimiters(), Lists.newArrayList(countLimiter1));

    // multilimiters get expanded
    MultiLimiter multiLimiter3 = new MultiLimiter(multiLimiter1, countLimiter3);
    Assert.assertEquals(multiLimiter3.getUnderlyingLimiters(), Lists.newArrayList(countLimiter1, countLimiter2, countLimiter3));

    // deduplication
    MultiLimiter multiLimiter4 = new MultiLimiter(countLimiter1, countLimiter1);
    Assert.assertEquals(multiLimiter4.getUnderlyingLimiters(), Lists.newArrayList(countLimiter1));

    // deduplication on expanded multilimiters
    MultiLimiter multiLimiter5 = new MultiLimiter(multiLimiter1, countLimiter1);
    Assert.assertEquals(multiLimiter5.getUnderlyingLimiters(), Lists.newArrayList(countLimiter1, countLimiter2));
  }

}