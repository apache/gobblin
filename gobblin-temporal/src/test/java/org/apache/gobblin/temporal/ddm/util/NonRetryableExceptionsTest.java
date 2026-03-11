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
package org.apache.gobblin.temporal.ddm.util;

import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;


public class NonRetryableExceptionsTest {

  @Test
  public void testMatchesQuotaExceededExceptions() {
    NSQuotaExceededException nsEx = new NSQuotaExceededException("ns quota exceeded");
    Assert.assertTrue(NonRetryableExceptions.matchNonRetryable(nsEx).isPresent());
    Assert.assertSame(NonRetryableExceptions.matchNonRetryable(nsEx).get(), nsEx);

    DSQuotaExceededException dsEx = new DSQuotaExceededException("ds quota exceeded");
    Assert.assertTrue(NonRetryableExceptions.matchNonRetryable(dsEx).isPresent());
    Assert.assertSame(NonRetryableExceptions.matchNonRetryable(dsEx).get(), dsEx);
  }

  @Test
  public void testDoesNotMatchArbitraryException() {
    Assert.assertFalse(NonRetryableExceptions.matchNonRetryable(new IOException("some error")).isPresent());
  }

  @Test
  public void testReturnsEmptyForNull() {
    Assert.assertFalse(NonRetryableExceptions.matchNonRetryable(null).isPresent());
  }
}
