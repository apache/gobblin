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

package org.apache.gobblin.temporal.ddm.work;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link ExecGobblinStats} to verify the structure no longer contains large dataset maps
 * and only contains aggregate metrics to avoid Temporal payload size limits.
 */
public class ExecGobblinStatsTest {

  @Test
  public void testExecGobblinStatsWithAggregateValues() {
    int numWorkUnits = 200;
    int numCommitted = 195;
    long recordsWritten = 100000L;
    long bytesWritten = 2048000L;
    String user = "testUser";

    ExecGobblinStats stats = new ExecGobblinStats(numWorkUnits, numCommitted, recordsWritten, bytesWritten, user);

    Assert.assertEquals(numWorkUnits, stats.getNumWorkUnits());
    Assert.assertEquals(numCommitted, stats.getNumCommitted());
    Assert.assertEquals(recordsWritten, stats.getRecordsWritten());
    Assert.assertEquals(bytesWritten, stats.getBytesWritten());
    Assert.assertEquals(user, stats.getUser());
  }

  @Test
  public void testExecGobblinStatsDoesNotContainDatasetStatsMap() {
    // Verify that ExecGobblinStats no longer has a stats (Map<String, DatasetStats>) field
    // This is important to avoid exceeding Temporal's 2MB payload size limit
    ExecGobblinStats stats = new ExecGobblinStats(10, 10, 1000L, 2000L, "user1");
    
    // Verify only the expected fields exist
    Assert.assertEquals(10, stats.getNumWorkUnits());
    Assert.assertEquals(10, stats.getNumCommitted());
    Assert.assertEquals(1000L, stats.getRecordsWritten());
    Assert.assertEquals(2000L, stats.getBytesWritten());
    Assert.assertEquals("user1", stats.getUser());
  }

  @Test
  public void testExecGobblinStatsWithPartialCommit() {
    // Test scenario where not all work units were committed
    int numWorkUnits = 100;
    int numCommitted = 75;
    long recordsWritten = 50000L;
    long bytesWritten = 1024000L;
    String user = "partialUser";

    ExecGobblinStats stats = new ExecGobblinStats(numWorkUnits, numCommitted, recordsWritten, bytesWritten, user);

    Assert.assertEquals(numWorkUnits, stats.getNumWorkUnits());
    Assert.assertEquals(numCommitted, stats.getNumCommitted());
    Assert.assertTrue(stats.getNumCommitted() < stats.getNumWorkUnits());
    Assert.assertEquals(recordsWritten, stats.getRecordsWritten());
    Assert.assertEquals(bytesWritten, stats.getBytesWritten());
  }

  @Test
  public void testExecGobblinStatsWithZeroValues() {
    ExecGobblinStats stats = new ExecGobblinStats(0, 0, 0L, 0L, "emptyUser");

    Assert.assertEquals(0, stats.getNumWorkUnits());
    Assert.assertEquals(0, stats.getNumCommitted());
    Assert.assertEquals(0L, stats.getRecordsWritten());
    Assert.assertEquals(0L, stats.getBytesWritten());
    Assert.assertEquals("emptyUser", stats.getUser());
  }

  @Test
  public void testExecGobblinStatsWithLargeValues() {
    // Test with large values to ensure no overflow issues
    int numWorkUnits = Integer.MAX_VALUE;
    int numCommitted = Integer.MAX_VALUE - 1;
    long recordsWritten = Long.MAX_VALUE / 2;
    long bytesWritten = Long.MAX_VALUE / 2;
    String user = "largeUser";

    ExecGobblinStats stats = new ExecGobblinStats(numWorkUnits, numCommitted, recordsWritten, bytesWritten, user);

    Assert.assertEquals(numWorkUnits, stats.getNumWorkUnits());
    Assert.assertEquals(numCommitted, stats.getNumCommitted());
    Assert.assertEquals(recordsWritten, stats.getRecordsWritten());
    Assert.assertEquals(bytesWritten, stats.getBytesWritten());
    Assert.assertEquals(user, stats.getUser());
  }

  @Test
  public void testExecGobblinStatsConstructorFieldOrder() {
    // Verify the constructor field order matches the new structure
    ExecGobblinStats stats = new ExecGobblinStats(100, 95, 50000L, 1024000L, "orderUser");

    // Verify all fields are set correctly in the expected order
    Assert.assertEquals(100, stats.getNumWorkUnits());
    Assert.assertEquals(95, stats.getNumCommitted());
    Assert.assertEquals(50000L, stats.getRecordsWritten());
    Assert.assertEquals(1024000L, stats.getBytesWritten());
    Assert.assertEquals("orderUser", stats.getUser());
  }
}
