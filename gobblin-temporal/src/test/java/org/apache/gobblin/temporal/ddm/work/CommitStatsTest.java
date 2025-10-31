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

import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.temporal.exception.FailedDatasetUrnsException;


/**
 * Tests for {@link CommitStats} to verify the structure no longer contains large dataset maps
 * and only contains aggregate metrics to avoid Temporal payload size limits.
 */
public class CommitStatsTest {

  @Test
  public void testCommitStatsWithAggregateValues() {
    int numCommittedWorkUnits = 100;
    long recordsWritten = 50000L;
    long bytesWritten = 1024000L;
    Optional<FailedDatasetUrnsException> optFailure = Optional.empty();

    CommitStats stats = new CommitStats(numCommittedWorkUnits, recordsWritten, bytesWritten, optFailure);

    Assert.assertEquals(numCommittedWorkUnits, stats.getNumCommittedWorkUnits());
    Assert.assertEquals(recordsWritten, stats.getRecordsWritten());
    Assert.assertEquals(bytesWritten, stats.getBytesWritten());
    Assert.assertFalse(stats.getOptFailure().isPresent());
  }

  @Test
  public void testCommitStatsWithFailure() {
    int numCommittedWorkUnits = 50;
    long recordsWritten = 25000L;
    long bytesWritten = 512000L;
    FailedDatasetUrnsException failure = new FailedDatasetUrnsException(java.util.Collections.singleton("dataset1"));
    Optional<FailedDatasetUrnsException> optFailure = Optional.of(failure);

    CommitStats stats = new CommitStats(numCommittedWorkUnits, recordsWritten, bytesWritten, optFailure);

    Assert.assertEquals(numCommittedWorkUnits, stats.getNumCommittedWorkUnits());
    Assert.assertEquals(recordsWritten, stats.getRecordsWritten());
    Assert.assertEquals(bytesWritten, stats.getBytesWritten());
    Assert.assertTrue(stats.getOptFailure().isPresent());
    Assert.assertEquals(failure, stats.getOptFailure().get());
  }

  @Test
  public void testCreateEmpty() {
    CommitStats emptyStats = CommitStats.createEmpty();

    Assert.assertEquals(0, emptyStats.getNumCommittedWorkUnits());
    Assert.assertEquals(0L, emptyStats.getRecordsWritten());
    Assert.assertEquals(0L, emptyStats.getBytesWritten());
    Assert.assertFalse(emptyStats.getOptFailure().isPresent());
  }

  @Test
  public void testCommitStatsDoesNotContainDatasetStatsMap() {
    // Verify that CommitStats no longer has a datasetStats field
    // This is important to avoid exceeding Temporal's 2MB payload size limit
    CommitStats stats = new CommitStats(10, 1000L, 2000L, Optional.empty());
    
    // Verify only the expected fields exist
    Assert.assertEquals(10, stats.getNumCommittedWorkUnits());
    Assert.assertEquals(1000L, stats.getRecordsWritten());
    Assert.assertEquals(2000L, stats.getBytesWritten());
    Assert.assertFalse(stats.getOptFailure().isPresent());
  }

  @Test
  public void testCommitStatsWithZeroValues() {
    CommitStats stats = new CommitStats(0, 0L, 0L, Optional.empty());

    Assert.assertEquals(0, stats.getNumCommittedWorkUnits());
    Assert.assertEquals(0L, stats.getRecordsWritten());
    Assert.assertEquals(0L, stats.getBytesWritten());
    Assert.assertFalse(stats.getOptFailure().isPresent());
  }

  @Test
  public void testCommitStatsWithLargeValues() {
    // Test with large values to ensure no overflow issues
    int numCommittedWorkUnits = Integer.MAX_VALUE;
    long recordsWritten = Long.MAX_VALUE / 2;
    long bytesWritten = Long.MAX_VALUE / 2;

    CommitStats stats = new CommitStats(numCommittedWorkUnits, recordsWritten, bytesWritten, Optional.empty());

    Assert.assertEquals(numCommittedWorkUnits, stats.getNumCommittedWorkUnits());
    Assert.assertEquals(recordsWritten, stats.getRecordsWritten());
    Assert.assertEquals(bytesWritten, stats.getBytesWritten());
  }
}
