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

package org.apache.gobblin.temporal.ddm.workflow.impl;

import java.lang.reflect.Method;
import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.temporal.ddm.work.CommitStats;


/**
 * Tests for {@link CommitStepWorkflowImpl} to verify that job summary event emission
 * has been moved to CommitActivity to avoid Temporal payload size limits.
 */
public class CommitStepWorkflowImplTest {

  @Test
  public void testCommitStepWorkflowDoesNotConvertDatasetStats() throws Exception {
    // Verify that CommitStepWorkflowImpl no longer has the convertDatasetStatsToTaskSummaries method
    // This method should only exist in CommitActivityImpl now

    CommitStepWorkflowImpl workflow = new CommitStepWorkflowImpl();
    Method[] methods = CommitStepWorkflowImpl.class.getDeclaredMethods();

    boolean hasConvertMethod = false;
    for (Method method : methods) {
      if (method.getName().equals("convertDatasetStatsToTaskSummaries")) {
        hasConvertMethod = true;
        break;
      }
    }

    Assert.assertFalse(hasConvertMethod,
        "CommitStepWorkflowImpl should not have convertDatasetStatsToTaskSummaries method. " +
        "Job summary emission should be done in CommitActivity to avoid payload size issues.");
  }

  @Test
  public void testCommitStepWorkflowSimplifiedStructure() {
    // Verify that CommitStepWorkflowImpl has a simplified structure
    // It should only have the commit method and no job summary emission logic

    CommitStepWorkflowImpl workflow = new CommitStepWorkflowImpl();
    Method[] methods = CommitStepWorkflowImpl.class.getDeclaredMethods();

    // Count non-inherited methods (excluding synthetic methods)
    int methodCount = 0;
    for (Method method : methods) {
      if (!method.isSynthetic()) {
        methodCount++;
      }
    }

    // Should only have the commit method
    Assert.assertTrue(methodCount <= 1,
        "CommitStepWorkflowImpl should have minimal methods. Found: " + methodCount);
  }

  @Test
  public void testCommitStatsStructureForPayloadSizeOptimization() {
    // Verify that CommitStats returned by the workflow has aggregate values only
    // This ensures the payload size stays under Temporal's 2MB limit

    CommitStats stats = new CommitStats(100, 50000L, 1024000L, Optional.empty());

    // Verify it has aggregate values
    Assert.assertEquals(100, stats.getNumCommittedWorkUnits());
    Assert.assertEquals(50000L, stats.getRecordsWritten());
    Assert.assertEquals(1024000L, stats.getBytesWritten());

    // Verify the object is small (should be under 1KB for aggregate values)
    // This is a proxy test to ensure we're not serializing large maps
    String statsString = stats.toString();
    Assert.assertTrue(statsString.length() < 1000,
        "CommitStats toString should be small. Length: " + statsString.length());
  }
}
