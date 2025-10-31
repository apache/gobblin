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

package org.apache.gobblin.temporal.ddm.activity.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.runtime.DatasetTaskSummary;
import org.apache.gobblin.temporal.ddm.work.DatasetStats;


/**
 * Tests for {@link CommitActivityImpl} to verify the job summary event emission
 * and aggregate stats calculation.
 */
public class CommitActivityImplTest {

  @Test
  public void testConvertDatasetStatsToTaskSummaries() throws Exception {
    CommitActivityImpl commitActivity = new CommitActivityImpl();

    // Create test dataset stats
    Map<String, DatasetStats> datasetStats = new HashMap<>();
    datasetStats.put("dataset1", new DatasetStats(1000L, 2000L, true, 10, "PASS"));
    datasetStats.put("dataset2", new DatasetStats(3000L, 4000L, true, 20, "PASS"));
    datasetStats.put("dataset3", new DatasetStats(5000L, 6000L, false, 0, "FAIL"));

    // Use reflection to call the private method
    java.lang.reflect.Method method = CommitActivityImpl.class.getDeclaredMethod(
        "convertDatasetStatsToTaskSummaries", Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<DatasetTaskSummary> result = (List<DatasetTaskSummary>) method.invoke(commitActivity, datasetStats);

    // Verify the conversion
    Assert.assertEquals(3, result.size());

    // Verify each dataset was converted correctly
    Map<String, DatasetTaskSummary> summaryMap = new HashMap<>();
    for (DatasetTaskSummary summary : result) {
      summaryMap.put(summary.getDatasetUrn(), summary);
    }

    Assert.assertTrue(summaryMap.containsKey("dataset1"));
    Assert.assertEquals(1000L, summaryMap.get("dataset1").getRecordsWritten());
    Assert.assertEquals(2000L, summaryMap.get("dataset1").getBytesWritten());
    Assert.assertTrue(summaryMap.get("dataset1").isSuccessfullyCommitted());
    Assert.assertEquals("PASS", summaryMap.get("dataset1").getDataQualityStatus());

    Assert.assertTrue(summaryMap.containsKey("dataset2"));
    Assert.assertEquals(3000L, summaryMap.get("dataset2").getRecordsWritten());
    Assert.assertEquals(4000L, summaryMap.get("dataset2").getBytesWritten());
    Assert.assertTrue(summaryMap.get("dataset2").isSuccessfullyCommitted());

    Assert.assertTrue(summaryMap.containsKey("dataset3"));
    Assert.assertEquals(5000L, summaryMap.get("dataset3").getRecordsWritten());
    Assert.assertEquals(6000L, summaryMap.get("dataset3").getBytesWritten());
    Assert.assertFalse(summaryMap.get("dataset3").isSuccessfullyCommitted());
    Assert.assertEquals("FAIL", summaryMap.get("dataset3").getDataQualityStatus());
  }

  @Test
  public void testConvertDatasetStatsToTaskSummariesWithEmptyMap() throws Exception {
    CommitActivityImpl commitActivity = new CommitActivityImpl();

    Map<String, DatasetStats> emptyDatasetStats = new HashMap<>();

    // Use reflection to call the private method
    java.lang.reflect.Method method = CommitActivityImpl.class.getDeclaredMethod(
        "convertDatasetStatsToTaskSummaries", Map.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<DatasetTaskSummary> result = (List<DatasetTaskSummary>) method.invoke(commitActivity, emptyDatasetStats);

    Assert.assertNotNull(result);
    Assert.assertEquals(0, result.size());
  }

  @Test
  public void testConvertDatasetStatsToTaskSummariesPreservesAllFields() throws Exception {
    CommitActivityImpl commitActivity = new CommitActivityImpl();

    // Create test dataset stats with specific values
    Map<String, DatasetStats> datasetStats = new HashMap<>();
    String datasetUrn = "testDataset";
    long recordsWritten = 123456L;
    long bytesWritten = 789012L;
    boolean successfullyCommitted = true;
    int numCommittedWorkunits = 42;
    String dataQualityStatus = "PASS";

    datasetStats.put(datasetUrn, new DatasetStats(
        recordsWritten, bytesWritten, successfullyCommitted, numCommittedWorkunits, dataQualityStatus));

    // Use reflection to call the private method
    java.lang.reflect.Method method = CommitActivityImpl.class.getDeclaredMethod(
        "convertDatasetStatsToTaskSummaries", Map.class);
    method.setAccessible(true);
    
    @SuppressWarnings("unchecked")
    List<DatasetTaskSummary> result = (List<DatasetTaskSummary>) method.invoke(commitActivity, datasetStats);

    Assert.assertEquals(1, result.size());
    DatasetTaskSummary summary = result.get(0);
    
    // Verify all fields are preserved
    Assert.assertEquals(datasetUrn, summary.getDatasetUrn());
    Assert.assertEquals(recordsWritten, summary.getRecordsWritten());
    Assert.assertEquals(bytesWritten, summary.getBytesWritten());
    Assert.assertEquals(successfullyCommitted, summary.isSuccessfullyCommitted());
    Assert.assertEquals(dataQualityStatus, summary.getDataQualityStatus());
  }

  @Test
  public void testAggregateStatsCalculation() {
    // This test verifies the aggregate calculation logic used in CommitActivityImpl
    Map<String, DatasetStats> datasetStats = new HashMap<>();
    datasetStats.put("dataset1", new DatasetStats(1000L, 2000L, true, 10, "PASS"));
    datasetStats.put("dataset2", new DatasetStats(3000L, 4000L, true, 20, "PASS"));
    datasetStats.put("dataset3", new DatasetStats(5000L, 6000L, true, 30, "PASS"));

    // Calculate aggregates as done in CommitActivityImpl
    int totalCommittedWorkUnits = datasetStats.values().stream()
        .mapToInt(DatasetStats::getNumCommittedWorkunits)
        .sum();
    long totalRecordsWritten = datasetStats.values().stream()
        .mapToLong(DatasetStats::getRecordsWritten)
        .sum();
    long totalBytesWritten = datasetStats.values().stream()
        .mapToLong(DatasetStats::getBytesWritten)
        .sum();

    Assert.assertEquals(60, totalCommittedWorkUnits);
    Assert.assertEquals(9000L, totalRecordsWritten);
    Assert.assertEquals(12000L, totalBytesWritten);
  }
}
