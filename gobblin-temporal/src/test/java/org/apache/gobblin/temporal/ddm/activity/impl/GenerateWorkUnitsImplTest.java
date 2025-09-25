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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.runtime.CombinedWorkUnitAndDatasetStateGenerator;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.util.DataStateStoreUtils;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.source.workunit.BasicWorkUnitStream;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.WorkUnitsSizeSummary;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class GenerateWorkUnitsImplTest {

  @Test
  public void testFetchesWorkDirsFromWorkUnits() {
    List<WorkUnit> workUnits = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      WorkUnit workUnit = WorkUnit.createEmpty();
      workUnit.setProp("writer.staging.dir", "/tmp/jobId/task-staging/" + i);
      workUnit.setProp("writer.output.dir", "/tmp/jobId/task-output/" + i);
      workUnit.setProp("qualitychecker.row.err.file", "/tmp/jobId/row-err/file");
      workUnit.setProp("qualitychecker.clean.err.dir", "true");
      workUnits.add(workUnit);
    }
    WorkUnitStream workUnitStream = new BasicWorkUnitStream.Builder(workUnits)
        .setFiniteStream(true)
        .build();
    Set<String> output = GenerateWorkUnitsImpl.calculateWorkDirsToCleanup(workUnitStream);
    Assert.assertEquals(output.size(), 11);
  }

  @Test
  public void testFetchesWorkDirsFromMultiWorkUnits() {
    List<WorkUnit> workUnits = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      MultiWorkUnit multiWorkUnit = MultiWorkUnit.createEmpty();
      for (int j = 0; j < 3; j++) {
        WorkUnit workUnit = WorkUnit.createEmpty();
        workUnit.setProp("writer.staging.dir", "/tmp/jobId/task-staging/");
        workUnit.setProp("writer.output.dir", "/tmp/jobId/task-output/");
        workUnit.setProp("qualitychecker.row.err.file", "/tmp/jobId/row-err/file");
        workUnit.setProp("qualitychecker.clean.err.dir", "true");
        multiWorkUnit.addWorkUnit(workUnit);
      }
      workUnits.add(multiWorkUnit);
    }
    WorkUnitStream workUnitStream = new BasicWorkUnitStream.Builder(workUnits)
        .setFiniteStream(true)
        .build();
    Set<String> output = GenerateWorkUnitsImpl.calculateWorkDirsToCleanup(workUnitStream);
    Assert.assertEquals(output.size(), 3);
  }

  @Test
  public void testFetchesUniqueWorkDirsFromMultiWorkUnits() {
    List<WorkUnit> workUnits = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      MultiWorkUnit multiWorkUnit = MultiWorkUnit.createEmpty();
      for (int j = 0; j < 3; j++) {
        WorkUnit workUnit = WorkUnit.createEmpty();
        // Each MWU will have its own staging and output dir
        workUnit.setProp("writer.staging.dir", "/tmp/jobId/" + i + "/task-staging/");
        workUnit.setProp("writer.output.dir", "/tmp/jobId/" + i + "task-output/");
        workUnit.setProp("qualitychecker.row.err.file", "/tmp/jobId/row-err/file");
        workUnit.setProp("qualitychecker.clean.err.dir", "true");
        multiWorkUnit.addWorkUnit(workUnit);
      }
      workUnits.add(multiWorkUnit);
    }
    WorkUnitStream workUnitStream = new BasicWorkUnitStream.Builder(workUnits)
        .setFiniteStream(true)
        .build();
    Set<String> output = GenerateWorkUnitsImpl.calculateWorkDirsToCleanup(workUnitStream);
    Assert.assertEquals(output.size(), 11);
  }

  @Test
  public void testDigestWorkUnitsSize() {
    int numSingleWorkUnits = 5;
    int numMultiWorkUnits = 15;
    long singleWorkUnitSizeFactor = 100L;
    long multiWorkUnitSizeFactor = 70L;
    List<WorkUnit> workUnits = new ArrayList<>();

    // configure size of non-multi-work-units (increments of `singleWorkUnitSizeFactor`, starting from 0)
    for (int i = 0; i < numSingleWorkUnits; i++) {
      workUnits.add(createWorkUnitOfSize(i * singleWorkUnitSizeFactor));
    }

    // configure size of multi-work-units, each containing between 1 and 4 sub-work-unit children
    for (int i = 0; i < numMultiWorkUnits; i++) {
      MultiWorkUnit multiWorkUnit = MultiWorkUnit.createEmpty();
      int subWorkUnitCount = 1 + (i % 4); // 1 to 4
      for (int j = 0; j < subWorkUnitCount; j++) {
        multiWorkUnit.addWorkUnit(createWorkUnitOfSize((j + 1) * multiWorkUnitSizeFactor));
      }
      workUnits.add(multiWorkUnit);
    }

    // calc expectations
    long expectedTotalSize = 0L;
    int expectedNumTopLevelWorkUnits = numSingleWorkUnits + numMultiWorkUnits;
    int expectedNumConstituentWorkUnits = numSingleWorkUnits;
    for (int i = 0; i < numSingleWorkUnits; i++) {
      expectedTotalSize += i * singleWorkUnitSizeFactor;
    }
    for (int i = 0; i < numMultiWorkUnits; i++) {
      int numSubWorkUnitsThisMWU = 1 + (i % 4);
      expectedNumConstituentWorkUnits += numSubWorkUnitsThisMWU;
      for (int j = 0; j < numSubWorkUnitsThisMWU; j++) {
        expectedTotalSize += (j + 1) * multiWorkUnitSizeFactor;
      }
    }

    GenerateWorkUnitsImpl.WorkUnitsSizeDigest wuSizeDigest = GenerateWorkUnitsImpl.digestWorkUnitsSize(workUnits);

    Assert.assertEquals(wuSizeDigest.getTotalSize(), expectedTotalSize);
    Assert.assertEquals(wuSizeDigest.getTopLevelWorkUnitsSizeDigest().size(), expectedNumTopLevelWorkUnits);
    Assert.assertEquals(wuSizeDigest.getConstituentWorkUnitsSizeDigest().size(), expectedNumConstituentWorkUnits);

    int numQuantilesDesired = expectedNumTopLevelWorkUnits; // for simpler math during quantile verification (below)
    WorkUnitsSizeSummary wuSizeInfo = wuSizeDigest.asSizeSummary(numQuantilesDesired);
    Assert.assertEquals(wuSizeInfo.getTotalSize(), expectedTotalSize);
    Assert.assertEquals(wuSizeInfo.getTopLevelWorkUnitsCount(), expectedNumTopLevelWorkUnits);
    Assert.assertEquals(wuSizeInfo.getConstituentWorkUnitsCount(), expectedNumConstituentWorkUnits);
    Assert.assertEquals(wuSizeInfo.getQuantilesCount(), numQuantilesDesired);
    Assert.assertEquals(wuSizeInfo.getQuantilesWidth(), 1.0 / expectedNumTopLevelWorkUnits);
    Assert.assertEquals(wuSizeInfo.getTopLevelQuantilesMinSizes().size(), numQuantilesDesired); // same as `asSizeSummary` param
    Assert.assertEquals(wuSizeInfo.getConstituentQuantilesMinSizes().size(), numQuantilesDesired); // same as `asSizeSummary` param

    // expected sizes for (n=5) top-level non-multi-WUs: (1x) 0, (1x) 100, (1x) 200, (1x) 300, (1x) 400
    // expected sizes for (n=15) top-level multi-WUs: [a] (4x) 70; [b] (4x) 210 (= 70+140); [c] (4x) 420 (= 70+140+210); [d] (3x) 700 (= 70+140+210+280)
    Assert.assertEquals(wuSizeInfo.getTopLevelQuantilesMinSizes().toArray(),
        new Double[]{
            70.0, 70.0, 70.0, 70.0, // 4x MWU [a]
            100.0, 200.0, // non-MWU [2, 3]
            210.0, 210.0, 210.0, 210.0, // 4x MWU [b]
            300.0, 400.0, // non-MWU [4, 5]
            420.0, 420.0, 420.0, 420.0, // 4x MWU [c]
            700.0, 700.0, 700.0, 700.0 }); // 3x MWU [d] + "100-percentile" (all WUs)

    // expected sizes for (n=36) constituents from multi-WUs: [m] (15x = 4+4+4+3) 70; [n] (11x = 4+4+3) 140; [o] (7x = 4+3) 210; [p] (3x) 280
    Assert.assertEquals(wuSizeInfo.getConstituentQuantilesMinSizes().toArray(),
        new Double[]{
            70.0, 70.0, 70.0, 70.0, 70.0, 70.0, 70.0, // (per 15x MWU [m]) - 15/41 * 20 ~ 7.3
            100.0, // non-MWU [2]
            140.0, 140.0, 140.0, 140.0, 140.0, // (per 11x MWU [n]) - (15+1+11/41) * 20 ~ 13.2  |  13.2 - 8 = 5.2
            200.0, // non-MWU [3]
            210.0, 210.0, 210.0, // (per 7x MWU [o]) - (15+1+11+1+7/41) * 20 ~ 17.0  |  17.0 - 14 = 3
            280.0, 280.0, // 3x MWU [p] ... (15+1+11+1+7+3/41) * 20 ~ 18.5  |  18.5 - 17 = 2
            400.0 }); // with only one 20-quantile remaining, non-MWU [5] completes the "100-percentile" (all WUs)
  }

  @Test
  public void testDigestWorkUnitsSizeWithEmptyWorkUnits() {
    List<WorkUnit> workUnits = new ArrayList<>();
    GenerateWorkUnitsImpl.WorkUnitsSizeDigest wuSizeDigest = GenerateWorkUnitsImpl.digestWorkUnitsSize(workUnits);

    Assert.assertEquals(wuSizeDigest.getTotalSize(), 0L);
    Assert.assertEquals(wuSizeDigest.getTopLevelWorkUnitsSizeDigest().size(), 0);
    Assert.assertEquals(wuSizeDigest.getConstituentWorkUnitsSizeDigest().size(), 0);

    int numQuantilesDesired = 10;
    WorkUnitsSizeSummary wuSizeInfo = wuSizeDigest.asSizeSummary(numQuantilesDesired);
    Assert.assertEquals(wuSizeInfo.getTotalSize(), 0L);
    Assert.assertEquals(wuSizeInfo.getTopLevelWorkUnitsCount(), 0);
    Assert.assertEquals(wuSizeInfo.getConstituentWorkUnitsCount(), 0);
    Assert.assertEquals(wuSizeInfo.getQuantilesCount(), numQuantilesDesired);
    Assert.assertEquals(wuSizeInfo.getQuantilesWidth(), 1.0 / numQuantilesDesired);
    Assert.assertEquals(wuSizeInfo.getTopLevelQuantilesMinSizes().size(), numQuantilesDesired); // same as `asSizeSummary` param
    Assert.assertEquals(wuSizeInfo.getConstituentQuantilesMinSizes().size(), numQuantilesDesired); // same as `asSizeSummary` param
    Assert.assertEquals(wuSizeInfo.getConstituentWorkUnitsMeanSize(), 0.0);
    Assert.assertEquals(wuSizeInfo.getTopLevelWorkUnitsMeanSize(), 0.0);
    Assert.assertEquals(wuSizeInfo.getConstituentWorkUnitsMeanSize(), 0.0);
    Assert.assertEquals(wuSizeInfo.getTopLevelWorkUnitsMedianSize(), 0.0);
    Assert.assertEquals(wuSizeInfo.getConstituentWorkUnitsMedianSize(), 0.0);
  }

  @Test
  public void testAddDatasetStateFunctionalAndSharedResourceBrokerToJobState() throws Exception {
    // Arrange
    Properties jobProps = new Properties();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "test-job");
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, "test-job-id");
    
    JobState jobState = new JobState(jobProps);
    SharedResourcesBroker<GobblinScopeTypes> mockBroker = mock(SharedResourcesBroker.class);
    DatasetStateStore mockDatasetStateStore = mock(DatasetStateStore.class);
    
    // Get access to the private method
    Method privateMethod = GenerateWorkUnitsImpl.class.getDeclaredMethod(
        "addDatasetSataeFunctionalAndSharedResourceBrokerToJobState", Properties.class, JobState.class);
    privateMethod.setAccessible(true);
    
    // Mock static method calls
    try (MockedStatic<JobStateUtils> mockedJobStateUtils = Mockito.mockStatic(JobStateUtils.class);
         MockedStatic<DataStateStoreUtils> mockedDataStateStoreUtils = Mockito.mockStatic(DataStateStoreUtils.class)) {
      
      mockedJobStateUtils.when(() -> JobStateUtils.getSharedResourcesBroker(jobState))
          .thenReturn(mockBroker);
      mockedDataStateStoreUtils.when(() -> DataStateStoreUtils.createStateStore(any()))
          .thenReturn(mockDatasetStateStore);
      
      // Act
      privateMethod.invoke(null, jobProps, jobState);
      
      // Assert
      Assert.assertEquals(jobState.getBroker(), mockBroker, "SharedResourcesBroker should be set on JobState");
      Assert.assertNotNull(jobState.getWorkUnitAndDatasetStateFunctional(), "WorkUnitAndDatasetStateFunctional should be set");
      Assert.assertTrue(jobState.getWorkUnitAndDatasetStateFunctional() instanceof CombinedWorkUnitAndDatasetStateGenerator,
          "WorkUnitAndDatasetStateFunctional should be instance of CombinedWorkUnitAndDatasetStateGenerator");
      
      // Verify interactions
      mockedJobStateUtils.verify(() -> JobStateUtils.getSharedResourcesBroker(jobState), times(1));
      mockedDataStateStoreUtils.verify(() -> DataStateStoreUtils.createStateStore(any()), times(1));
    }
  }

  @Test
  public void testAddDatasetStateFunctionalAndSharedResourceBrokerToJobStateWithIOException() throws Exception {
    // Arrange
    Properties jobProps = new Properties();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "test-job");
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, "test-job-id");
    
    JobState jobState = new JobState(jobProps);
    
    // Get access to the private method
    Method privateMethod = GenerateWorkUnitsImpl.class.getDeclaredMethod(
        "addDatasetSataeFunctionalAndSharedResourceBrokerToJobState", Properties.class, JobState.class);
    privateMethod.setAccessible(true);
    
    // Mock static method calls to throw IOException
    try (MockedStatic<JobStateUtils> mockedJobStateUtils = Mockito.mockStatic(JobStateUtils.class);
         MockedStatic<DataStateStoreUtils> mockedDataStateStoreUtils = Mockito.mockStatic(DataStateStoreUtils.class)) {
      
      mockedDataStateStoreUtils.when(() -> DataStateStoreUtils.createStateStore(any()))
          .thenThrow(new IOException("Failed to create state store"));
      
      // Act & Assert
      try {
        privateMethod.invoke(null, jobProps, jobState);
        Assert.fail("Expected IOException to be thrown");
      } catch (InvocationTargetException e) {
        Assert.assertTrue(e.getCause() instanceof IOException, "Root cause should be IOException");
        Assert.assertEquals(e.getCause().getMessage(), "Failed to create state store");
        // Verify broker was never set due to exception
        Assert.assertNull(jobState.getBroker(), "Broker should not be set when exception occurs");
        Assert.assertNull(jobState.getWorkUnitAndDatasetStateFunctional(), 
            "WorkUnitAndDatasetStateFunctional should not be set when exception occurs");
      }
    }
  }

  @Test
  public void testAddDatasetStateFunctionalAndSharedResourceBrokerToJobStateWithNullBroker() throws Exception {
    // Arrange
    Properties jobProps = new Properties();
    jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, "test-job");
    jobProps.setProperty(ConfigurationKeys.JOB_ID_KEY, "test-job-id");
    
    JobState jobState = new JobState(jobProps);
    DatasetStateStore mockDatasetStateStore = mock(DatasetStateStore.class);
    
    // Get access to the private method
    Method privateMethod = GenerateWorkUnitsImpl.class.getDeclaredMethod(
        "addDatasetSataeFunctionalAndSharedResourceBrokerToJobState", Properties.class, JobState.class);
    privateMethod.setAccessible(true);
    
    // Mock static method calls - return null broker to test null handling
    try (MockedStatic<JobStateUtils> mockedJobStateUtils = Mockito.mockStatic(JobStateUtils.class);
         MockedStatic<DataStateStoreUtils> mockedDataStateStoreUtils = Mockito.mockStatic(DataStateStoreUtils.class)) {
      
      mockedJobStateUtils.when(() -> JobStateUtils.getSharedResourcesBroker(jobState))
          .thenReturn(null);
      mockedDataStateStoreUtils.when(() -> DataStateStoreUtils.createStateStore(any()))
          .thenReturn(mockDatasetStateStore);
      
      // Act
      privateMethod.invoke(null, jobProps, jobState);
      
      // Assert
      Assert.assertNull(jobState.getBroker(), "Broker should be null when null broker is returned");
      Assert.assertNotNull(jobState.getWorkUnitAndDatasetStateFunctional(), 
          "WorkUnitAndDatasetStateFunctional should still be set even with null broker");
    }
  }

  @Test
  public void testAddDatasetStateFunctionalAndSharedResourceBrokerToJobStateWithEmptyJobProps() throws Exception {
    // Arrange
    Properties emptyJobProps = new Properties();
    JobState jobState = new JobState();
    jobState.setJobName("default-job");
    jobState.setJobId("default-job-id");
    
    SharedResourcesBroker<GobblinScopeTypes> mockBroker = mock(SharedResourcesBroker.class);
    DatasetStateStore mockDatasetStateStore = mock(DatasetStateStore.class);
    
    // Get access to the private method
    Method privateMethod = GenerateWorkUnitsImpl.class.getDeclaredMethod(
        "addDatasetSataeFunctionalAndSharedResourceBrokerToJobState", Properties.class, JobState.class);
    privateMethod.setAccessible(true);
    
    // Mock static method calls
    try (MockedStatic<JobStateUtils> mockedJobStateUtils = Mockito.mockStatic(JobStateUtils.class);
         MockedStatic<DataStateStoreUtils> mockedDataStateStoreUtils = Mockito.mockStatic(DataStateStoreUtils.class)) {
      
      mockedJobStateUtils.when(() -> JobStateUtils.getSharedResourcesBroker(jobState))
          .thenReturn(mockBroker);
      mockedDataStateStoreUtils.when(() -> DataStateStoreUtils.createStateStore(any()))
          .thenReturn(mockDatasetStateStore);
      
      // Act
      privateMethod.invoke(null, emptyJobProps, jobState);
      
      // Assert
      Assert.assertEquals(jobState.getBroker(), mockBroker, "SharedResourcesBroker should be set on JobState");
      Assert.assertNotNull(jobState.getWorkUnitAndDatasetStateFunctional(), "WorkUnitAndDatasetStateFunctional should be set");
      
      // Verify that empty properties were still used to create state store
      mockedDataStateStoreUtils.verify(() -> DataStateStoreUtils.createStateStore(any()), times(1));
    }
  }

  @Test
  public void testMethodNameTypoExists() {
    // This test documents the existing typo in the method name
    // "addDatasetSataeFunctionalAndSharedResourceBrokerToJobState" should be 
    // "addDatasetStateFunctionalAndSharedResourceBrokerToJobState"
    
    // Verify the method exists with the current (incorrect) spelling
    try {
      GenerateWorkUnitsImpl.class.getDeclaredMethod(
          "addDatasetSataeFunctionalAndSharedResourceBrokerToJobState", 
          Properties.class, JobState.class);
      // If we get here, the method exists with the typo
      Assert.assertTrue(true, "Method with typo exists - consider renaming to fix spelling");
    } catch (NoSuchMethodException e) {
      // If the method name was corrected, this test would fail and should be updated
      Assert.fail("Method with typo 'addDatasetSataeFunctionalAndSharedResourceBrokerToJobState' not found - " +
          "has the typo been fixed? If so, update this test.");
    }
  }

  public static WorkUnit createWorkUnitOfSize(long size) {
    WorkUnit workUnit = WorkUnit.createEmpty();
    workUnit.setProp(ServiceConfigKeys.WORK_UNIT_SIZE, size);
    return workUnit;
  }
}
