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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.source.workunit.BasicWorkUnitStream;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.apache.gobblin.temporal.ddm.work.WorkUnitsSizeSummary;


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
    Assert.assertEquals(wuSizeInfo.getTopLevelQuantilesMinSizes().size(), numQuantilesDesired); // same as `asSizeInfo` param
    Assert.assertEquals(wuSizeInfo.getConstituentQuantilesMinSizes().size(), numQuantilesDesired); // same as `asSizeInfo` param

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

  public static WorkUnit createWorkUnitOfSize(long size) {
    WorkUnit workUnit = WorkUnit.createEmpty();
    workUnit.setProp(ServiceConfigKeys.WORK_UNIT_SIZE, size);
    return workUnit;
  }
}
