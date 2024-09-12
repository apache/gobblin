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

import org.apache.gobblin.source.workunit.BasicWorkUnitStream;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;


public class GenerateWorkUnitsImplTest {

  @Test
  public void testFetchesWorkDirsFromWorkUnits() {
    List<WorkUnit> workUnits = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      WorkUnit workUnit = new WorkUnit();
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
      MultiWorkUnit multiWorkUnit = new MultiWorkUnit();
      for (int j = 0; j < 3; j++) {
        WorkUnit workUnit = new WorkUnit();
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
      MultiWorkUnit multiWorkUnit = new MultiWorkUnit();
      for (int j = 0; j < 3; j++) {
        WorkUnit workUnit = new WorkUnit();
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
}
