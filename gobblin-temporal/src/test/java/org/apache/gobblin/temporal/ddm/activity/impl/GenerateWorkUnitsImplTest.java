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
        .setFiniteStream(true)
        .build();
    Set<String> output = GenerateWorkUnitsImpl.calculateWorkDirsToCleanup(workUnitStream);
    System.out.println(output);
    Assert.assertEquals(output.size(), 3);
  }
}
