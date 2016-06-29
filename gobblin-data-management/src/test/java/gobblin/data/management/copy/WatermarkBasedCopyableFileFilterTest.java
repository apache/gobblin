/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.watermark.CopyableFileWatermarkGenerator;
import gobblin.data.management.copy.watermark.CopyableFileWatermarkHelper;
import gobblin.data.management.copy.watermark.FullPathCopyableFileWatermarkGenerator;
import gobblin.data.management.copy.watermark.StringWatermark;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.workunit.WorkUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Test for {@link WatermarkBasedCopyableFileFilter}.
 */
@Test(groups = {"gobblin.data.management.copy"})
public class WatermarkBasedCopyableFileFilterTest {

  @Test
  public void testGetPreivousWatermarkForFilterIfNecessary()
      throws IOException {
    WatermarkInterval watermarkInterval1 = new WatermarkInterval(new StringWatermark("1"), new StringWatermark("6"));
    WatermarkInterval watermarkInterval2 = new WatermarkInterval(new StringWatermark("3"), new StringWatermark("4"));
    WatermarkInterval watermarkInterval3 = new WatermarkInterval(new StringWatermark("0"), new StringWatermark("5"));
    WatermarkInterval watermarkInterval4 = new WatermarkInterval(new StringWatermark("2"), new StringWatermark("7"));

    State jobState = new State();
    jobState.setProp(CopyableFileWatermarkHelper.WATERMARK_CREATOR, FullPathCopyableFileWatermarkGenerator.class.getName());
    CopyableDataset mockedDataset = mock(CopyableDataset.class);
    when(mockedDataset.datasetURN()).thenReturn("dummy");

    WorkUnit committedWorkUnit1 = WorkUnit.create(null, watermarkInterval1);
    committedWorkUnit1.setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkUnitState.WorkingState.COMMITTED);
    WorkUnitState committedWorkUnitState1 = new WorkUnitState(committedWorkUnit1, jobState);
    committedWorkUnitState1.setProp(ConfigurationKeys.DATASET_URN_KEY, "dataset1");
    WorkUnit committedWorkUnit2 = WorkUnit.create(null, watermarkInterval2);
    committedWorkUnit2.setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkUnitState.WorkingState.COMMITTED);
    WorkUnitState committedWorkUnitState2 = new WorkUnitState(committedWorkUnit2, jobState);
    committedWorkUnitState2.setProp(ConfigurationKeys.DATASET_URN_KEY, "dataset1");

    WorkUnit uncommittedWorkUnit1 = WorkUnit.create(null, watermarkInterval3);
    uncommittedWorkUnit1.setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkUnitState.WorkingState.FAILED);
    WorkUnitState uncommittedWorkUnitState1 = new WorkUnitState(uncommittedWorkUnit1, jobState);
    uncommittedWorkUnitState1.setProp(ConfigurationKeys.DATASET_URN_KEY, "dataset2");
    WorkUnit uncommittedWorkUnit2 = WorkUnit.create(null, watermarkInterval4);
    uncommittedWorkUnit2.setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkUnitState.WorkingState.CANCELLED);
    WorkUnitState uncommittedWorkUnitState2 = new WorkUnitState(uncommittedWorkUnit2, jobState);
    uncommittedWorkUnitState2.setProp(ConfigurationKeys.DATASET_URN_KEY, "dataset2");

    CopyableFileWatermarkGenerator watermarkGenerator = new FullPathCopyableFileWatermarkGenerator();
    List<WorkUnitState> workUnitStates1 = Lists
        .newArrayList(committedWorkUnitState1, committedWorkUnitState2, uncommittedWorkUnitState1,
            uncommittedWorkUnitState2);
    SourceState sourceState1 = new SourceState(jobState, workUnitStates1);
    WatermarkBasedCopyableFileFilter copyableFileFilter1 =
        new WatermarkBasedCopyableFileFilter(sourceState1, mockedDataset);
    Map<String, WatermarkBasedCopyableFileFilter.IncludeExcludeWatermark> previousWatermarks =
        copyableFileFilter1.getPreviousWatermarkForFilter(sourceState1, Optional.of(watermarkGenerator));
    Assert.assertEquals(previousWatermarks.keySet(), Sets.newHashSet("dataset1", "dataset2"));
    Assert.assertEquals(((StringWatermark) previousWatermarks.get("dataset1").getWatermark()).getValue(), "6");
    Assert.assertEquals(previousWatermarks.get("dataset1").isInclude(), false);
    Assert.assertEquals(((StringWatermark) previousWatermarks.get("dataset2").getWatermark()).getValue(), "0");
    Assert.assertEquals(previousWatermarks.get("dataset2").isInclude(), true);

    WorkUnit uncommittedWorkUnit3 = WorkUnit.create(null, watermarkInterval3);
    uncommittedWorkUnit3.setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkUnitState.WorkingState.FAILED);
    WorkUnitState uncommittedWorkUnitState3 = new WorkUnitState(uncommittedWorkUnit1, jobState);
    uncommittedWorkUnitState3.setProp(ConfigurationKeys.DATASET_URN_KEY, "dataset1");
    List<WorkUnitState> workUnitStates2 = Lists
        .newArrayList(committedWorkUnitState1, committedWorkUnitState2, uncommittedWorkUnitState1,
            uncommittedWorkUnitState2, uncommittedWorkUnitState3);
    SourceState sourceState2 = new SourceState(jobState, workUnitStates2);
    WatermarkBasedCopyableFileFilter copyableFileFilter2 =
        new WatermarkBasedCopyableFileFilter(sourceState2, mockedDataset);
    previousWatermarks =
        copyableFileFilter2.getPreviousWatermarkForFilter(sourceState2, Optional.of(watermarkGenerator));
    Assert.assertEquals(previousWatermarks.keySet(), Sets.newHashSet("dataset1", "dataset2"));
    Assert.assertEquals(((StringWatermark) previousWatermarks.get("dataset1").getWatermark()).getValue(), "0");
    Assert.assertEquals(previousWatermarks.get("dataset1").isInclude(), true);
    Assert.assertEquals(((StringWatermark) previousWatermarks.get("dataset2").getWatermark()).getValue(), "0");
    Assert.assertEquals(previousWatermarks.get("dataset2").isInclude(), true);
  }
}
