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

package org.apache.gobblin.data.management.source;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.materializer.HiveMaterializerTaskFactory;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.gobblin.dataset.PartitionableDataset;
import org.apache.gobblin.dataset.test.SimpleDatasetForTesting;
import org.apache.gobblin.dataset.test.SimpleDatasetPartitionForTesting;
import org.apache.gobblin.dataset.test.SimplePartitionableDatasetForTesting;
import org.apache.gobblin.dataset.test.StaticDatasetsFinderForTesting;
import org.apache.gobblin.runtime.mapreduce.MRTaskFactory;
import org.apache.gobblin.runtime.task.FailedTask;
import org.apache.gobblin.runtime.task.NoopTask;
import org.apache.gobblin.runtime.task.TaskUtils;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import static org.apache.gobblin.data.management.source.LoopingDatasetFinderSource.IS_ROLLOVER_WU;


public class LoopingDatasetFinderSourceTest {

  @Test
  public void testNonDrilldown() {
    Dataset dataset1 = new SimpleDatasetForTesting("dataset1");
    Dataset dataset2 = new SimplePartitionableDatasetForTesting("dataset2", Lists.newArrayList(new SimpleDatasetPartitionForTesting("p1"), new SimpleDatasetPartitionForTesting("p2")));
    Dataset dataset3 = new SimpleDatasetForTesting("dataset3");
    Dataset dataset4 = new SimpleDatasetForTesting("dataset4");
    Dataset dataset5 = new SimpleDatasetForTesting("dataset5");

    IterableDatasetFinder finder = new StaticDatasetsFinderForTesting(
        Lists.newArrayList(dataset5, dataset4, dataset3, dataset2, dataset1));

    MySource mySource = new MySource(false, finder);

    SourceState sourceState = new SourceState();
    sourceState.setProp(LoopingDatasetFinderSource.MAX_WORK_UNITS_PER_RUN_KEY, 3);

    WorkUnitStream workUnitStream = mySource.getWorkunitStream(sourceState);
    List<WorkUnit> workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 3);
    Assert.assertEquals(workUnits.get(0).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset1");
    Assert.assertNull(workUnits.get(0).getProp(DatasetFinderSourceTest.PARTITION_URN));
    Assert.assertEquals(workUnits.get(1).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset2");
    Assert.assertNull(workUnits.get(1).getProp(DatasetFinderSourceTest.PARTITION_URN));
    Assert.assertEquals(workUnits.get(2).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset3");
    Assert.assertNull(workUnits.get(2).getProp(DatasetFinderSourceTest.PARTITION_URN));

    // Second run should continue where it left off
    List<WorkUnitState> workUnitStates = workUnits.stream().map(WorkUnitState::new).collect(Collectors.toList());
    SourceState sourceStateSpy = Mockito.spy(sourceState);
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates();

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 3);
    Assert.assertEquals(workUnits.get(0).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset4");
    Assert.assertNull(workUnits.get(0).getProp(DatasetFinderSourceTest.PARTITION_URN));
    Assert.assertEquals(workUnits.get(1).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset5");
    Assert.assertNull(workUnits.get(1).getProp(DatasetFinderSourceTest.PARTITION_URN));
    Assert.assertTrue(workUnits.get(2).getPropAsBoolean(LoopingDatasetFinderSource.END_OF_DATASETS_KEY));

    // Loop around
    workUnitStates = workUnits.stream().map(WorkUnitState::new).collect(Collectors.toList());
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates();

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 3);
    Assert.assertEquals(workUnits.get(0).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset1");
    Assert.assertNull(workUnits.get(0).getProp(DatasetFinderSourceTest.PARTITION_URN));
    Assert.assertEquals(workUnits.get(1).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset2");
    Assert.assertNull(workUnits.get(1).getProp(DatasetFinderSourceTest.PARTITION_URN));
    Assert.assertEquals(workUnits.get(2).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset3");
    Assert.assertNull(workUnits.get(2).getProp(DatasetFinderSourceTest.PARTITION_URN));
  }

  @Test
  public void testDrilldown() {
    // Create three datasets, two of them partitioned
    Dataset dataset1 = new SimpleDatasetForTesting("dataset1");
    Dataset dataset2 = new SimplePartitionableDatasetForTesting("dataset2",
        Lists.newArrayList(new SimpleDatasetPartitionForTesting("p1"),
            new SimpleDatasetPartitionForTesting("p2"), new SimpleDatasetPartitionForTesting("p3")));
    Dataset dataset3 = new SimplePartitionableDatasetForTesting("dataset3",
        Lists.newArrayList(new SimpleDatasetPartitionForTesting("p1"),
            new SimpleDatasetPartitionForTesting("p2"), new SimpleDatasetPartitionForTesting("p3")));

    IterableDatasetFinder finder = new StaticDatasetsFinderForTesting(
        Lists.newArrayList(dataset3, dataset2, dataset1));

    MySource mySource = new MySource(true, finder);

    // Limit to 3 wunits per run
    SourceState sourceState = new SourceState();
    sourceState.setProp(LoopingDatasetFinderSource.MAX_WORK_UNITS_PER_RUN_KEY, 3);

    // first run, get three first work units
    WorkUnitStream workUnitStream = mySource.getWorkunitStream(sourceState);
    List<WorkUnit> workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 3);
    Assert.assertEquals(workUnits.get(0).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset1");
    Assert.assertNull(workUnits.get(0).getProp(DatasetFinderSourceTest.PARTITION_URN));
    Assert.assertEquals(workUnits.get(1).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset2");
    Assert.assertEquals(workUnits.get(1).getProp(DatasetFinderSourceTest.PARTITION_URN), "p1");
    Assert.assertEquals(workUnits.get(2).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset2");
    Assert.assertEquals(workUnits.get(2).getProp(DatasetFinderSourceTest.PARTITION_URN), "p2");

    // Second run should continue where it left off
    List<WorkUnitState> workUnitStates = workUnits.stream().map(WorkUnitState::new).collect(Collectors.toList());
    SourceState sourceStateSpy = Mockito.spy(sourceState);
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates();

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 3);
    Assert.assertEquals(workUnits.get(0).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset2");
    Assert.assertEquals(workUnits.get(0).getProp(DatasetFinderSourceTest.PARTITION_URN), "p3");
    Assert.assertEquals(workUnits.get(1).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset3");
    Assert.assertEquals(workUnits.get(1).getProp(DatasetFinderSourceTest.PARTITION_URN), "p1");
    Assert.assertEquals(workUnits.get(2).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset3");
    Assert.assertEquals(workUnits.get(2).getProp(DatasetFinderSourceTest.PARTITION_URN), "p2");

    // third run, continue from where it left off
    workUnitStates = workUnits.stream().map(WorkUnitState::new).collect(Collectors.toList());
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates();

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 2);
    Assert.assertEquals(workUnits.get(0).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset3");
    Assert.assertEquals(workUnits.get(0).getProp(DatasetFinderSourceTest.PARTITION_URN), "p3");
    Assert.assertTrue(workUnits.get(1).getPropAsBoolean(LoopingDatasetFinderSource.END_OF_DATASETS_KEY));

    // fourth run, finished all work units, loop around
    workUnitStates = workUnits.stream().map(WorkUnitState::new).collect(Collectors.toList());
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates();

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 3);
    Assert.assertEquals(workUnits.get(0).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset1");
    Assert.assertNull(workUnits.get(0).getProp(DatasetFinderSourceTest.PARTITION_URN));
    Assert.assertEquals(workUnits.get(1).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset2");
    Assert.assertEquals(workUnits.get(1).getProp(DatasetFinderSourceTest.PARTITION_URN), "p1");
    Assert.assertEquals(workUnits.get(2).getProp(DatasetFinderSourceTest.DATASET_URN), "dataset2");
    Assert.assertEquals(workUnits.get(2).getProp(DatasetFinderSourceTest.PARTITION_URN), "p2");
  }

  @Test
  public void testFailedWorkUnitRetry() {
    IterableDatasetFinder finder = new StaticDatasetsFinderForTesting(Collections.emptyList());
    MySource mySource = new MySource(true, finder);

    SourceState sourceState = new SourceState();
    sourceState.setProp(LoopingDatasetFinderSource.MAX_WORK_UNITS_PER_RUN_KEY, 3);

    WorkUnit failedWorkUnit_1 = new FailedTask.FailedWorkUnit();
    failedWorkUnit_1.setProp(LoopingDatasetFinderSource.DATASET_URN, "failed_dataset_1");
    failedWorkUnit_1.setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkUnitState.WorkingState.FAILED);

    WorkUnit failedWorkUnit_2 = new FailedTask.FailedWorkUnit();
    failedWorkUnit_2.setProp(LoopingDatasetFinderSource.DATASET_URN, "failed_dataset_2");
    failedWorkUnit_2.setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkUnitState.WorkingState.FAILED);

    WorkUnit failedWorkUnit_3 = new FailedTask.FailedWorkUnit();
    failedWorkUnit_3.setProp(LoopingDatasetFinderSource.DATASET_URN, "failed_dataset_3");
    failedWorkUnit_3.setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkUnitState.WorkingState.FAILED);

    List<WorkUnit> workUnits = Lists.newArrayList(failedWorkUnit_1, failedWorkUnit_2, failedWorkUnit_3);
    List<WorkUnitState> workUnitStates = workUnits.stream().map(WorkUnitState::new).collect(Collectors.toList());
    SourceState sourceStateSpy = Mockito.spy(sourceState);
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates();

    WorkUnitStream workUnitStream = mySource.getWorkunitStream(sourceStateSpy);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 3);
    Assert.assertEquals(workUnits.get(0).getProp(LoopingDatasetFinderSource.DATASET_URN), "failed_dataset_1");
    Assert.assertEquals(workUnits.get(1).getProp(LoopingDatasetFinderSource.DATASET_URN), "failed_dataset_2");
    Assert.assertEquals(workUnits.get(2).getProp(LoopingDatasetFinderSource.DATASET_URN), "failed_dataset_3");
  }

  // Question here will be:
  // - What is the process of getting previous state from state store, and what are the things that being persisted there.
  // - And what will be the difference in terms of datasetState and JobState for this previous work unit persistency ?
  @Test
  public void testRolleverWorkUnitUseCase() {
    IterableDatasetFinder finder = new StaticDatasetsFinderForTesting(Collections.emptyList());
    MySource mySource = new MySource(true, finder);

    // Limit 1 work unit per run, and number of failed work units is larger therefore there should be rolled over tasks.
    // Failed/RolledOver tasks will be kept in retrying mode until it exceeds the number of retry times now.
    SourceState sourceState = new SourceState();
    sourceState.setProp(LoopingDatasetFinderSource.MAX_WORK_UNITS_PER_RUN_KEY, 1);

    WorkUnit failedWorkUnit_1 = new FailedTask.FailedWorkUnit();
    failedWorkUnit_1.setProp(LoopingDatasetFinderSource.DATASET_URN, "failed_dataset_1");
    failedWorkUnit_1.setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkUnitState.WorkingState.FAILED);

    WorkUnit failedWorkUnit_2 = new FailedTask.FailedWorkUnit();
    failedWorkUnit_2.setProp(LoopingDatasetFinderSource.DATASET_URN, "rolledOver_dataset_2");
    TaskUtils.setTaskFactoryClass(failedWorkUnit_2, MRTaskFactory.class);
    failedWorkUnit_2.setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkUnitState.WorkingState.FAILED);

    WorkUnit failedWorkUnit_3 = new FailedTask.FailedWorkUnit();
    TaskUtils.setTaskFactoryClass(failedWorkUnit_3, HiveMaterializerTaskFactory.class);
    failedWorkUnit_3.setProp(LoopingDatasetFinderSource.DATASET_URN, "rolledOver_dataset_3");
    failedWorkUnit_3.setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY, WorkUnitState.WorkingState.FAILED);

    List<WorkUnit> workUnits = Lists.newArrayList(failedWorkUnit_1, failedWorkUnit_2, failedWorkUnit_3);
    List<WorkUnitState> workUnitStates = workUnits.stream().map(WorkUnitState::new).collect(Collectors.toList());
    SourceState sourceStateSpy = Mockito.spy(sourceState);
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates();

    WorkUnitStream workUnitStream = mySource.getWorkunitStream(sourceStateSpy);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 3);
    Assert.assertEquals(workUnits.get(0).getProp(LoopingDatasetFinderSource.DATASET_URN), "failed_dataset_1");
    Assert.assertTrue(workUnits.get(1).getPropAsBoolean(IS_ROLLOVER_WU));
    Assert.assertEquals(workUnits.get(1).getProp("org.apache.gobblin.runtime.taskFactoryClass"), NoopTask.Factory.class.getName());
    Assert.assertTrue(workUnits.get(2).getPropAsBoolean(IS_ROLLOVER_WU));
    Assert.assertEquals(workUnits.get(2).getProp("org.apache.gobblin.runtime.taskFactoryClass"), NoopTask.Factory.class.getName());


    // The case when all rollover WUs are being processed.
    SourceState sourceStateBig = new SourceState();
    sourceState.setProp(LoopingDatasetFinderSource.MAX_WORK_UNITS_PER_RUN_KEY, 3);
    SourceState sourceStateSpyBig = Mockito.spy(sourceStateBig);
    Mockito.doReturn(workUnitStates).when(sourceStateSpyBig).getPreviousWorkUnitStates();
    WorkUnitStream workUnitStreamBig = mySource.getWorkunitStream(sourceStateSpyBig);
    workUnits = Lists.newArrayList(workUnitStreamBig.getWorkUnits());

    // Containing one more end of stream WU.
    Assert.assertEquals(workUnits.size(), 4);
    Assert.assertEquals(workUnits.get(0).getProp(LoopingDatasetFinderSource.DATASET_URN), "failed_dataset_1");
    Assert.assertFalse(workUnits.get(1).getPropAsBoolean(IS_ROLLOVER_WU));
    Assert.assertTrue(TaskUtils.getTaskFactory(workUnits.get(1)).get() instanceof MRTaskFactory);
    Assert.assertFalse(workUnits.get(2).getPropAsBoolean(IS_ROLLOVER_WU));
    Assert.assertTrue(TaskUtils.getTaskFactory(workUnits.get(2)).get() instanceof HiveMaterializerTaskFactory);
  }

  public static class MySource extends LoopingDatasetFinderSource<String, String> {
    private final IterableDatasetFinder datasetsFinder;

    public MySource(boolean drilldownIntoPartitions, IterableDatasetFinder datasetsFinder) {
      super(drilldownIntoPartitions);
      this.datasetsFinder = datasetsFinder;
    }

    @Override
    public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException {
      return null;
    }

    @Override
    protected WorkUnit workUnitForDataset(Dataset dataset) {
      WorkUnit workUnit = new WorkUnit();
      workUnit.setProp(DatasetFinderSourceTest.DATASET_URN, dataset.getUrn());
      return workUnit;
    }

    @Override
    protected WorkUnit workUnitForDatasetPartition(PartitionableDataset.DatasetPartition partition) {
      WorkUnit workUnit = new WorkUnit();
      workUnit.setProp(DatasetFinderSourceTest.DATASET_URN, partition.getDataset().getUrn());
      workUnit.setProp(DatasetFinderSourceTest.PARTITION_URN, partition.getUrn());
      return workUnit;
    }

    @Override
    public void shutdown(SourceState state) {

    }

    @Override
    protected IterableDatasetFinder createDatasetsFinder(SourceState state) throws IOException {
      return this.datasetsFinder;
    }
  }

}
