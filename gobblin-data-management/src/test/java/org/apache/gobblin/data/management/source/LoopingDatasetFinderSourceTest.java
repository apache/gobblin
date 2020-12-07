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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.hadoop.conf.Configuration;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.gobblin.dataset.PartitionableDataset;
import org.apache.gobblin.dataset.test.SimpleDatasetForTesting;
import org.apache.gobblin.dataset.test.SimpleDatasetPartitionForTesting;
import org.apache.gobblin.dataset.test.SimplePartitionableDatasetForTesting;
import org.apache.gobblin.dataset.test.StaticDatasetsFinderForTesting;
import org.apache.gobblin.runtime.FsDatasetStateStore;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoopingDatasetFinderSourceTest {
  private static final String TEST_JOB_NAME_1 = "TestJob1";
  private static final String TEST_JOB_NAME_2 = "TestJob2";
  private static final String TEST_JOB_ID = "TestJob11";
  private static final String TEST_TASK_ID_PREFIX = "TestTask-";
  private static final String TEST_STATE_STORE_ROOT_DIR = "/tmp/LoopingSourceTest";

  private FsDatasetStateStore fsDatasetStateStore;
  private long startTime = System.currentTimeMillis();

  @BeforeClass
  public void setUp()
      throws IOException {
    this.fsDatasetStateStore = new FsDatasetStateStore(ConfigurationKeys.LOCAL_FS_URI, TEST_STATE_STORE_ROOT_DIR);

    // clear data that may have been left behind by a prior test run
    this.fsDatasetStateStore.delete(TEST_JOB_NAME_1);
    this.fsDatasetStateStore.delete(TEST_JOB_NAME_2);
  }

  @Test
  public void testNonDrilldown() {
    Dataset dataset1 = new SimpleDatasetForTesting("dataset1");
    Dataset dataset2 = new SimplePartitionableDatasetForTesting("dataset2",
        Lists.newArrayList(new SimpleDatasetPartitionForTesting("p1"), new SimpleDatasetPartitionForTesting("p2")));
    Dataset dataset3 = new SimpleDatasetForTesting("dataset3");
    Dataset dataset4 = new SimpleDatasetForTesting("dataset4");
    Dataset dataset5 = new SimpleDatasetForTesting("dataset5");

    IterableDatasetFinder finder =
        new StaticDatasetsFinderForTesting(Lists.newArrayList(dataset5, dataset4, dataset3, dataset2, dataset1));

    MySource mySource = new MySource(false, finder);

    SourceState sourceState = new SourceState();
    sourceState.setProp(LoopingDatasetFinderSource.MAX_WORK_UNITS_PER_RUN_KEY, 3);

    WorkUnitStream workUnitStream = mySource.getWorkunitStream(sourceState);
    List<WorkUnit> workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 4);
    verifyWorkUnitState(workUnits, "dataset3", null, false, false);

    // Second run should continue where it left off
    List<WorkUnitState> workUnitStates = workUnits.stream().map(WorkUnitState::new).collect(Collectors.toList());
    SourceState sourceStateSpy = Mockito.spy(sourceState);
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates();

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 3);
    verifyWorkUnitState(workUnits, "dataset5", null, true, false);

    // Loop around
    workUnitStates = workUnits.stream().map(WorkUnitState::new).collect(Collectors.toList());
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates();

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 4);
    verifyWorkUnitState(workUnits, "dataset3", null, false, false);
  }

  @Test
  public void testDrilldown() {
    // Create three datasets, two of them partitioned
    Dataset dataset1 = new SimpleDatasetForTesting("dataset1");
    Dataset dataset2 = new SimplePartitionableDatasetForTesting("dataset2", Lists
        .newArrayList(new SimpleDatasetPartitionForTesting("p1"), new SimpleDatasetPartitionForTesting("p2"),
            new SimpleDatasetPartitionForTesting("p3")));
    Dataset dataset3 = new SimplePartitionableDatasetForTesting("dataset3", Lists
        .newArrayList(new SimpleDatasetPartitionForTesting("p1"), new SimpleDatasetPartitionForTesting("p2"),
            new SimpleDatasetPartitionForTesting("p3")));

    IterableDatasetFinder finder = new StaticDatasetsFinderForTesting(Lists.newArrayList(dataset3, dataset2, dataset1));

    MySource mySource = new MySource(true, finder);

    // Limit to 3 wunits per run
    SourceState sourceState = new SourceState();
    sourceState.setProp(LoopingDatasetFinderSource.MAX_WORK_UNITS_PER_RUN_KEY, 3);

    // first run, get three first work units
    WorkUnitStream workUnitStream = mySource.getWorkunitStream(sourceState);
    List<WorkUnit> workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 4);
    verifyWorkUnitState(workUnits, "dataset2", "p2", false, false);

    // Second run should continue where it left off
    List<WorkUnitState> workUnitStates = workUnits.stream().map(WorkUnitState::new).collect(Collectors.toList());
    SourceState sourceStateSpy = Mockito.spy(sourceState);
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates();

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 4);
    verifyWorkUnitState(workUnits, "dataset3", "p2", false, false);

    // third run, continue from where it left off
    workUnitStates = workUnits.stream().map(WorkUnitState::new).collect(Collectors.toList());
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates();

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 2);
    verifyWorkUnitState(workUnits, "dataset3", "p3", true, false);

    // fourth run, finished all work units, loop around
    workUnitStates = workUnits.stream().map(WorkUnitState::new).collect(Collectors.toList());
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates();

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 4);
    verifyWorkUnitState(workUnits, "dataset2", "p2", false, false);
  }

  @Test
  public void testNonDrilldownDatasetState()
      throws IOException {
    Dataset dataset1 = new SimpleDatasetForTesting("dataset1");
    Dataset dataset2 = new SimplePartitionableDatasetForTesting("dataset2",
        Lists.newArrayList(new SimpleDatasetPartitionForTesting("p1"), new SimpleDatasetPartitionForTesting("p2")));
    Dataset dataset3 = new SimpleDatasetForTesting("dataset3");
    Dataset dataset4 = new SimpleDatasetForTesting("dataset4");
    Dataset dataset5 = new SimpleDatasetForTesting("dataset5");

    IterableDatasetFinder finder =
        new StaticDatasetsFinderForTesting(Lists.newArrayList(dataset5, dataset4, dataset3, dataset2, dataset1));

    MySource mySource = new MySource(false, finder, fsDatasetStateStore, TEST_JOB_NAME_1);

    SourceState sourceState = new SourceState();
    sourceState.setProp(LoopingDatasetFinderSource.MAX_WORK_UNITS_PER_RUN_KEY, 3);
    sourceState.setProp(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, TEST_STATE_STORE_ROOT_DIR);
    sourceState.setProp(ConfigurationKeys.JOB_NAME_KEY, TEST_JOB_NAME_1);
    WorkUnitStream workUnitStream = mySource.getWorkunitStream(sourceState, true);
    List<WorkUnit> workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 4);
    List<LongWatermark> watermarks1 = new ArrayList<>();
    List<Dataset> datasets1 = new ArrayList<>();
    Assert.assertEquals(workUnits.get(0).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset1");
    Assert.assertEquals(workUnits.get(0).getLowWatermark(LongWatermark.class).getValue(), 0);
    watermarks1.add(workUnits.get(0).getExpectedHighWatermark(LongWatermark.class));
    datasets1.add(dataset1);

    Assert.assertEquals(workUnits.get(1).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset2");
    Assert.assertEquals(workUnits.get(1).getLowWatermark(LongWatermark.class).getValue(), 0);
    watermarks1.add(workUnits.get(1).getExpectedHighWatermark(LongWatermark.class));
    datasets1.add(dataset2);

    Assert.assertEquals(workUnits.get(2).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset3");
    Assert.assertEquals(workUnits.get(2).getLowWatermark(LongWatermark.class).getValue(), 0);
    watermarks1.add(workUnits.get(2).getExpectedHighWatermark(LongWatermark.class));
    datasets1.add(dataset3);

    Assert.assertEquals(workUnits.get(3).getProp(ConfigurationKeys.DATASET_URN_KEY),
        ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);

    Dataset globalWmDataset = new SimpleDatasetForTesting(ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);
    datasets1.add(globalWmDataset);

    verifyWorkUnitState(workUnits,"dataset3", null, false, true);
    persistDatasetState(datasets1, watermarks1, TEST_JOB_NAME_1);
    testDatasetStates(datasets1, watermarks1, TEST_JOB_NAME_1);

    // Second run should continue where it left off
    List<LongWatermark> watermarks2 = new ArrayList<>();
    List<Dataset> datasets2 = new ArrayList<>();

    int workUnitSize = workUnits.size();
    List<WorkUnitState> workUnitStates =
        workUnits.subList(workUnitSize - 1, workUnitSize).stream().map(WorkUnitState::new).collect(Collectors.toList());
    SourceState sourceStateSpy = Mockito.spy(sourceState);
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates(ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy,true);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 3);
    Assert.assertEquals(workUnits.get(0).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset4");
    Assert.assertEquals(workUnits.get(0).getLowWatermark(LongWatermark.class).getValue(), 0);
    watermarks2.add(workUnits.get(0).getExpectedHighWatermark(LongWatermark.class));
    datasets2.add(dataset4);

    Assert.assertEquals(workUnits.get(1).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset5");
    Assert.assertEquals(workUnits.get(1).getLowWatermark(LongWatermark.class).getValue(), 0);
    watermarks2.add(workUnits.get(1).getExpectedHighWatermark(LongWatermark.class));
    datasets2.add(dataset5);

    Assert.assertTrue(workUnits.get(2).getPropAsBoolean(LoopingDatasetFinderSource.END_OF_DATASETS_KEY));
    Assert.assertEquals(workUnits.get(2).getProp(ConfigurationKeys.DATASET_URN_KEY), ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);
    datasets2.add(globalWmDataset);

    verifyWorkUnitState(workUnits,"dataset5",null,true, true);
    persistDatasetState(datasets2, watermarks2, TEST_JOB_NAME_1);
    testDatasetStates(datasets2, watermarks2, TEST_JOB_NAME_1);

    // Loop around
    List<LongWatermark> watermarks3 = new ArrayList<>();
    List<Dataset> datasets3 = new ArrayList<>();

    workUnitSize = workUnits.size();
    workUnitStates = workUnits.subList(workUnitSize - 1, workUnitSize).stream().map(WorkUnitState::new).collect(Collectors.toList());
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates(ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy,true);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 4);
    Assert.assertEquals(workUnits.get(0).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset1");
    Assert.assertEquals(workUnits.get(0).getLowWatermark(LongWatermark.class).getValue(), watermarks1.get(0).getValue());
    watermarks3.add(workUnits.get(0).getExpectedHighWatermark(LongWatermark.class));
    datasets3.add(dataset1);

    Assert.assertEquals(workUnits.get(1).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset2");
    Assert.assertEquals(workUnits.get(1).getLowWatermark(LongWatermark.class).getValue(), watermarks1.get(1).getValue());
    watermarks3.add(workUnits.get(1).getExpectedHighWatermark(LongWatermark.class));
    datasets3.add(dataset2);

    Assert.assertEquals(workUnits.get(2).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset3");
    Assert.assertEquals(workUnits.get(2).getLowWatermark(LongWatermark.class).getValue(), watermarks1.get(2).getValue());
    watermarks3.add(workUnits.get(2).getExpectedHighWatermark(LongWatermark.class));
    datasets3.add(dataset3);

    Assert.assertEquals(workUnits.get(3).getProp(ConfigurationKeys.DATASET_URN_KEY), ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);
    datasets3.add(globalWmDataset);

    verifyWorkUnitState(workUnits,"dataset3",null,false, true);
    persistDatasetState(datasets3, watermarks3, TEST_JOB_NAME_1);
    testDatasetStates(datasets3, watermarks3, TEST_JOB_NAME_1);
  }

  @Test
  public void testDrilldownDatasetState()
      throws IOException {
    // Create three datasets, two of them partitioned
    Dataset dataset1 = new SimpleDatasetForTesting("dataset1");
    Dataset dataset2 = new SimplePartitionableDatasetForTesting("dataset2", Lists
        .newArrayList(new SimpleDatasetPartitionForTesting("p1"), new SimpleDatasetPartitionForTesting("p2"),
            new SimpleDatasetPartitionForTesting("p3")));
    Dataset dataset3 = new SimplePartitionableDatasetForTesting("dataset3", Lists
        .newArrayList(new SimpleDatasetPartitionForTesting("p1"), new SimpleDatasetPartitionForTesting("p2"),
            new SimpleDatasetPartitionForTesting("p3")));

    IterableDatasetFinder finder = new StaticDatasetsFinderForTesting(Lists.newArrayList(dataset3, dataset2, dataset1));

    MySource mySource = new MySource(true, finder, fsDatasetStateStore, TEST_JOB_NAME_2);

    // Limit to 3 wunits per run
    SourceState sourceState = new SourceState();
    sourceState.setProp(LoopingDatasetFinderSource.MAX_WORK_UNITS_PER_RUN_KEY, 3);
    sourceState.setProp(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, TEST_STATE_STORE_ROOT_DIR);
    sourceState.setProp(ConfigurationKeys.JOB_NAME_KEY, TEST_JOB_NAME_2);

    // first run, get three first work units
    WorkUnitStream workUnitStream = mySource.getWorkunitStream(sourceState,true);
    List<WorkUnit> workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    List<LongWatermark> watermarks1 = new ArrayList<>();
    List<Dataset> datasets1 = new ArrayList<>();

    Assert.assertEquals(workUnits.size(), 4);
    Assert.assertEquals(workUnits.get(0).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset1");
    Assert.assertEquals(workUnits.get(0).getLowWatermark(LongWatermark.class).getValue(), 0);
    watermarks1.add(workUnits.get(0).getExpectedHighWatermark(LongWatermark.class));
    datasets1.add(dataset1);

    Assert.assertEquals(workUnits.get(1).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset2@p1");
    Assert.assertEquals(workUnits.get(1).getLowWatermark(LongWatermark.class).getValue(), 0);
    watermarks1.add(workUnits.get(1).getExpectedHighWatermark(LongWatermark.class));
    datasets1.add(new SimpleDatasetForTesting("dataset2@p1"));

    Assert.assertEquals(workUnits.get(2).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset2@p2");
    Assert.assertEquals(workUnits.get(2).getLowWatermark(LongWatermark.class).getValue(), 0);
    watermarks1.add(workUnits.get(2).getExpectedHighWatermark(LongWatermark.class));
    datasets1.add(new SimpleDatasetForTesting("dataset2@p2"));

    Assert.assertEquals(workUnits.get(3).getProp(ConfigurationKeys.DATASET_URN_KEY), ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);
    Assert.assertEquals(workUnits.get(3).getProp(LoopingDatasetFinderSource.DATASET_URN), "dataset2");
    Assert.assertEquals(workUnits.get(3).getProp(LoopingDatasetFinderSource.PARTITION_URN), "p2");
    Dataset globalWmDataset = new SimpleDatasetForTesting(ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);
    datasets1.add(globalWmDataset);

    verifyWorkUnitState(workUnits,"dataset2","p2",false, true);
    persistDatasetState(datasets1, watermarks1, TEST_JOB_NAME_2);
    testDatasetStates(datasets1, watermarks1, TEST_JOB_NAME_2);

    // Second run should continue where it left off
    int workUnitSize = workUnits.size();
    List<WorkUnitState> workUnitStates =
        workUnits.subList(workUnitSize - 1, workUnitSize).stream().map(WorkUnitState::new).collect(Collectors.toList());

    List<LongWatermark> watermarks2 = new ArrayList<>();
    List<Dataset> datasets2 = new ArrayList<>();

    SourceState sourceStateSpy = Mockito.spy(sourceState);
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates(ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy,true);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 4);
    Assert.assertEquals(workUnits.get(0).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset2@p3");
    Assert.assertEquals(workUnits.get(0).getLowWatermark(LongWatermark.class).getValue(), 0);
    watermarks2.add(workUnits.get(0).getExpectedHighWatermark(LongWatermark.class));
    datasets2.add(new SimpleDatasetForTesting("dataset2@p3"));

    Assert.assertEquals(workUnits.get(1).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset3@p1");
    Assert.assertEquals(workUnits.get(1).getLowWatermark(LongWatermark.class).getValue(), 0);
    watermarks2.add(workUnits.get(1).getExpectedHighWatermark(LongWatermark.class));
    datasets2.add(new SimpleDatasetForTesting("dataset3@p1"));

    Assert.assertEquals(workUnits.get(2).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset3@p2");
    Assert.assertEquals(workUnits.get(2).getLowWatermark(LongWatermark.class).getValue(), 0);
    watermarks2.add(workUnits.get(2).getExpectedHighWatermark(LongWatermark.class));
    datasets2.add(new SimpleDatasetForTesting("dataset3@p2"));

    Assert.assertEquals(workUnits.get(3).getProp(ConfigurationKeys.DATASET_URN_KEY), ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);
    Assert.assertEquals(workUnits.get(3).getProp(LoopingDatasetFinderSource.DATASET_URN), "dataset3");
    Assert.assertEquals(workUnits.get(3).getProp(LoopingDatasetFinderSource.PARTITION_URN), "p2");
    datasets2.add(globalWmDataset);

    verifyWorkUnitState(workUnits,"dataset3","p2",false, true);
    persistDatasetState(datasets2, watermarks2, TEST_JOB_NAME_2);
    testDatasetStates(datasets2, watermarks2, TEST_JOB_NAME_2);

    // third run, continue from where it left off
    workUnitSize = workUnits.size();
    workUnitStates =
        workUnits.subList(workUnitSize - 1, workUnitSize).stream().map(WorkUnitState::new).collect(Collectors.toList());
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates(ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);

    List<LongWatermark> watermarks3 = new ArrayList<>();
    List<Dataset> datasets3 = new ArrayList<>();

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy,true);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 2);
    Assert.assertEquals(workUnits.get(0).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset3@p3");
    Assert.assertEquals(workUnits.get(0).getLowWatermark(LongWatermark.class).getValue(), 0);
    watermarks3.add(workUnits.get(0).getExpectedHighWatermark(LongWatermark.class));
    datasets3.add(new SimpleDatasetForTesting("dataset3@p3"));

    Assert.assertTrue(workUnits.get(1).getPropAsBoolean(LoopingDatasetFinderSource.END_OF_DATASETS_KEY));
    Assert.assertEquals(workUnits.get(1).getProp(ConfigurationKeys.DATASET_URN_KEY), ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);
    Assert.assertEquals(workUnits.get(1).getProp(LoopingDatasetFinderSource.DATASET_URN), "dataset3");
    Assert.assertEquals(workUnits.get(1).getProp(LoopingDatasetFinderSource.PARTITION_URN), "p3");
    datasets3.add(globalWmDataset);

    verifyWorkUnitState(workUnits,"dataset3","p3",true, true);
    persistDatasetState(datasets3, watermarks3, TEST_JOB_NAME_2);
    testDatasetStates(datasets3, watermarks3, TEST_JOB_NAME_2);

    // fourth run, finished all work units, loop around
    workUnitSize = workUnits.size();
    workUnitStates =
        workUnits.subList(workUnitSize - 1, workUnitSize).stream().map(WorkUnitState::new).collect(Collectors.toList());
    Mockito.doReturn(workUnitStates).when(sourceStateSpy).getPreviousWorkUnitStates(ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);

    List<LongWatermark> watermarks4 = new ArrayList<>();
    List<Dataset> datasets4 = new ArrayList<>();

    workUnitStream = mySource.getWorkunitStream(sourceStateSpy,true);
    workUnits = Lists.newArrayList(workUnitStream.getWorkUnits());

    Assert.assertEquals(workUnits.size(), 4);
    Assert.assertEquals(workUnits.get(0).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset1");
    Assert.assertEquals(workUnits.get(0).getLowWatermark(LongWatermark.class).getValue(), watermarks1.get(0).getValue());
    watermarks4.add(workUnits.get(0).getExpectedHighWatermark(LongWatermark.class));
    datasets4.add(new SimpleDatasetForTesting("dataset1"));

    Assert.assertEquals(workUnits.get(1).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset2@p1");
    Assert.assertEquals(workUnits.get(1).getLowWatermark(LongWatermark.class).getValue(), watermarks1.get(1).getValue());
    watermarks4.add(workUnits.get(1).getExpectedHighWatermark(LongWatermark.class));
    datasets4.add(new SimpleDatasetForTesting("dataset2@p1"));

    Assert.assertEquals(workUnits.get(2).getProp(ConfigurationKeys.DATASET_URN_KEY), "dataset2@p2");
    Assert.assertEquals(workUnits.get(2).getLowWatermark(LongWatermark.class).getValue(), watermarks1.get(2).getValue());
    watermarks4.add(workUnits.get(2).getExpectedHighWatermark(LongWatermark.class));
    datasets4.add(new SimpleDatasetForTesting("dataset2@p2"));

    Assert.assertEquals(workUnits.get(3).getProp(ConfigurationKeys.DATASET_URN_KEY), ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN);
    datasets4.add(new SimpleDatasetForTesting(ConfigurationKeys.GLOBAL_WATERMARK_DATASET_URN));

    verifyWorkUnitState(workUnits,"dataset2","p2",false,true);
    persistDatasetState(datasets4, watermarks4, TEST_JOB_NAME_2);
    testDatasetStates(datasets4, watermarks4, TEST_JOB_NAME_2);
  }

  public void verifyWorkUnitState(List<WorkUnit> workUnits, String datasetUrn, String partitionUrn,
      boolean endOfDatasets, boolean isDatasetStateStoreEnabled) {
    int i;
    for (i = 0; i < workUnits.size() - 1; i++) {
      Assert.assertNull(workUnits.get(i).getProp(LoopingDatasetFinderSource.DATASET_URN));
      Assert.assertNull(workUnits.get(i).getProp(LoopingDatasetFinderSource.PARTITION_URN));
      if(!isDatasetStateStoreEnabled) {
        Assert.assertNull(workUnits.get(i).getProp(ConfigurationKeys.DATASET_URN_KEY));
      }
      Assert.assertNull(workUnits.get(i).getProp(LoopingDatasetFinderSource.GLOBAL_WATERMARK_DATASET_KEY));
      Assert.assertNull(workUnits.get(i).getProp(LoopingDatasetFinderSource.END_OF_DATASETS_KEY));
    }
    Assert.assertEquals(workUnits.get(i).getProp(LoopingDatasetFinderSource.DATASET_URN), datasetUrn);
    if (partitionUrn != null) {
      Assert.assertEquals(workUnits.get(i).getProp(LoopingDatasetFinderSource.PARTITION_URN), partitionUrn);
    } else {
      Assert.assertNull(workUnits.get(i).getProp(LoopingDatasetFinderSource.PARTITION_URN));
    }
    if (!endOfDatasets) {
      Assert.assertNull(workUnits.get(i).getProp(LoopingDatasetFinderSource.END_OF_DATASETS_KEY));
    } else {
      Assert.assertTrue(workUnits.get(i).getPropAsBoolean(LoopingDatasetFinderSource.END_OF_DATASETS_KEY));
    }
    Assert
        .assertEquals(workUnits.get(i).getPropAsBoolean(LoopingDatasetFinderSource.GLOBAL_WATERMARK_DATASET_KEY), true);
  }

  public void persistDatasetState(List<Dataset> datasets, List<LongWatermark> watermarks, String jobName)
      throws IOException {
    Preconditions.checkArgument(datasets.size() >= 2);
    for (int i = 0; i < datasets.size(); i++) {
      String datasetUrn = datasets.get(i).getUrn();
      JobState.DatasetState datasetState = new JobState.DatasetState(jobName, TEST_JOB_ID);

      datasetState.setDatasetUrn(datasetUrn);
      datasetState.setState(JobState.RunningState.COMMITTED);
      datasetState.setId(datasetUrn);
      datasetState.setStartTime(this.startTime);
      datasetState.setEndTime(this.startTime + 1000);
      datasetState.setDuration(1000);

      TaskState taskState = new TaskState();
      taskState.setJobId(TEST_JOB_ID);
      taskState.setTaskId(TEST_TASK_ID_PREFIX + i);
      taskState.setId(TEST_TASK_ID_PREFIX + i);
      taskState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      if (i < datasets.size() - 1) {
        taskState.setActualHighWatermark(watermarks.get(i));
      }
      datasetState.addTaskState(taskState);

      this.fsDatasetStateStore.persistDatasetState(datasetUrn, datasetState);
    }
  }

  private void testDatasetStates(List<Dataset> datasets, List<LongWatermark> watermarks, String jobName)
      throws IOException {
    Preconditions.checkArgument(datasets.size() >= 2);
    for (int i = 0; i < datasets.size(); i++) {
      JobState.DatasetState datasetState =
          this.fsDatasetStateStore.getLatestDatasetState(jobName, datasets.get(i).getUrn());

      Assert.assertEquals(datasetState.getDatasetUrn(), datasets.get(i).getUrn());
      Assert.assertEquals(datasetState.getJobName(), jobName);
      Assert.assertEquals(datasetState.getJobId(), TEST_JOB_ID);
      Assert.assertEquals(datasetState.getState(), JobState.RunningState.COMMITTED);
      Assert.assertEquals(datasetState.getStartTime(), this.startTime);
      Assert.assertEquals(datasetState.getEndTime(), this.startTime + 1000);
      Assert.assertEquals(datasetState.getDuration(), 1000);

      Assert.assertEquals(datasetState.getCompletedTasks(), 1);
      TaskState taskState = datasetState.getTaskStates().get(0);
      Assert.assertEquals(taskState.getJobId(), TEST_JOB_ID);
      Assert.assertEquals(taskState.getTaskId(), TEST_TASK_ID_PREFIX + i);
      Assert.assertEquals(taskState.getId(), TEST_TASK_ID_PREFIX + i);
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      if (i < datasets.size() - 1) {
        Assert.assertEquals(taskState.getActualHighWatermark(LongWatermark.class).getValue(),
            watermarks.get(i).getValue());
      }
    }
  }

  public static class MySource extends LoopingDatasetFinderSource<String, String> {
    private final IterableDatasetFinder datasetsFinder;
    private boolean isDatasetStateStoreEnabled;
    private DatasetStateStore fsDatasetStateStore;
    private String jobName;
    private Long LAST_PROCESSED_TS = System.currentTimeMillis();

    MySource(boolean drilldownIntoPartitions, IterableDatasetFinder datasetsFinder) {
      super(drilldownIntoPartitions);
      this.datasetsFinder = datasetsFinder;
      this.isDatasetStateStoreEnabled = false;
    }

    MySource(boolean drilldownIntoPartitions, IterableDatasetFinder datasetsFinder,
        FsDatasetStateStore fsDatasetStateStore, String jobName) {
      super(drilldownIntoPartitions);
      this.datasetsFinder = datasetsFinder;
      this.isDatasetStateStoreEnabled = true;
      this.fsDatasetStateStore = fsDatasetStateStore;
      this.jobName = jobName;
    }

    @Override
    public Extractor<String, String> getExtractor(WorkUnitState state)
        throws IOException {
      return null;
    }

    @Override
    protected WorkUnit workUnitForDataset(Dataset dataset) {
      WorkUnit workUnit = new WorkUnit();
      if(isDatasetStateStoreEnabled) {
        JobState.DatasetState datasetState = null;
        try {
          datasetState =
              (JobState.DatasetState) this.fsDatasetStateStore.getLatestDatasetState(this.jobName, dataset.getUrn());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        LongWatermark previousWatermark;
        if(datasetState != null) {
          previousWatermark = datasetState.getTaskStatesAsWorkUnitStates().get(0).getActualHighWatermark(LongWatermark.class);
        } else {
          previousWatermark = new LongWatermark(0);
        }
        workUnit.setWatermarkInterval(new WatermarkInterval(previousWatermark, new LongWatermark(LAST_PROCESSED_TS)));
      }
      return workUnit;
    }

    @Override
    protected WorkUnit workUnitForDatasetPartition(PartitionableDataset.DatasetPartition partition) {
      WorkUnit workUnit = new WorkUnit();
      if(isDatasetStateStoreEnabled) {
        String datasetUrn = partition.getDataset().getUrn()+"@"+partition.getUrn();
        JobState.DatasetState datasetState = null;
        try {
          datasetState =
              (JobState.DatasetState) this.fsDatasetStateStore.getLatestDatasetState(this.jobName, datasetUrn);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        LongWatermark previousWatermark;
        if(datasetState != null) {
          previousWatermark = datasetState.getTaskStatesAsWorkUnitStates().get(0).getActualHighWatermark(LongWatermark.class);
        } else {
          previousWatermark = new LongWatermark(0);
        }
        workUnit.setWatermarkInterval(new WatermarkInterval(previousWatermark, new LongWatermark(LAST_PROCESSED_TS)));
      }
      return workUnit;
    }

    @Override
    public void shutdown(SourceState state) {

    }

    @Override
    protected IterableDatasetFinder createDatasetsFinder(SourceState state)
        throws IOException {
      return this.datasetsFinder;
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration(false));
    Path rootDir = new Path(TEST_STATE_STORE_ROOT_DIR);
    if (fs.exists(rootDir)) {
      fs.delete(rootDir, true);
    }
  }
}
