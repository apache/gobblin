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
import java.util.List;
import java.util.stream.Stream;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.gobblin.dataset.PartitionableDataset;
import org.apache.gobblin.dataset.test.SimpleDatasetForTesting;
import org.apache.gobblin.dataset.test.SimpleDatasetPartitionForTesting;
import org.apache.gobblin.dataset.test.SimplePartitionableDatasetForTesting;
import org.apache.gobblin.dataset.test.StaticDatasetsFinderForTesting;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitStream;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;


public class DatasetFinderSourceTest {

  public static final String DATASET_URN = "test.datasetUrn";
  public static final String PARTITION_URN = "test.partitionUrn";

  @Test
  public void testNonDrilledDown() {

    Dataset dataset1 = new SimpleDatasetForTesting("dataset1");
    Dataset dataset2 = new SimplePartitionableDatasetForTesting("dataset2", Lists.newArrayList(new SimpleDatasetPartitionForTesting("p1"), new SimpleDatasetPartitionForTesting("p2")));
    Dataset dataset3 = new SimpleDatasetForTesting("dataset3");

    IterableDatasetFinder finder = new StaticDatasetsFinderForTesting(Lists.newArrayList(dataset1, dataset2, dataset3));

    MySource mySource = new MySource(false, finder);
    List<WorkUnit> workUnits = mySource.getWorkunits(new SourceState());

    Assert.assertEquals(workUnits.size(), 3);
    Assert.assertEquals(workUnits.get(0).getProp(DATASET_URN), "dataset1");
    Assert.assertNull(workUnits.get(0).getProp(PARTITION_URN));
    Assert.assertEquals(workUnits.get(1).getProp(DATASET_URN), "dataset2");
    Assert.assertNull(workUnits.get(1).getProp(PARTITION_URN));
    Assert.assertEquals(workUnits.get(2).getProp(DATASET_URN), "dataset3");
    Assert.assertNull(workUnits.get(2).getProp(PARTITION_URN));

    WorkUnitStream workUnitStream = mySource.getWorkunitStream(new SourceState());

    Assert.assertEquals(Lists.newArrayList(workUnitStream.getWorkUnits()), workUnits);
  }

  @Test
  public void testDrilledDown() {
    Dataset dataset1 = new SimpleDatasetForTesting("dataset1");
    Dataset dataset2 = new SimplePartitionableDatasetForTesting("dataset2", Lists.newArrayList(new SimpleDatasetPartitionForTesting("p1"), new SimpleDatasetPartitionForTesting("p2")));
    Dataset dataset3 = new SimpleDatasetForTesting("dataset3");

    IterableDatasetFinder finder = new StaticDatasetsFinderForTesting(Lists.newArrayList(dataset1, dataset2, dataset3));

    MySource mySource = new MySource(true, finder);
    List<WorkUnit> workUnits = mySource.getWorkunits(new SourceState());

    Assert.assertEquals(workUnits.size(), 4);
    Assert.assertEquals(workUnits.get(0).getProp(DATASET_URN), "dataset1");
    Assert.assertNull(workUnits.get(0).getProp(PARTITION_URN));
    Assert.assertEquals(workUnits.get(1).getProp(DATASET_URN), "dataset2");
    Assert.assertEquals(workUnits.get(1).getProp(PARTITION_URN), "p1");
    Assert.assertEquals(workUnits.get(2).getProp(DATASET_URN), "dataset2");
    Assert.assertEquals(workUnits.get(2).getProp(PARTITION_URN), "p2");
    Assert.assertEquals(workUnits.get(3).getProp(DATASET_URN), "dataset3");
    Assert.assertNull(workUnits.get(3).getProp(PARTITION_URN));

    WorkUnitStream workUnitStream = mySource.getWorkunitStream(new SourceState());

    Assert.assertEquals(Lists.newArrayList(workUnitStream.getWorkUnits()), workUnits);
  }

  public static class MySource extends DatasetFinderSource<String, String> {
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
      workUnit.setProp(DATASET_URN, dataset.getUrn());
      return workUnit;
    }

    @Override
    protected WorkUnit workUnitForDatasetPartition(PartitionableDataset.DatasetPartition partition) {
      WorkUnit workUnit = new WorkUnit();
      workUnit.setProp(DATASET_URN, partition.getDataset().getUrn());
      workUnit.setProp(PARTITION_URN, partition.getUrn());
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
