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

package org.apache.gobblin.source.extractor.filebased;

import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.dataset.Descriptor;
import org.apache.gobblin.dataset.PartitionDescriptor;
import org.apache.gobblin.source.DatePartitionedJsonFileSource;
import org.apache.gobblin.source.PartitionedFileSourceBase;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.hadoop.AvroFileSource;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


@Test
public class FileBasedSourceTest {

  private static final String SOURCE_LINEAGE_KEY = "gobblin.event.lineage.source";
  SharedResourcesBroker<GobblinScopeTypes> instanceBroker;
  SharedResourcesBroker<GobblinScopeTypes> jobBroker;
  Path sourceDir;

  @BeforeClass
  public void setup() {
    instanceBroker = SharedResourcesBrokerFactory
        .createDefaultTopLevelBroker(ConfigFactory.empty(), GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    jobBroker = instanceBroker
        .newSubscopedBuilder(new JobScopeInstance("LineageEventTest", String.valueOf(System.currentTimeMillis())))
        .build();
    sourceDir = new Path(getClass().getResource("/source").toString());
  }

  @Test
  public void testFailJobWhenPreviousStateExistsButDoesNotHaveSnapshot() {
    try {
      DummyFileBasedSource source = new DummyFileBasedSource();

      WorkUnitState workUnitState = new WorkUnitState();
      workUnitState.setId("priorState");
      List<WorkUnitState> workUnitStates = Lists.newArrayList(workUnitState);

      State state = new State();
      state.setProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, Extract.TableType.SNAPSHOT_ONLY.toString());
      state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_PRIOR_SNAPSHOT_REQUIRED, true);

      SourceState sourceState = new SourceState(state, workUnitStates);

      source.getWorkunits(sourceState);
      Assert.fail("Expected RuntimeException, but no exceptions were thrown.");
    } catch (RuntimeException e) {
      Assert.assertEquals("No 'source.filebased.fs.snapshot' found on state of prior job", e.getMessage());
    }
  }

  @Test
  void numberOfWorkUnits()
      throws IOException {
    SourceState sourceState = new SourceState();
    sourceState.setBroker(jobBroker);
    DatePartitionedJsonFileSource source = new DatePartitionedJsonFileSource();
    initState(sourceState);
    List<WorkUnit> workUnits = source.getWorkunits(sourceState);
    Assert.assertEquals(3, workUnits.size());
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testFailOnInvalidSourceDirectory() {
    SourceState sourceState = new SourceState();
    sourceState.setBroker(jobBroker);
    AvroFileSource source = new AvroFileSource();
    initState(sourceState);
    Path path = new Path(sourceDir, "testFailOnInvalidSourceDirectory");
    sourceState.setProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY, path.toString());
    source.getWorkunits(sourceState);
  }

  @Test
  public void testSourceLineage() {
    String dataset = Path.getPathWithoutSchemeAndAuthority(sourceDir).toString();

    SourceState sourceState = new SourceState();
    sourceState.setBroker(jobBroker);
    initState(sourceState);

    // Avro file based source
    AvroFileSource fileSource = new AvroFileSource();
    List<WorkUnit> workUnits = fileSource.getWorkunits(sourceState);
    DatasetDescriptor datasetDescriptor = new DatasetDescriptor("hdfs", URI.create("file:///"), dataset);
    for (WorkUnit workUnit : workUnits) {
      Assert.assertEquals(workUnit.getProp(SOURCE_LINEAGE_KEY), Descriptor.toJson(datasetDescriptor));
    }

    // Partitioned file based source
    // Test platform configuration
    sourceState.setProp(ConfigurationKeys.SOURCE_FILEBASED_PLATFORM, DatasetConstants.PLATFORM_FILE);
    DatePartitionedJsonFileSource partitionedFileSource = new DatePartitionedJsonFileSource();
    workUnits = partitionedFileSource.getWorkunits(sourceState);
    datasetDescriptor = new DatasetDescriptor("file", URI.create("file:///"), dataset);

    Set<String> partitions = Sets.newHashSet("2017-12", "2018-01");
    for (WorkUnit workUnit : workUnits) {
      if (workUnit instanceof MultiWorkUnit) {
        DatasetDescriptor finalDatasetDescriptor = datasetDescriptor;
        ((MultiWorkUnit) workUnit).getWorkUnits().forEach( wu -> verifyPartitionSourceLineage(wu, partitions,
            finalDatasetDescriptor));
      } else {
        verifyPartitionSourceLineage(workUnit, partitions, datasetDescriptor);
      }
    }
  }

  private void verifyPartitionSourceLineage(WorkUnit wu, Set<String> partitions, DatasetDescriptor datasetDescriptor) {
    PartitionDescriptor descriptor = (PartitionDescriptor) Descriptor.fromJson(wu.getProp(SOURCE_LINEAGE_KEY));
    Assert.assertTrue(partitions.contains(descriptor.getName()));
    Assert.assertEquals(descriptor.getDataset(), datasetDescriptor);
  }

  private void initState(State state) {
    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY, sourceDir.toString());
    state.setProp(PartitionedFileSourceBase.DATE_PARTITIONED_SOURCE_PARTITION_PATTERN, "yyyy-MM");
    state.setProp(PartitionedFileSourceBase.DATE_PARTITIONED_SOURCE_MIN_WATERMARK_VALUE, "2017-11");
    state.setProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY, "snapshot_only");
    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.SCHEMA_IN_SOURCE_DIR, "true");
    state.setProp(ConfigurationKeys.SCHEMA_FILENAME, "metadata.json");
  }

  @AfterClass
  public void cleanup()
      throws IOException {
    if (jobBroker != null) {
      jobBroker.close();
    }

    if (instanceBroker != null) {
      instanceBroker.close();
    }
  }

  private static class DummyFileBasedSource extends FileBasedSource<String, String> {
    @Override
    public void initFileSystemHelper(State state)
        throws FileBasedHelperException {
    }

    @Override
    protected List<WorkUnit> getPreviousWorkUnitsForRetry(SourceState state) {
      return Lists.newArrayList();
    }

    @Override
    public List<String> getcurrentFsSnapshot(State state) {
      return Lists.newArrayList("SnapshotEntry");
    }

    @Override
    public Extractor<String, String> getExtractor(WorkUnitState state)
        throws IOException {
      return new DummyExtractor();
    }
  }

  private static class DummyExtractor implements Extractor<String, String> {
    @Override
    public String getSchema() {
      return "";
    }

    @Override
    public String readRecord(String reuse)
        throws DataRecordException, IOException {
      return null;
    }

    @Override
    public long getExpectedRecordCount() {
      return 0;
    }

    @Override
    public long getHighWatermark() {
      return 0;
    }

    @Override
    public void close()
        throws IOException {
    }
  }
}
