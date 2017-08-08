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

package org.apache.gobblin.runtime;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.local.LocalJobLauncher;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;


/**
 * Unit tests around the state store.
 *
 * <p>
 *   This test uses the {@link LocalJobLauncher} to launch and run a dummy job and checks the
 *   state store between runs of the dummy job to make sure important things like watermarks
 *   are carried over properly between runs.
 * </p>
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.runtime" })
public class DatasetStateStoreTest {

  private static final String JOB_NAME = DatasetStateStoreTest.class.getSimpleName();
  private static final String NAMESPACE = "TestNamespace";
  private static final String TABLE = "TestTable";
  private static final String FOO = "foo";
  private static final String BAR = "bar";
  private static final String WORK_UNIT_INDEX_KEY = "work.unit.index";
  private static final String LAST_READ_RECORD_KEY = "last.read.record";

  private StateStore<JobState.DatasetState> datasetStateStore;
  private Properties jobConfig = new Properties();

  @BeforeClass
  public void setUp() throws Exception {
    Properties properties = new Properties();
    try (FileReader fr = new FileReader("gobblin-test/resource/gobblin.test.properties")) {
      properties.load(fr);
    }

    this.datasetStateStore = new FsStateStore<>(
        properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI),
        properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY), JobState.DatasetState.class);

    // clear data that might be there from a prior run
    this.datasetStateStore.delete(JOB_NAME);

    this.jobConfig.putAll(properties);
    this.jobConfig.setProperty(ConfigurationKeys.JOB_NAME_KEY, JOB_NAME);
    this.jobConfig.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, DummySource.class.getName());
    this.jobConfig.setProperty(ConfigurationKeys.WRITER_BUILDER_CLASS, DummyDataWriterBuilder.class.getName());
  }

  @Test
  public void testLaunchFirstJob() throws Exception {
    try (JobLauncher launcher = new LocalJobLauncher(this.jobConfig)) {
      launcher.launchJob(null);
    }
    verifyJobState(1);
  }

  @Test(dependsOnMethods = "testLaunchFirstJob")
  public void testLaunchSecondJob() throws Exception {
    try (JobLauncher launcher = new LocalJobLauncher(this.jobConfig)) {
      launcher.launchJob(null);
    }
    verifyJobState(2);
  }

  @Test(dependsOnMethods = "testLaunchSecondJob")
  public void testLaunchThirdJob() throws Exception {
    try (JobLauncher launcher = new LocalJobLauncher(this.jobConfig)) {
      launcher.launchJob(null);
    }
    verifyJobState(3);
  }

  @AfterClass
  public void tearDown() throws IOException {
    this.datasetStateStore.delete(JOB_NAME);
  }

  private void verifyJobState(int run) throws IOException {
    List<JobState.DatasetState> datasetStateList = this.datasetStateStore.getAll(JOB_NAME, "current.jst");
    Assert.assertEquals(datasetStateList.size(), 1);

    JobState jobState = datasetStateList.get(0);
    Assert.assertEquals(jobState.getState(), JobState.RunningState.COMMITTED);
    Assert.assertEquals(jobState.getTaskStates().size(), DummySource.NUM_WORK_UNITS);

    for (TaskState taskState : jobState.getTaskStates()) {
      Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.COMMITTED);
      Assert.assertEquals(taskState.getProp(FOO), BAR);

      // Check if the low watermark is properly kept track of
      int expectedLowWatermark =
          (run - 1) * DummySource.NUM_WORK_UNITS * DummySource.NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR
              + taskState.getPropAsInt(WORK_UNIT_INDEX_KEY) * DummySource.NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR + 1;
      Assert.assertEquals(taskState.getPropAsInt(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY), expectedLowWatermark);

      // Check if the high watermark is properly kept track of
      int expectedHighWatermark = expectedLowWatermark + DummySource.NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR - 1;
      Assert.assertEquals(taskState.getPropAsInt(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY),
          expectedHighWatermark);

      Assert.assertEquals(taskState.getPropAsInt(LAST_READ_RECORD_KEY), expectedHighWatermark);
    }
  }

  /**
   * A dummy implementation of {@link Source}.
   */
  public static class DummySource extends AbstractSource<String, Integer> {

    private static final int NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR = 1000;
    private static final int NUM_WORK_UNITS = 5;

    @Override
    public List<WorkUnit> getWorkunits(SourceState sourceState) {
      sourceState.setProp(FOO, BAR);

      if (Iterables.isEmpty(sourceState.getPreviousWorkUnitStates())) {
        return initializeWorkUnits();
      }

      List<WorkUnit> workUnits = Lists.newArrayList();
      for (WorkUnitState workUnitState : sourceState.getPreviousWorkUnitStates()) {
        WorkUnit workUnit = WorkUnit.create(createExtract(Extract.TableType.SNAPSHOT_ONLY, NAMESPACE, TABLE));
        workUnit.setLowWaterMark(workUnitState.getPropAsInt(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY)
            + NUM_WORK_UNITS * NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR);
        workUnit.setHighWaterMark(workUnitState.getPropAsInt(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY)
            + NUM_WORK_UNITS * NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR);
        workUnit.setProp(WORK_UNIT_INDEX_KEY, workUnitState.getPropAsInt(WORK_UNIT_INDEX_KEY));
        workUnits.add(workUnit);
      }

      return workUnits;
    }

    @Override
    public Extractor<String, Integer> getExtractor(WorkUnitState state) throws IOException {
      return new DummyExtractor(state);
    }

    @Override
    public void shutdown(SourceState state) {
      // Nothing to do
    }

    private List<WorkUnit> initializeWorkUnits() {
      List<WorkUnit> workUnits = Lists.newArrayList();
      for (int i = 0; i < NUM_WORK_UNITS; i++) {
        WorkUnit workUnit = WorkUnit.create(createExtract(Extract.TableType.SNAPSHOT_ONLY, NAMESPACE, TABLE));
        workUnit.setLowWaterMark(i * NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR + 1);
        workUnit.setHighWaterMark((i + 1) * NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR);
        workUnit.setProp(WORK_UNIT_INDEX_KEY, i);
        workUnits.add(workUnit);
      }
      return workUnits;
    }
  }

  /**
   * A dummy implementation of {@link Extractor}.
   */
  private static class DummyExtractor implements Extractor<String, Integer> {

    private final WorkUnitState workUnitState;
    private int current;

    DummyExtractor(WorkUnitState workUnitState) {
      this.workUnitState = workUnitState;
      workUnitState.setProp(FOO, BAR);
      this.current = Integer.parseInt(this.workUnitState.getProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY));
    }

    @Override
    public String getSchema() {
      return "";
    }

    @Override
    public Integer readRecord(Integer reuse) throws DataRecordException, IOException {
      if (this.current > this.workUnitState.getPropAsInt(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY)) {
        return null;
      }
      this.workUnitState.setProp(LAST_READ_RECORD_KEY, this.current);
      return this.current++;
    }

    @Override
    public long getExpectedRecordCount() {
      return DummySource.NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR;
    }

    @Override
    public long getHighWatermark() {
      return this.workUnitState.getHighWaterMark();
    }

    @Override
    public void close() throws IOException {
      // Nothing to do
    }
  }

  /**
   * A dummy implementation of {@link DataWriterBuilder} to work with {@link DummySource}.
   */
  public static class DummyDataWriterBuilder extends DataWriterBuilder<String, Integer> {

    @Override
    public DataWriter<Integer> build() throws IOException {
      return new DummyDataWriter();
    }
  }

  /**
   * A dummy implementation of {@link DataWriter} to work with {@link DummySource}.
   */
  private static class DummyDataWriter implements DataWriter<Integer> {

    @Override
    public void write(Integer record) throws IOException {
      // Nothing to do
    }

    @Override
    public void commit() throws IOException {
      // Nothing to do
    }

    @Override
    public void cleanup() throws IOException {
      // Nothing to do
    }

    @Override
    public long recordsWritten() {
      return DummySource.NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR;
    }

    @Override
    public long bytesWritten() throws IOException {
      return DummySource.NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR * 4;
    }

    @Override
    public void close() throws IOException {
      // Nothing to do
    }
  }
}
