/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */


package gobblin.runtime;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.metastore.FsStateStore;
import gobblin.metastore.StateStore;
import gobblin.runtime.local.LocalJobLauncher;
import gobblin.source.Source;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;


/**
 * Unit tests around the state store.
 *
 * <p>
 *   This test using the {@link LocalJobLauncher} to launch and run a dummy job and checks the
 *   state store between runs of the dummy job to make sure important things like watermarks
 *   are carried over properly between runs.
 * </p>
 *
 * @author ynli
 */
@Test(groups = {"gobblin.runtime"})
public class JobStateStoreTest {

  private static final String JOB_NAME = JobStateStoreTest.class.getSimpleName();
  private static final String NAMESPACE = "foo";
  private static final String TABLE = "bar";

  private JobLauncher jobLauncher;
  private StateStore<JobState> jobStateStore;
  private Properties jobProps;

  @BeforeClass
  public void setUp() throws Exception {
    Properties properties = new Properties();
    properties.load(new FileReader("gobblin-test/resource/gobblin.test.properties"));
    this.jobLauncher = new LocalJobLauncher(properties);

    this.jobStateStore = new FsStateStore<JobState>(
        properties.getProperty(ConfigurationKeys.STATE_STORE_FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI),
        properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY),
        JobState.class);

    this.jobProps = new Properties();
    this.jobProps.putAll(properties);
    this.jobProps.setProperty(ConfigurationKeys.JOB_NAME_KEY, JOB_NAME);
    this.jobProps.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, DummySource.class.getName());
    this.jobProps.setProperty(ConfigurationKeys.WRITER_BUILDER_CLASS, DummyDataWriterBuilder.class.getName());
  }

  @Test
  public void testLaunchJob() throws Exception {
    this.jobLauncher.launchJob(this.jobProps, null);
  }

  @AfterClass
  public void tearDown() throws IOException {
    this.jobStateStore.delete(JOB_NAME);
  }

  /**
   * A dummy implementation of {@link Source}.
   */
  public static class DummySource extends AbstractSource<String, Integer> {

    private static final int NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR = 1000;
    private static final int NUM_WORK_UNITS = 5;

    @Override
    public List<WorkUnit> getWorkunits(SourceState sourceState) {
      if (sourceState.getPreviousWorkUnitStates().isEmpty()) {
        return initializeWorkUnits(sourceState);
      }

      List<WorkUnit> workUnits = Lists.newArrayList();
      for (WorkUnitState workUnitState : sourceState.getPreviousWorkUnitStates()) {
        WorkUnit workUnit =
            sourceState.createWorkUnit(sourceState.createExtract(Extract.TableType.SNAPSHOT_ONLY, NAMESPACE, TABLE));
        workUnit.setLowWaterMark(workUnitState.getHighWaterMark() + 1);
        workUnit.setHighWaterMark(workUnitState.getHighWaterMark() + NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR);
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

    private List<WorkUnit> initializeWorkUnits(SourceState sourceState) {
      List<WorkUnit> workUnits = Lists.newArrayList();
      for (int i = 0; i < NUM_WORK_UNITS; i++) {
        WorkUnit workUnit =
            sourceState.createWorkUnit(sourceState.createExtract(Extract.TableType.SNAPSHOT_ONLY, NAMESPACE, TABLE));
        workUnit.setLowWaterMark(i * NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR + 1);
        workUnit.setHighWaterMark((i + 1) * NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR);
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
      this.current = Integer.parseInt(this.workUnitState.getProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY));
    }

    @Override
    public String getSchema() {
      return "";
    }

    @Override
    public Integer readRecord(Integer reuse) throws DataRecordException, IOException {
      return this.current > this.workUnitState.getHighWaterMark() ? null : this.current++;
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
