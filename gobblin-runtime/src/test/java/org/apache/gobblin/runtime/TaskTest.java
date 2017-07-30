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

package gobblin.runtime;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.fork.ForkOperator;
import gobblin.fork.IdentityForkOperator;
import gobblin.publisher.TaskPublisher;
import gobblin.qualitychecker.row.RowLevelPolicyCheckResults;
import gobblin.qualitychecker.row.RowLevelPolicyChecker;
import gobblin.qualitychecker.task.TaskLevelPolicyCheckResults;
import gobblin.qualitychecker.task.TaskLevelPolicyChecker;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;


/**
 * Integration tests for {@link Task}.
 */
@Test
@Slf4j
public class TaskTest {

  TaskState getEmptyTestTaskState(String taskId) {
    // Create a TaskState
    WorkUnit workUnit = WorkUnit.create(
            new Extract(Extract.TableType.SNAPSHOT_ONLY, this.getClass().getName(), this.getClass().getSimpleName()));
    workUnit.setProp(ConfigurationKeys.TASK_KEY_KEY, "taskKey");
    TaskState taskState = new TaskState(new WorkUnitState(workUnit));
    taskState.setProp(ConfigurationKeys.METRICS_ENABLED_KEY, Boolean.toString(false));
    taskState.setTaskId(taskId);
    taskState.setJobId("1234");
    return taskState;
  }

  @DataProvider(name = "stateOverrides")
  public Object[][] createTestsForDifferentExecutionModes() {
    State synchronousStateOverrides = new State();
    synchronousStateOverrides.setProp(ConfigurationKeys.TASK_SYNCHRONOUS_EXECUTION_MODEL_KEY, true);

    State streamStateOverrides = new State();
    streamStateOverrides.setProp(ConfigurationKeys.TASK_SYNCHRONOUS_EXECUTION_MODEL_KEY, false);

    return new Object[][] {
        { synchronousStateOverrides },
        { streamStateOverrides }
    };
  }

  /**
   * Check if a {@link WorkUnitState.WorkingState} of a {@link Task} is set properly after a {@link Task} fails once,
   * but then is successful the next time.
   */
  @Test(dataProvider = "stateOverrides")
  public void testRetryTask(State overrides) throws Exception {
    // Create a TaskState
    TaskState taskState = getEmptyTestTaskState("testRetryTaskId");
    taskState.addAll(overrides);
    // Create a mock TaskContext
    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getExtractor()).thenReturn(new FailOnceExtractor());
    when(mockTaskContext.getForkOperator()).thenReturn(new IdentityForkOperator());
    when(mockTaskContext.getTaskState()).thenReturn(taskState);
    when(mockTaskContext.getTaskLevelPolicyChecker(any(TaskState.class), anyInt()))
        .thenReturn(mock(TaskLevelPolicyChecker.class));
    when(mockTaskContext.getRowLevelPolicyChecker()).
        thenReturn(new RowLevelPolicyChecker(Lists.newArrayList(), "ss", FileSystem.getLocal(new Configuration())));
    when(mockTaskContext.getRowLevelPolicyChecker(anyInt())).
        thenReturn(new RowLevelPolicyChecker(Lists.newArrayList(), "ss", FileSystem.getLocal(new Configuration())));

    // Create a mock TaskPublisher
    TaskPublisher mockTaskPublisher = mock(TaskPublisher.class);
    when(mockTaskPublisher.canPublish()).thenReturn(TaskPublisher.PublisherState.SUCCESS);
    when(mockTaskContext.getTaskPublisher(any(TaskState.class), any(TaskLevelPolicyCheckResults.class)))
        .thenReturn(mockTaskPublisher);

    // Create a mock TaskStateTracker
    TaskStateTracker mockTaskStateTracker = mock(TaskStateTracker.class);

    // Create a TaskExecutor - a real TaskExecutor must be created so a Fork is run in a separate thread
    TaskExecutor taskExecutor = new TaskExecutor(new Properties());

    // Create the Task
    Task realTask = new Task(mockTaskContext, mockTaskStateTracker, taskExecutor, Optional.<CountDownLatch> absent());
    Task task = spy(realTask);
    doNothing().when(task).submitTaskCommittedEvent();

    // The first run of the Task should fail
    task.run();
    task.commit();
    Assert.assertEquals(task.getTaskState().getWorkingState(), WorkUnitState.WorkingState.FAILED);

    // The second run of the Task should succeed
    task.run();
    task.commit();
    Assert.assertEquals(task.getTaskState().getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);
  }

  private TaskContext getMockTaskContext(TaskState taskState, Extractor mockExtractor,
      ArrayList<ArrayList<Object>> writerCollectors, ForkOperator mockForkOperator)
      throws Exception {

    int numForks = writerCollectors.size();

    // Create a mock RowLevelPolicyChecker
    RowLevelPolicyChecker mockRowLevelPolicyChecker =
        spy(new RowLevelPolicyChecker(Lists.newArrayList(), "ss", FileSystem.getLocal(new Configuration())));
    when(mockRowLevelPolicyChecker.executePolicies(any(Object.class), any(RowLevelPolicyCheckResults.class)))
        .thenReturn(true);
    when(mockRowLevelPolicyChecker.getFinalState()).thenReturn(new State());

    // Create a mock TaskPublisher
    TaskPublisher mockTaskPublisher = mock(TaskPublisher.class);
    when(mockTaskPublisher.canPublish()).thenReturn(TaskPublisher.PublisherState.SUCCESS);

    // Create a mock TaskContext
    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getExtractor()).thenReturn(mockExtractor);
    when(mockTaskContext.getRawSourceExtractor()).thenReturn(mockExtractor);
    when(mockTaskContext.getForkOperator()).thenReturn(mockForkOperator);
    when(mockTaskContext.getTaskState()).thenReturn(taskState);
    when(mockTaskContext.getTaskPublisher(any(TaskState.class), any(TaskLevelPolicyCheckResults.class)))
        .thenReturn(mockTaskPublisher);
    when(mockTaskContext.getRowLevelPolicyChecker()).thenReturn(mockRowLevelPolicyChecker);
    when(mockTaskContext.getRowLevelPolicyChecker(anyInt())).thenReturn(mockRowLevelPolicyChecker);
    when(mockTaskContext.getTaskLevelPolicyChecker(any(TaskState.class), anyInt())).thenReturn(mock(TaskLevelPolicyChecker.class));
    for (int i =0; i < numForks; ++i) {
      when(mockTaskContext.getDataWriterBuilder(numForks, i)).thenReturn(new RecordCollectingWriterBuilder(writerCollectors.get(i)));
    }
    return mockTaskContext;
  }

  /**
   * Test that forks work correctly when the operator picks one outgoing fork
   */
  @Test(dataProvider = "stateOverrides")
  public void testForkCorrectnessRoundRobin(State overrides)
      throws Exception {
    // Create a TaskState
    TaskState taskState = getEmptyTestTaskState("testForkTaskId");
    taskState.addAll(overrides);

    int numRecords = 9;
    int numForks = 3;
    ForkOperator mockForkOperator = new RoundRobinForkOperator(numForks);


    // The following code depends on exact multiples
    Assert.assertTrue(numRecords % numForks == 0);

    ArrayList<ArrayList<Object>> recordCollectors = runTaskAndGetResults(taskState, numRecords, numForks, mockForkOperator);

    // Check that we got the right records in the collectors
    int recordsPerFork = numRecords/numForks;
    for (int forkNumber=0; forkNumber < numForks; ++ forkNumber) {
      ArrayList<Object> forkRecords = recordCollectors.get(forkNumber);
      Assert.assertEquals(forkRecords.size(), recordsPerFork);
      for (int j=0; j < recordsPerFork; ++j) {
        Object forkRecord = forkRecords.get(j);
        Assert.assertEquals((String) forkRecord, "" + (j * recordsPerFork + forkNumber));
      }
    }
  }

  /**
   * Test that forks work correctly when the operator picks all outgoing forks
   */
  @Test(dataProvider = "stateOverrides")
  public void testForkCorrectnessIdentity(State overrides)
      throws Exception {
    // Create a TaskState
    TaskState taskState = getEmptyTestTaskState("testForkTaskId");
    taskState.addAll(overrides);

    int numRecords = 100;
    int numForks = 5;

    // Identity Fork Operator looks for number of forks in work unit state.
    taskState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, "" + numForks);
    ForkOperator mockForkOperator = new IdentityForkOperator();

    ArrayList<ArrayList<Object>> recordCollectors = runTaskAndGetResults(taskState, numRecords, numForks, mockForkOperator);

    // Check that we got the right records in the collectors
    int recordsPerFork = numRecords;
    for (int forkNumber=0; forkNumber < numForks; ++ forkNumber) {
      ArrayList<Object> forkRecords = recordCollectors.get(forkNumber);
      Assert.assertEquals(forkRecords.size(), recordsPerFork);
      for (int j=0; j < recordsPerFork; ++j) {
        Object forkRecord = forkRecords.get(j);
        Assert.assertEquals((String) forkRecord, "" + j);
      }
    }
  }


  /**
   * Test that forks work correctly when the operator picks a subset of outgoing forks
   */
  @Test(dataProvider = "stateOverrides")
  public void testForkCorrectnessSubset(State overrides)
      throws Exception {
    // Create a TaskState
    TaskState taskState = getEmptyTestTaskState("testForkTaskId");
    taskState.addAll(overrides);

    int numRecords = 20;
    int numForks = 5;
    int subset = 2;

    ForkOperator mockForkOperator = new SubsetForkOperator(numForks, subset);

    ArrayList<ArrayList<Object>> recordCollectors = runTaskAndGetResults(taskState, numRecords, numForks, mockForkOperator);

    log.info("Records collected: {}", recordCollectors);
    // Check that we got the right records in the collectors
    int totalRecordsExpected = numRecords * subset;
    int totalRecordsFound = 0;
    HashMap<String, ArrayList<Integer>> recordsMap = new HashMap<>();
    for (int forkNumber=0; forkNumber < numForks; ++ forkNumber) {
      ArrayList<Object> forkRecords = recordCollectors.get(forkNumber);
      for (Object forkRecord: forkRecords) {
        String recordAsString = (String) forkRecord;
        totalRecordsFound++;
        if (recordsMap.containsKey(recordAsString)) {
          recordsMap.get(recordAsString).add(forkNumber);
        } else {
          ArrayList<Integer> forksFound = new ArrayList<>();
          forksFound.add(forkNumber);
          recordsMap.put(recordAsString, forksFound);
        }
      }
    }
    Assert.assertEquals(totalRecordsFound, totalRecordsExpected, "Total records");
    for (Map.Entry<String, ArrayList<Integer>> recordForks: recordsMap.entrySet()) {
      Assert.assertEquals(recordForks.getValue().size(), subset);
    }

  }



  private ArrayList<ArrayList<Object>> runTaskAndGetResults(TaskState taskState, int numRecords, int numForks,
      ForkOperator mockForkOperator)
      throws Exception {
    ArrayList<ArrayList<Object>> recordCollectors = new ArrayList<>(numForks);
    for (int i=0; i < numForks; ++i) {
      recordCollectors.add(new ArrayList<Object>());
    }

    TaskContext mockTaskContext = getMockTaskContext(taskState,
        new StringExtractor(numRecords), recordCollectors, mockForkOperator);

    // Create a mock TaskStateTracker
    TaskStateTracker mockTaskStateTracker = mock(TaskStateTracker.class);

    // Create a TaskExecutor - a real TaskExecutor must be created so a Fork is run in a separate thread
    TaskExecutor taskExecutor = new TaskExecutor(new Properties());

    // Create the Task
    Task task = new Task(mockTaskContext, mockTaskStateTracker, taskExecutor, Optional.<CountDownLatch>absent());

    // Run and commit
    task.run();
    task.commit();
    return recordCollectors;
  }

  /**
   * An implementation of {@link Extractor} that throws an {@link IOException} during the invocation of
   * {@link #readRecord(Object)}.
   */
  private static class FailOnceExtractor implements Extractor<Object, Object> {

    private final AtomicBoolean HAS_FAILED = new AtomicBoolean();

    @Override
    public Object getSchema() throws IOException {
      return null;
    }

    @Override
    public Object readRecord(@Deprecated Object reuse) throws DataRecordException, IOException {
      if (!HAS_FAILED.get()) {
        HAS_FAILED.set(true);
        throw new IOException("Injected failure");
      }
      return null;
    }

    @Override
    public long getExpectedRecordCount() {
      return -1;
    }

    @Override
    public long getHighWatermark() {
      return -1;
    }

    @Override
    public void close() throws IOException {
      // Do nothing
    }
  }


  private static class StringExtractor implements Extractor<Object, String> {

    private final int _numRecords;
    private int _currentRecord;
    public StringExtractor(int numRecords) {
      _numRecords = numRecords;
      _currentRecord = -1;
    }

    @Override
    public Object getSchema()
        throws IOException {
      return "";
    }

    @Override
    public String readRecord(@Deprecated String reuse)
        throws DataRecordException, IOException {
      if (_currentRecord < _numRecords-1) {
        _currentRecord++;
        return "" + _currentRecord;
      } else {
        return null;
      }
    }

    @Override
    public long getExpectedRecordCount() {
      return _numRecords;
    }

    @Override
    public long getHighWatermark() {
      return -1;
    }

    @Override
    public void close()
        throws IOException {

    }
  }

  private static class RoundRobinForkOperator implements ForkOperator<Object, Object> {

    private final int _numForks;
    private final Boolean[] _forkedSchemas;
    private final Boolean[] _forkedRecords;
    private int _lastForkTaken;

    public RoundRobinForkOperator(int numForks) {
      _numForks = numForks;
      _forkedSchemas = new Boolean[_numForks];
      _forkedRecords = new Boolean[_numForks];
      _lastForkTaken = _numForks-1;
      for (int i=0; i < _numForks; ++i) {
        _forkedSchemas[i] = Boolean.TRUE;
        _forkedRecords[i] = Boolean.FALSE;
      }
    }

    @Override
    public void init(WorkUnitState workUnitState)
        throws Exception {
    }

    @Override
    public int getBranches(WorkUnitState workUnitState) {
      return _numForks;
    }

    @Override
    public List<Boolean> forkSchema(WorkUnitState workUnitState, Object input) {
      return Arrays.asList(_forkedSchemas);
    }

    @Override
    public List<Boolean> forkDataRecord(WorkUnitState workUnitState, Object input) {
      _forkedRecords[_lastForkTaken] = Boolean.FALSE;
      _lastForkTaken = (_lastForkTaken+1)%_numForks;
      _forkedRecords[_lastForkTaken] = Boolean.TRUE;
      return Arrays.asList(_forkedRecords);
    }

    @Override
    public void close()
        throws IOException {

    }
  }

  private static class SubsetForkOperator implements ForkOperator<Object, Object> {

    private final int _numForks;
    private final int _subsetSize;
    private final Boolean[] _forkedSchemas;
    private final Boolean[] _forkedRecords;
    private final Random _random;

    public SubsetForkOperator(int numForks, int subsetSize) {
      Preconditions.checkArgument(subsetSize >=0 && subsetSize <= numForks,
          "Subset size should be in range [0, numForks]");
      _numForks = numForks;
      _subsetSize = subsetSize;
      _forkedSchemas = new Boolean[_numForks];
      _forkedRecords = new Boolean[_numForks];
      _random = new Random();
      for (int i=0; i < _numForks; ++i) {
        _forkedSchemas[i] = Boolean.TRUE;
        _forkedRecords[i] = Boolean.FALSE;
      }
    }

    @Override
    public void init(WorkUnitState workUnitState)
        throws Exception {
    }

    @Override
    public int getBranches(WorkUnitState workUnitState) {
      return _numForks;
    }

    @Override
    public List<Boolean> forkSchema(WorkUnitState workUnitState, Object input) {
      return Arrays.asList(_forkedSchemas);
    }

    @Override
    public List<Boolean> forkDataRecord(WorkUnitState workUnitState, Object input) {

      for (int i=0; i < _numForks; ++i) {
        _forkedRecords[i] = Boolean.FALSE;
      }

      // Really lazy way of getting a random subset, not intended for production use
      int chosenRecords = 0;
      while (chosenRecords != _subsetSize) {
        int index = _random.nextInt(_numForks);
        if (!_forkedRecords[index]) {
          _forkedRecords[index] = Boolean.TRUE;
          chosenRecords++;
        }
      }
      return Arrays.asList(_forkedRecords);
    }

    @Override
    public void close()
        throws IOException {

    }
  }

  private class RecordCollectingWriterBuilder extends DataWriterBuilder {
    private final ArrayList<Object> _recordSink;

    public RecordCollectingWriterBuilder(ArrayList<Object> objects) {
      super();
      _recordSink = objects;
    }

    @Override
    public DataWriter build()
        throws IOException {
      return new DataWriter() {
        @Override
        public void write(Object record)
            throws IOException {
          _recordSink.add(record);
        }

        @Override
        public void commit()
            throws IOException {

        }

        @Override
        public void cleanup()
            throws IOException {

        }

        @Override
        public long recordsWritten() {
          return _recordSink.size();
        }

        @Override
        public long bytesWritten()
            throws IOException {
          return -1;
        }

        @Override
        public void close()
            throws IOException {

        }
      };
    }
  }
}
