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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.fork.ForkOperator;
import org.apache.gobblin.fork.IdentityForkOperator;
import org.apache.gobblin.publisher.TaskPublisher;
import org.apache.gobblin.qualitychecker.row.RowLevelPolicyCheckResults;
import org.apache.gobblin.qualitychecker.row.RowLevelPolicyChecker;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicyCheckResults;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicyChecker;
import org.apache.gobblin.runtime.util.TaskMetrics;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


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
    when(mockTaskContext.getTaskMetrics()).thenReturn(TaskMetrics.get(taskState));
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
   * Test the addition of a task timestamp to the file name
   */
  @Test
  public void testTimestampInFilename()
      throws Exception {
    // Create a TaskState
    TaskState taskState = getEmptyTestTaskState("testTimestampInFilename");
    taskState.setProp(ConfigurationKeys.TASK_START_TIME_MILLIS_KEY, "12345");
    taskState.setProp(ConfigurationKeys.WRITER_ADD_TASK_TIMESTAMP, "true");

    int numRecords = 1;
    int numForks = 1;
    ForkOperator mockForkOperator = new RoundRobinForkOperator(numForks);

    ArrayList<ArrayList<Object>> recordCollectors = new ArrayList<>(numForks);
    for (int i=0; i < numForks; ++i) {
      recordCollectors.add(new ArrayList<>());
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

    DataWriterBuilder writerBuilder = mockTaskContext.getDataWriterBuilder(numForks, 0);

    // writer id should have the expected name with the timestamp
    Assert.assertEquals(writerBuilder.getWriterId(), "testTimestampInFilename_12345_0");
  }

  /**
   * Test the addition of a task timestamp to the file name fails if the task start time is not present
   */
  @Test(expectedExceptions = {ExecutionException.class, NullPointerException.class})
  public void testTimestampInFilenameError()
      throws Exception {
    // Create a TaskState
    TaskState taskState = getEmptyTestTaskState("testTimestampInFilenameError");
    taskState.setProp(ConfigurationKeys.WRITER_ADD_TASK_TIMESTAMP, "true");

    int numRecords = 1;
    int numForks = 1;
    ForkOperator mockForkOperator = new RoundRobinForkOperator(numForks);

    ArrayList<ArrayList<Object>> recordCollectors = new ArrayList<>(numForks);
    for (int i=0; i < numForks; ++i) {
      recordCollectors.add(new ArrayList<>());
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
  }

  /**
   * A test that calls {@link Task#cancel()} while {@link Task#run()} is executing. Ensures that the countdown latch
   * is decremented and TaskState is set to FAILED.
   * @throws Exception
   */
  @Test
  public void testTaskCancelBeforeCompletion()
      throws Exception {
    // Create a TaskState
    TaskState taskState = getEmptyTestTaskState("testCancelBeforeCompletion");

    int numRecords = -1;
    int numForks = 1;
    ForkOperator mockForkOperator = new RoundRobinForkOperator(numForks);

    ArrayList<ArrayList<Object>> recordCollectors = new ArrayList<>(numForks);
    for (int i=0; i < numForks; ++i) {
      recordCollectors.add(new ArrayList<>());
    }

    TaskContext mockTaskContext = getMockTaskContext(taskState,
        new StringExtractor(numRecords), recordCollectors, mockForkOperator);

    // Create a dummy TaskStateTracker
    TaskStateTracker dummyTaskStateTracker = new GobblinMultiTaskAttemptTest.DummyTestStateTracker(new Properties(), log);

    // Create a TaskExecutor - a real TaskExecutor must be created so a Fork is run in a separate thread
    TaskExecutor taskExecutor = new TaskExecutor(new Properties());

    CountUpAndDownLatch countDownLatch = new CountUpAndDownLatch(0);
    // Create the Task
    Task task = new DelayedFailureTask(mockTaskContext, dummyTaskStateTracker, taskExecutor, Optional.of(countDownLatch));
    //Increment the countDownLatch to signal a new task creation.
    countDownLatch.countUp();

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future taskFuture = executorService.submit(new Thread(() -> task.run()));
    task.setTaskFuture(taskFuture);

    //Wait for task to enter RUNNING state
    AssertWithBackoff.create().maxSleepMs(10).timeoutMs(1000).backoffFactor(1)
        .assertTrue(input -> task.getWorkingState() == WorkUnitState.WorkingState.RUNNING,
            "Waiting for task to enter RUNNING state");

    Assert.assertEquals(countDownLatch.getCount(), 1);

    task.shutdown();

    //Ensure task is still RUNNING, since shutdown() is a NO-OP and the extractor should continue.
    Assert.assertEquals(countDownLatch.getCount(), 1);
    Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.RUNNING);

    //Call task cancel
    task.cancel();

    //Ensure task is still RUNNING immediately after cancel() due to the delay introduced in task failure handling.
    Assert.assertEquals(countDownLatch.getCount(), 1);
    Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.RUNNING);

    //Ensure countDownLatch is eventually counted down to 0
    AssertWithBackoff.create().maxSleepMs(100).timeoutMs(5000).backoffFactor(1)
        .assertTrue(input -> countDownLatch.getCount() == 0, "Waiting for the task to complete.");

    //Ensure the TaskState is set to FAILED
    Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.FAILED);
  }

  /**
   * A test that calls {@link Task#cancel()} after {@link Task#run()} is completed. In this case the cancel() method should
   * be a NO-OP and should leave the task state unchanged.
   * @throws Exception
   */
  @Test
  public void testTaskCancelAfterCompletion()
      throws Exception {
    // Create a TaskState
    TaskState taskState = getEmptyTestTaskState("testCancelAfterCompletion");

    int numRecords = -1;
    int numForks = 1;
    ForkOperator mockForkOperator = new RoundRobinForkOperator(numForks);

    ArrayList<ArrayList<Object>> recordCollectors = new ArrayList<>(numForks);
    for (int i=0; i < numForks; ++i) {
      recordCollectors.add(new ArrayList<>());
    }

    TaskContext mockTaskContext = getMockTaskContext(taskState,
        new StringExtractor(numRecords, false), recordCollectors, mockForkOperator);

    // Create a dummy TaskStateTracker
    TaskStateTracker dummyTaskStateTracker = new GobblinMultiTaskAttemptTest.DummyTestStateTracker(new Properties(), log);

    // Create a TaskExecutor - a real TaskExecutor must be created so a Fork is run in a separate thread
    TaskExecutor taskExecutor = new TaskExecutor(new Properties());

    CountUpAndDownLatch countDownLatch = new CountUpAndDownLatch(0);
    // Create the Task
    Task task = new Task(mockTaskContext, dummyTaskStateTracker, taskExecutor, Optional.of(countDownLatch));
    //Increment the countDownLatch to signal a new task creation.
    countDownLatch.countUp();

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future taskFuture = executorService.submit(new Thread(() -> task.run()));
    task.setTaskFuture(taskFuture);

    //Wait for task to enter RUNNING state
    AssertWithBackoff.create().maxSleepMs(10).timeoutMs(1000).backoffFactor(1)
        .assertTrue(input -> task.getWorkingState() == WorkUnitState.WorkingState.RUNNING,
            "Waiting for task to enter RUNNING state");

    Assert.assertEquals(countDownLatch.getCount(), 1);

    task.shutdown();

    //Ensure countDownLatch is counted down to 0 i.e. task is done.
    AssertWithBackoff.create().maxSleepMs(100).timeoutMs(5000).backoffFactor(1)
        .assertTrue(input -> countDownLatch.getCount() == 0, "Waiting for the task to complete.");

    //Ensure the TaskState is RUNNING
    Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.RUNNING);

    //Call task cancel
    task.cancel();

    //Ensure the TaskState is unchanged on cancel()
    Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.RUNNING);

    //Ensure task state is successful on commit()
    task.commit();
    Assert.assertEquals(taskState.getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);
  }

  /**
   * An implementation of {@link Extractor} that throws an {@link IOException} during the invocation of
   * {@link #readRecord(Object)}.
   */
  private static class FailOnceExtractor implements Extractor<Object, Object> {

    private final AtomicBoolean HAS_FAILED = new AtomicBoolean();

    @Override
    public Object getSchema() {
      return null;
    }

    @Override
    public Object readRecord(@Deprecated Object reuse) throws IOException {
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
    public void close() {
      // Do nothing
    }
  }

  private static class StringExtractor implements Extractor<Object, String> {
    //Num records to extract. If set to -1, it is treated as an unbounded extractor.
    private final int _numRecords;
    private int _currentRecord;
    private boolean _shouldIgnoreShutdown = true;
    private AtomicBoolean _shutdownRequested = new AtomicBoolean(false);

    public StringExtractor(int numRecords) {
      this(numRecords, true);
    }

    public StringExtractor(int numRecords, boolean shouldIgnoreShutdown) {
      _numRecords = numRecords;
      _currentRecord = -1;
      _shouldIgnoreShutdown = shouldIgnoreShutdown;
    }

    @Override
    public Object getSchema() {
      return "";
    }

    @Override
    public String readRecord(@Deprecated String reuse) {
      if (!_shutdownRequested.get() && (_numRecords == -1 || _currentRecord < _numRecords-1)) {
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
    public void close() {
    }

    @Override
    public void shutdown() {
      if (!this._shouldIgnoreShutdown) {
        this._shutdownRequested.set(true);
      }
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
    public void init(WorkUnitState workUnitState) {
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
    public void close() {

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
    public void init(WorkUnitState workUnitState) {
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
    public void close() {

    }
  }

  private class RecordCollectingWriterBuilder extends DataWriterBuilder {
    private final ArrayList<Object> _recordSink;

    public RecordCollectingWriterBuilder(ArrayList<Object> objects) {
      super();
      _recordSink = objects;
    }

    @Override
    public DataWriter build() {
      return new DataWriter() {
        @Override
        public void write(Object record) {
          _recordSink.add(record);
        }

        @Override
        public void commit() {

        }

        @Override
        public void cleanup() {

        }

        @Override
        public long recordsWritten() {
          return _recordSink.size();
        }

        @Override
        public long bytesWritten() {
          return -1;
        }

        @Override
        public void close() {

        }
      };
    }
  }

  /**
   * An extension of {@link Task} that introduces a fixed delay on encountering an exception.
   */
  private static class DelayedFailureTask extends Task {
    public DelayedFailureTask(TaskContext context, TaskStateTracker taskStateTracker, TaskExecutor taskExecutor,
        Optional<CountDownLatch> countDownLatch) {
      super(context, taskStateTracker, taskExecutor, countDownLatch);
    }

    @Override
    protected void failTask(Throwable t) {
      try {
        Thread.sleep(1000);
        super.failTask(t);
      } catch (InterruptedException e) {
        log.error("Encountered exception: {}", e);
      }
    }
  }
}
