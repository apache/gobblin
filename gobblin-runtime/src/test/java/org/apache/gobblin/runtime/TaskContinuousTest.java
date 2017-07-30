/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.runtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.fork.IdentityForkOperator;
import gobblin.publisher.TaskPublisher;
import gobblin.qualitychecker.row.RowLevelPolicyChecker;
import gobblin.qualitychecker.task.TaskLevelPolicyCheckResults;
import gobblin.qualitychecker.task.TaskLevelPolicyChecker;
import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.DefaultCheckpointableWatermark;
import gobblin.source.extractor.Extractor;
import gobblin.stream.RecordEnvelope;
import gobblin.source.extractor.StreamingExtractor;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ExecutorsUtils;
import gobblin.writer.DataWriter;
import gobblin.writer.WatermarkAwareWriter;
import gobblin.writer.WatermarkStorage;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@Slf4j
@Test
public class TaskContinuousTest {

  private static class OneRecordExtractor implements StreamingExtractor<Object, String> {

    private long index = 0L;
    private final String record;
    private volatile boolean closed = false;

    public OneRecordExtractor(String record) {
      this.record = record;
    }

    @Override
    public Object getSchema()
    throws IOException {
      return null;
    }

    @Override
    public RecordEnvelope<String> readRecordEnvelope()
    throws DataRecordException, IOException {
      if (!this.closed) {
        if (index == 0) {
          RecordEnvelope<String> recordEnvelope =
              new RecordEnvelope<>(record, new DefaultCheckpointableWatermark("default", new LongWatermark(index)));
          log.debug("Returning record with index {}", index);
          index++;
          return recordEnvelope;
        }
        else {
          return null;
        }
      } else {
        log.info("Extractor has been closed, returning null");
        return null;
      }
    }

    @Override
    public long getExpectedRecordCount() {
      return this.index;
    }

    @Override
    public long getHighWatermark() {
      return -1;
    }

    @Override
    public void close()
    throws IOException {
      this.closed = true;
    }

    public boolean validateWatermarks(boolean exact, Map<String, CheckpointableWatermark> watermarkMap) {
    if (!watermarkMap.isEmpty()) {
      // watermark must be <= the index
      LongWatermark longWatermark = (LongWatermark) watermarkMap.values().iterator().next().getWatermark();
      if (exact) {
        System.out.println(index-1 + ":" + longWatermark.getValue());
        return ((index-1) == longWatermark.getValue());
      } else {
        return (index > longWatermark.getValue());
      }
    }
    return true;
  }

    @Override
    public void start(WatermarkStorage watermarkStorage) {

    }
  }



  private static class ContinuousExtractor implements StreamingExtractor<Object, String> {

    private long index = 0L;
    private final long sleepTimeInMillis;
    private volatile boolean closed = false;

    public ContinuousExtractor(long sleepTimeInMillis) {
      this.sleepTimeInMillis = sleepTimeInMillis;
    }

    @Override
    public Object getSchema()
        throws IOException {
      return null;
    }

    @Override
    public RecordEnvelope<String> readRecordEnvelope()
        throws DataRecordException, IOException {
      if (!this.closed) {
        try {
          Thread.sleep(this.sleepTimeInMillis);
        } catch (InterruptedException e) {
          Throwables.propagate(e);
        }
        String record = index + "";
        RecordEnvelope<String> recordEnvelope =
            new RecordEnvelope<>(record, new DefaultCheckpointableWatermark("default", new LongWatermark(index)));
        index++;
        return recordEnvelope;
      } else {
        log.info("Extractor has been closed, returning null");
        return null;
      }
    }

    @Override
    public long getExpectedRecordCount() {
      return this.index;
    }

    @Override
    public long getHighWatermark() {
      return -1;
    }

    @Override
    public void close()
        throws IOException {
      this.closed = true;
    }


    public boolean validateWatermarks(boolean exact, Map<String, CheckpointableWatermark> watermarkMap) {
      if (!watermarkMap.isEmpty()) {
        // watermark must be <= the index
        LongWatermark longWatermark = (LongWatermark) watermarkMap.values().iterator().next().getWatermark();
        if (exact) {
          System.out.println(index-1 + ":" + longWatermark.getValue());
          return ((index-1) == longWatermark.getValue());
        } else {
          return (index > longWatermark.getValue());
        }
      }
      return true;
    }

    @Override
    public void start(WatermarkStorage watermarkStorage) {

    }
  }


  class MockWatermarkStorage implements WatermarkStorage {
    private ConcurrentHashMap<String, CheckpointableWatermark> watermarks = new ConcurrentHashMap<>();

    @Override
    public void commitWatermarks(Iterable<CheckpointableWatermark> watermarks)
        throws IOException {
      for (CheckpointableWatermark watermark: watermarks) {
        this.watermarks.put(watermark.getSource(), watermark);
      }

    }

    @Override
    public Map<String, CheckpointableWatermark> getCommittedWatermarks(
        Class<? extends CheckpointableWatermark> watermarkClass, Iterable<String> sourcePartitions)
        throws IOException {
      // make a copy
      return new HashMap<>(this.watermarks);
    }
  }

  /**
   * Test that a streaming task will work correctly when the extractor only produces one record
   * @throws Exception
   */
  @Test
  public void testContinuousTaskOneRecord()
      throws Exception {

    ArrayList<Object> recordCollector = new ArrayList<>(100);

    String testRecord = "hello";

    OneRecordExtractor oneRecordExtractor =
        new OneRecordExtractor(testRecord);


    TaskContext mockTaskContext =
        getMockTaskContext(recordCollector, oneRecordExtractor);

    // Create a mock TaskPublisher
    TaskPublisher mockTaskPublisher = mock(TaskPublisher.class);
    when(mockTaskPublisher.canPublish()).thenReturn(TaskPublisher.PublisherState.SUCCESS);
    when(mockTaskContext.getTaskPublisher(any(TaskState.class), any(TaskLevelPolicyCheckResults.class))).thenReturn(mockTaskPublisher);

    // Create a mock TaskStateTracker
    TaskStateTracker mockTaskStateTracker = mock(TaskStateTracker.class);

    // Create a TaskExecutor - a real TaskExecutor must be created so a Fork is run in a separate thread
    TaskExecutor taskExecutor = new TaskExecutor(new Properties());

    // Create the Task
    Task task = new Task(mockTaskContext, mockTaskStateTracker, taskExecutor, Optional.<CountDownLatch>absent());

    ScheduledExecutorService taskRunner = new ScheduledThreadPoolExecutor(1, ExecutorsUtils.newThreadFactory(Optional.of(log)));

    taskRunner.execute(task);


    // Let the task run for 2 seconds
    int sleepIterations = 2;
    int currentIteration = 0;
    WatermarkStorage mockWatermarkStorage = mockTaskContext.getWatermarkStorage();
    Map<String, CheckpointableWatermark> externalWatermarkStorage;
    while (currentIteration < sleepIterations) {
      Thread.sleep(1000);
      currentIteration++;
      externalWatermarkStorage =
          mockWatermarkStorage.getCommittedWatermarks(CheckpointableWatermark.class, ImmutableList
          .of("default"));
      if (!externalWatermarkStorage.isEmpty()) {
        for (CheckpointableWatermark watermark : externalWatermarkStorage.values()) {
          log.info("Observed committed watermark: {}", watermark);
        }
        log.info("Task progress: {}", task.getProgress());
        // Ensure that watermarks seem reasonable at each step
        Assert.assertTrue(oneRecordExtractor.validateWatermarks(false, externalWatermarkStorage));
      }
    }

    // Let's try to shutdown the task
    task.shutdown();
    log.info("Shutting down task now");
    boolean success = task.awaitShutdown(3000);
    Assert.assertTrue(success, "Task should shutdown in 3 seconds");
    log.info("Task done waiting to shutdown {}", success);

    externalWatermarkStorage =
        mockWatermarkStorage.getCommittedWatermarks(CheckpointableWatermark.class, ImmutableList
            .of("0"));

    // Ensure that committed watermarks match exactly the input rows because we shutdown in an orderly manner.
    Assert.assertTrue(oneRecordExtractor.validateWatermarks(true, externalWatermarkStorage));

    // Ensure that the record made it to the writer correctly
    Assert.assertEquals(recordCollector.size(), 1);
    Assert.assertEquals(recordCollector.get(0), testRecord);


    task.commit();

    // Shutdown the executor
    taskRunner.shutdown();
    taskRunner.awaitTermination(100, TimeUnit.MILLISECONDS);


  }

  private TaskContext getMockTaskContext(ArrayList<Object> recordCollector,
      Extractor mockExtractor)
      throws Exception {

    TaskState taskState = getStreamingTaskState();
    // Create a mock RowLevelPolicyChecker
    RowLevelPolicyChecker mockRowLevelPolicyChecker =
        new RowLevelPolicyChecker(Lists.newArrayList(), "stateId", FileSystem.getLocal(new Configuration()));

    WatermarkStorage mockWatermarkStorage = new MockWatermarkStorage();

    // Create a mock TaskPublisher
    TaskPublisher mockTaskPublisher = mock(TaskPublisher.class);
    when(mockTaskPublisher.canPublish()).thenReturn(TaskPublisher.PublisherState.SUCCESS);


    // Create a mock TaskContext
    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getExtractor()).thenReturn(mockExtractor);
    when(mockTaskContext.getRawSourceExtractor()).thenReturn(mockExtractor);
    when(mockTaskContext.getWatermarkStorage()).thenReturn(mockWatermarkStorage);
    when(mockTaskContext.getForkOperator()).thenReturn(new IdentityForkOperator());
    when(mockTaskContext.getTaskState()).thenReturn(taskState);
    when(mockTaskContext.getTaskPublisher(any(TaskState.class), any(TaskLevelPolicyCheckResults.class)))
        .thenReturn(mockTaskPublisher);
    when(mockTaskContext.getRowLevelPolicyChecker()).thenReturn(mockRowLevelPolicyChecker);
    when(mockTaskContext.getRowLevelPolicyChecker(anyInt())).thenReturn(mockRowLevelPolicyChecker);
    when(mockTaskContext.getTaskLevelPolicyChecker(any(TaskState.class), anyInt())).thenReturn(mock(TaskLevelPolicyChecker.class));
    when(mockTaskContext.getDataWriterBuilder(anyInt(), anyInt())).thenReturn(new TestStreamingDataWriterBuilder(recordCollector));
    return mockTaskContext;
  }

  private TaskState getStreamingTaskState() {
    WorkUnitState workUnitState = new WorkUnitState(WorkUnit.create(
        new Extract(Extract.TableType.SNAPSHOT_ONLY, this.getClass().getName(), this.getClass().getSimpleName())));
    workUnitState.setProp(ConfigurationKeys.TASK_KEY_KEY, "1234");

    TaskState taskState = new TaskState(workUnitState);
    taskState.setProp(ConfigurationKeys.METRICS_ENABLED_KEY, Boolean.toString(false));
    taskState.setProp(TaskConfigurationKeys.TASK_EXECUTION_MODE, ExecutionModel.STREAMING.name());
    taskState.setJobId("1234");
    taskState.setTaskId("testContinuousTaskId");
    return taskState;
  }

  /**
   * Test that a streaming task will work correctly when an extractor is continuously producing records
   * No converters
   * Identity fork
   * One writer
   * @throws Exception
   */
  @Test
  public void testContinuousTask()
      throws Exception {
    // Create a TaskState
    TaskState taskState = getStreamingTaskState();

    ArrayList<Object> recordCollector = new ArrayList<>(100);
    long perRecordExtractLatencyMillis = 1000; // 1 second per record



    ContinuousExtractor continuousExtractor =
        new ContinuousExtractor(perRecordExtractLatencyMillis);

    TaskContext mockTaskContext = getMockTaskContext(recordCollector, continuousExtractor);


    // Create a mock TaskStateTracker
    TaskStateTracker mockTaskStateTracker = mock(TaskStateTracker.class);

    // Create a TaskExecutor - a real TaskExecutor must be created so a Fork is run in a separate thread
    TaskExecutor taskExecutor = new TaskExecutor(new Properties());

    // Create the Task
    Task task = new Task(mockTaskContext, mockTaskStateTracker, taskExecutor, Optional.<CountDownLatch>absent());

    ScheduledExecutorService taskRunner = new ScheduledThreadPoolExecutor(1, ExecutorsUtils.newThreadFactory(Optional.of(log)));

    taskRunner.execute(task);


    // Let the task run for 10 seconds
    int sleepIterations = 10;
    int currentIteration = 0;

    while (currentIteration < sleepIterations) {
      Thread.sleep(1000);
      currentIteration++;
      Map<String, CheckpointableWatermark> externalWatermarkStorage = mockTaskContext.getWatermarkStorage()
          .getCommittedWatermarks(CheckpointableWatermark.class, ImmutableList.of("default"));
      if (!externalWatermarkStorage.isEmpty()) {
        for (CheckpointableWatermark watermark : externalWatermarkStorage.values()) {
          log.info("Observed committed watermark: {}", watermark);
        }
        log.info("Task progress: {}", task.getProgress());
        // Ensure that watermarks seem reasonable at each step
        Assert.assertTrue(continuousExtractor.validateWatermarks(false, externalWatermarkStorage));
      }
    }

    // Let's try to shutdown the task
    task.shutdown();
    log.info("Shutting down task now");
    boolean success = task.awaitShutdown(30000);
    Assert.assertTrue(success, "Task should shutdown in 3 seconds");
    log.info("Task done waiting to shutdown {}", success);

    // Ensure that committed watermarks match exactly the input rows because we shutdown in an orderly manner.
    Assert.assertTrue(continuousExtractor.validateWatermarks(true, mockTaskContext.getWatermarkStorage()
        .getCommittedWatermarks(CheckpointableWatermark.class, ImmutableList.of("default"))));

    task.commit();

    // Shutdown the executor
    taskRunner.shutdown();
    taskRunner.awaitTermination(100, TimeUnit.MILLISECONDS);


  }

  private class TestStreamingDataWriterBuilder extends gobblin.writer.DataWriterBuilder {

    private final List<Object> _recordCollector;

    TestStreamingDataWriterBuilder(List<Object> recordCollector) {
      _recordCollector = recordCollector;
    }

    @Override
    public DataWriter build()
        throws IOException {
      return new WatermarkAwareWriter<Object>() {

        private AtomicReference<CheckpointableWatermark> lastWatermark = new AtomicReference<>(null);
        private AtomicReference<String> source = new AtomicReference<>(null);

        @Override
        public boolean isWatermarkCapable() {
          return true;
        }

        @Override
        public void writeEnvelope(RecordEnvelope<Object> recordEnvelope)
            throws IOException {
          _recordCollector.add(recordEnvelope.getRecord());
          String source = recordEnvelope.getWatermark().getSource();
          if (this.source.get() != null) {
            if (!source.equals(this.source.get())) {
              throw new RuntimeException("This writer only supports a single source");
            }
          }
          this.lastWatermark.set(recordEnvelope.getWatermark());
          recordEnvelope.ack();
          this.source.set(source);
        }

        @Override
        public Map<String, CheckpointableWatermark> getCommittableWatermark() {
          CheckpointableWatermark committable = lastWatermark.get();
          if (committable != null) {
            Map<String, CheckpointableWatermark> singletonMap = new HashMap<>();
            singletonMap.put(source.get(), committable);
            return singletonMap;
          } else {
            return Collections.EMPTY_MAP;
          }
        }

        @Override
        public Map<String, CheckpointableWatermark> getUnacknowledgedWatermark() {
          return Collections.EMPTY_MAP;
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
          return 0;
        }

        @Override
        public long bytesWritten()
            throws IOException {
          return 0;
        }

        @Override
        public void close()
            throws IOException {

        }
      };
    }
  }
}


