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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.fork.IdentityForkOperator;
import gobblin.publisher.TaskPublisher;
import gobblin.qualitychecker.row.RowLevelPolicyChecker;
import gobblin.qualitychecker.task.TaskLevelPolicyCheckResults;
import gobblin.qualitychecker.task.TaskLevelPolicyChecker;
import gobblin.records.ControlMessageHandler;
import gobblin.records.RecordStreamWithMetadata;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.stream.ControlMessage;
import gobblin.stream.RecordEnvelope;
import gobblin.stream.StreamEntity;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;

import io.reactivex.Flowable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;


/**
 * Tests for streaming model of Gobblin.
 */
public class TestRecordStream {

  @Test
  public void testControlMessages() throws Exception {

    MyExtractor extractor = new MyExtractor(new StreamEntity[]{new RecordEnvelope<>("a"),
        new MyControlMessage("1"), new RecordEnvelope<>("b"), new MyControlMessage("2")});
    MyConverter converter = new MyConverter();
    MyDataWriter writer = new MyDataWriter();

    // Create a TaskState
    TaskState taskState = getEmptyTestTaskState("testRetryTaskId");
    taskState.setProp(ConfigurationKeys.TASK_SYNCHRONOUS_EXECUTION_MODEL_KEY, false);
    // Create a mock TaskContext
    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getExtractor()).thenReturn(extractor);
    when(mockTaskContext.getForkOperator()).thenReturn(new IdentityForkOperator());
    when(mockTaskContext.getTaskState()).thenReturn(taskState);
    when(mockTaskContext.getConverters()).thenReturn(Lists.newArrayList(converter));
    when(mockTaskContext.getTaskLevelPolicyChecker(any(TaskState.class), anyInt()))
        .thenReturn(mock(TaskLevelPolicyChecker.class));
    when(mockTaskContext.getRowLevelPolicyChecker()).
        thenReturn(new RowLevelPolicyChecker(Lists.newArrayList(), "ss", FileSystem.getLocal(new Configuration())));
    when(mockTaskContext.getRowLevelPolicyChecker(anyInt())).
        thenReturn(new RowLevelPolicyChecker(Lists.newArrayList(), "ss", FileSystem.getLocal(new Configuration())));
    when(mockTaskContext.getDataWriterBuilder(anyInt(), anyInt())).thenReturn(writer);

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

    task.run();
    task.commit();
    Assert.assertEquals(task.getTaskState().getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);

    Assert.assertEquals(converter.records, Lists.newArrayList("a", "b"));
    Assert.assertEquals(converter.messages, Lists.newArrayList(new MyControlMessage("1"), new MyControlMessage("2")));

    Assert.assertEquals(writer.records, Lists.newArrayList("a", "b"));
    Assert.assertEquals(writer.messages, Lists.newArrayList(new MyControlMessage("1"), new MyControlMessage("2")));
  }

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

  @AllArgsConstructor
  static class MyExtractor implements Extractor<String, String> {
    private final StreamEntity<String>[] stream;

    @Override
    public String getSchema() throws IOException {
      return "schema";
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
    public void close() throws IOException {

    }

    @Override
    public RecordStreamWithMetadata<String, String> recordStream(AtomicBoolean shutdownRequest) throws IOException {
      return new RecordStreamWithMetadata<>(Flowable.fromArray(this.stream), "schema");
    }
  }

  @AllArgsConstructor
  @EqualsAndHashCode
  static class MyControlMessage extends ControlMessage<String> {
    private final String id;

    @Override
    public StreamEntity<String> getClone() {
      return new MyControlMessage(this.id);
    }
  }

  static class MyDataWriter extends DataWriterBuilder<String, String> implements DataWriter<String> {
    private List<String> records = new ArrayList<>();
    private List<ControlMessage<String>> messages = new ArrayList<>();

    @Override
    public void write(String record) throws IOException {
      this.records.add(record);
    }

    @Override
    public ControlMessageHandler getMessageHandler() {
      return messages::add;
    }

    @Override
    public void commit() throws IOException {}

    @Override
    public void cleanup() throws IOException {}

    @Override
    public long recordsWritten() {
      return 0;
    }

    @Override
    public long bytesWritten() throws IOException {
      return 0;
    }

    @Override
    public DataWriter<String> build() throws IOException {
      return this;
    }

    @Override
    public void close() throws IOException {}

  }

  static class MyConverter extends Converter<String, String, String, String> {
    private List<String> records = new ArrayList<>();
    private List<ControlMessage<String>> messages = new ArrayList<>();

    @Override
    public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
      return "schema";
    }

    @Override
    public Iterable<String> convertRecord(String outputSchema, String inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      records.add(inputRecord);
      return Lists.newArrayList(inputRecord);
    }

    @Override
    public ControlMessageHandler getMessageHandler() {
      return messages::add;
    }
  }

}
