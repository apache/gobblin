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
import java.util.Collections;
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

import org.apache.gobblin.ack.BasicAckableForTesting;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.Converter;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.fork.IdentityForkOperator;
import org.apache.gobblin.metadata.GlobalMetadata;
import org.apache.gobblin.publisher.TaskPublisher;
import org.apache.gobblin.qualitychecker.row.RowLevelPolicyChecker;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicyCheckResults;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicyChecker;
import org.apache.gobblin.records.ControlMessageHandler;
import org.apache.gobblin.records.FlushControlMessageHandler;
import org.apache.gobblin.records.RecordStreamProcessor;
import org.apache.gobblin.records.RecordStreamWithMetadata;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.stream.ControlMessage;
import org.apache.gobblin.stream.ControlMessageInjector;
import org.apache.gobblin.stream.FlushControlMessage;
import org.apache.gobblin.stream.MetadataUpdateControlMessage;
import org.apache.gobblin.stream.RecordEnvelope;
import org.apache.gobblin.stream.StreamEntity;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import io.reactivex.Flowable;
import lombok.AllArgsConstructor;


/**
 * Tests for streaming model of Gobblin.
 */
public class TestRecordStream {

  @Test
  public void testControlMessages() throws Exception {

    MyExtractor extractor = new MyExtractor(new StreamEntity[]{new RecordEnvelope<>("a"),
        new BasicTestControlMessage("1"), new RecordEnvelope<>("b"), new BasicTestControlMessage("2")});
    MyConverter converter = new MyConverter();
    MyDataWriter writer = new MyDataWriter();

    Task task = setupTask(extractor, writer, converter);

    task.run();
    task.commit();
    Assert.assertEquals(task.getTaskState().getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);

    Assert.assertEquals(converter.records, Lists.newArrayList("a", "b"));
    Assert.assertEquals(converter.messages, Lists.newArrayList(new BasicTestControlMessage("1"), new BasicTestControlMessage("2")));

    Assert.assertEquals(writer.records, Lists.newArrayList("a", "b"));
    Assert.assertEquals(writer.messages, Lists.newArrayList(new BasicTestControlMessage("1"), new BasicTestControlMessage("2")));
  }

  @Test
  public void testFlushControlMessages() throws Exception {
    MyExtractor extractor = new MyExtractor(new StreamEntity[]{new RecordEnvelope<>("a"),
        FlushControlMessage.builder().flushReason("flush1").build(), new RecordEnvelope<>("b"),
        FlushControlMessage.builder().flushReason("flush2").build()});
    MyConverter converter = new MyConverter();
    MyFlushDataWriter writer = new MyFlushDataWriter();

    Task task = setupTask(extractor, writer, converter);

    task.run();
    task.commit();
    Assert.assertEquals(task.getTaskState().getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);

    Assert.assertEquals(converter.records, Lists.newArrayList("a", "b"));
    Assert.assertEquals(converter.messages, Lists.newArrayList(
        FlushControlMessage.builder().flushReason("flush1").build(),
        FlushControlMessage.builder().flushReason("flush2").build()));

    Assert.assertEquals(writer.records, Lists.newArrayList("a", "b"));
    Assert.assertEquals(writer.flush_messages, Lists.newArrayList("flush called", "flush called"));
  }

  /**
   * Test of metadata update control messages that signal the converters to change schemas
   * @throws Exception
   */
  @Test
  public void testMetadataUpdateControlMessages() throws Exception {

    MyExtractor extractor = new MyExtractor(new StreamEntity[]{new RecordEnvelope<>("a"),
        new MetadataUpdateControlMessage<>(GlobalMetadata.<String>builder().schema("Schema1").build()), new RecordEnvelope<>("b"),
            new MetadataUpdateControlMessage(GlobalMetadata.<String>builder().schema("Schema2").build())});
    SchemaAppendConverter converter = new SchemaAppendConverter();
    MyDataWriter writer = new MyDataWriter();

    Task task = setupTask(extractor, writer, converter);

    task.run();
    task.commit();
    Assert.assertEquals(task.getTaskState().getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);

    Assert.assertEquals(converter.records, Lists.newArrayList("a:schema", "b:Schema1"));
    Assert.assertEquals(converter.messages,
        Lists.newArrayList(new MetadataUpdateControlMessage<>(GlobalMetadata.<String>builder().schema("Schema1").build()),
            new MetadataUpdateControlMessage<>(GlobalMetadata.<String>builder().schema("Schema2").build())));

    Assert.assertEquals(writer.records, Lists.newArrayList("a:schema", "b:Schema1"));
    Assert.assertEquals(writer.messages, Lists.newArrayList(new MetadataUpdateControlMessage<>(
        GlobalMetadata.<String>builder().schema("Schema1").build()),
        new MetadataUpdateControlMessage<>(GlobalMetadata.<String>builder().schema("Schema2").build())));
  }

  /**
   * Test with the converter configured in the list of {@link RecordStreamProcessor}s.
   * @throws Exception
   */
  @Test
  public void testMetadataUpdateWithStreamProcessors() throws Exception {

    MyExtractor extractor = new MyExtractor(new StreamEntity[]{new RecordEnvelope<>("a"),
        new MetadataUpdateControlMessage<>(GlobalMetadata.<String>builder().schema("Schema1").build()), new RecordEnvelope<>("b"),
        new MetadataUpdateControlMessage(GlobalMetadata.<String>builder().schema("Schema2").build())});
    SchemaAppendConverter converter = new SchemaAppendConverter();
    MyDataWriter writer = new MyDataWriter();

    Task task = setupTask(extractor, writer, Collections.EMPTY_LIST, Lists.newArrayList(converter));

    task.run();
    task.commit();
    Assert.assertEquals(task.getTaskState().getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);

    Assert.assertEquals(converter.records, Lists.newArrayList("a:schema", "b:Schema1"));
    Assert.assertEquals(converter.messages,
        Lists.newArrayList(new MetadataUpdateControlMessage<>(GlobalMetadata.<String>builder().schema("Schema1").build()),
            new MetadataUpdateControlMessage<>(GlobalMetadata.<String>builder().schema("Schema2").build())));

    Assert.assertEquals(writer.records, Lists.newArrayList("a:schema", "b:Schema1"));
    Assert.assertEquals(writer.messages, Lists.newArrayList(new MetadataUpdateControlMessage<>(
        GlobalMetadata.<String>builder().schema("Schema1").build()),
        new MetadataUpdateControlMessage<>(GlobalMetadata.<String>builder().schema("Schema2").build())));
  }

  /**
   * Test the injection of {@link ControlMessage}s
   * @throws Exception
   */
  @Test
  public void testInjectedControlMessages() throws Exception {

    MyExtractor extractor = new MyExtractor(new StreamEntity[]{new RecordEnvelope<>("schema:a"),
        new RecordEnvelope<>("schema:b"), new RecordEnvelope<>("schema1:c"), new RecordEnvelope<>("schema2:d")});
    SchemaChangeDetectionInjector injector = new SchemaChangeDetectionInjector();
    SchemaAppendConverter converter = new SchemaAppendConverter();
    MyDataWriter writer = new MyDataWriterWithSchemaCheck();

    Task task = setupTask(extractor, writer, Collections.EMPTY_LIST,
        Lists.newArrayList(injector, converter));

    task.run();
    task.commit();
    Assert.assertEquals(task.getTaskState().getWorkingState(), WorkUnitState.WorkingState.SUCCESSFUL);

    Assert.assertEquals(converter.records, Lists.newArrayList("a:schema", "b:schema", "c:schema1", "d:schema2"));
    Assert.assertEquals(converter.messages,
        Lists.newArrayList(new MetadataUpdateControlMessage<>(GlobalMetadata.<String>builder().schema("schema1").build()),
            new MetadataUpdateControlMessage<>(GlobalMetadata.<String>builder().schema("schema2").build())));

    Assert.assertEquals(writer.records, Lists.newArrayList("a:schema", "b:schema", "c:schema1", "d:schema2"));
    Assert.assertEquals(writer.messages, Lists.newArrayList(new MetadataUpdateControlMessage<>(
        GlobalMetadata.<String>builder().schema("schema1").build()),
        new MetadataUpdateControlMessage<>(GlobalMetadata.<String>builder().schema("schema2").build())));
  }

  @Test
  public void testAcks() throws Exception {

    StreamEntity[] entities = new StreamEntity[]{new RecordEnvelope<>("a"),
        new BasicTestControlMessage("1"), new RecordEnvelope<>("b"), new BasicTestControlMessage("2")};

    BasicAckableForTesting ackable = new BasicAckableForTesting();
    for (int i = 0; i < entities.length; i++) {
      entities[i].addCallBack(ackable);
    }

    MyExtractor extractor = new MyExtractor(entities);
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

    Assert.assertEquals(ackable.acked, 4);
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

  private Task setupTask(Extractor extractor, DataWriterBuilder writer, Converter converter) throws Exception {
    return setupTask(extractor, writer, Lists.newArrayList(converter), Collections.EMPTY_LIST);
  }

  private Task setupTask(Extractor extractor, DataWriterBuilder writer, List<Converter<?,?,?,?>> converters,
      List<RecordStreamProcessor<?,?,?,?>> recordStreamProcessors) throws Exception {
    // Create a TaskState
    TaskState taskState = getEmptyTestTaskState("testRetryTaskId");
    taskState.setProp(ConfigurationKeys.TASK_SYNCHRONOUS_EXECUTION_MODEL_KEY, false);
    // Create a mock TaskContext
    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getExtractor()).thenReturn(extractor);
    when(mockTaskContext.getForkOperator()).thenReturn(new IdentityForkOperator());
    when(mockTaskContext.getTaskState()).thenReturn(taskState);
    when(mockTaskContext.getConverters()).thenReturn(converters);
    when(mockTaskContext.getRecordStreamProcessors()).thenReturn(recordStreamProcessors);
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

    return task;
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
      return new RecordStreamWithMetadata<>(Flowable.fromArray(this.stream),
          GlobalMetadata.<String>builder().schema("schema").build());
    }
  }

  static class MyDataWriter extends DataWriterBuilder<String, String> implements DataWriter<String> {
    protected List<String> records = new ArrayList<>();
    protected List<ControlMessage<String>> messages = new ArrayList<>();
    protected String writerSchema;

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
      this.writerSchema = this.schema;
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


  /**
   * Converter that appends the output schema string to the record string
   */
  static class SchemaAppendConverter extends Converter<String, String, String, String> {
    private List<String> records = new ArrayList<>();
    private List<ControlMessage<String>> messages = new ArrayList<>();

    @Override
    public String convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
      return inputSchema;
    }

    @Override
    public Iterable<String> convertRecord(String outputSchema, String inputRecord, WorkUnitState workUnit)
        throws DataConversionException {
      String inputWithoutSchema = inputRecord.substring(inputRecord.indexOf(":") + 1);
      String outputRecord = inputWithoutSchema + ":" + outputSchema;
      records.add(outputRecord);
      return Lists.newArrayList(outputRecord);
    }

    @Override
    public ControlMessageHandler getMessageHandler() {
      return messages::add;
    }
  }

  /**
   * Input to this {@link RecordStreamProcessor} is a string of the form "schema:value".
   * It will inject a {@link MetadataUpdateControlMessage} when a schema change is detected.
   */
  static class SchemaChangeDetectionInjector extends ControlMessageInjector<String, String> {
    private List<String> records = new ArrayList<>();
    private List<ControlMessage<String>> messages = new ArrayList<>();
    private GlobalMetadata<String> globalMetadata;

    public Iterable<String> convertRecord(String outputSchema, String inputRecord, WorkUnitState workUnitState)
        throws DataConversionException {

      String outputRecord = inputRecord.split(":")[1];
      records.add(outputRecord);
      return Lists.newArrayList(outputRecord);
    }

    @Override
    protected void setInputGlobalMetadata(GlobalMetadata<String> inputGlobalMetadata, WorkUnitState workUnitState) {
      this.globalMetadata = inputGlobalMetadata;
    }

    @Override
    public Iterable<ControlMessage<String>> injectControlMessagesBefore(RecordEnvelope<String> inputRecordEnvelope,
        WorkUnitState workUnitState) {
      String recordSchema = inputRecordEnvelope.getRecord().split(":")[0];

      if (!recordSchema.equals(this.globalMetadata.getSchema())) {
        return Lists.newArrayList(new MetadataUpdateControlMessage<>(
            GlobalMetadata.<String>builder().schema(recordSchema).build()));
      }

      return null;
    }

    @Override
    public Iterable<ControlMessage<String>> injectControlMessagesAfter(RecordEnvelope<String> inputRecordEnvelope,
        WorkUnitState workUnitState) {
      return null;
    }

    @Override
    public ControlMessageHandler getMessageHandler() {
      return messages::add;
    }
  }

  static class MyFlushDataWriter extends MyDataWriter {
    private List<String> flush_messages = new ArrayList<>();

    @Override
    public ControlMessageHandler getMessageHandler() {
      return new FlushControlMessageHandler(this);
    }

    @Override
    public void flush() throws IOException {
      flush_messages.add("flush called");
    }
  }

  static class MyDataWriterWithSchemaCheck extends MyDataWriter {
    @Override
    public void write(String record) throws IOException {
      super.write(record);

      Assert.assertEquals(this.writerSchema, record.split(":")[1]);
    }
  }
}
