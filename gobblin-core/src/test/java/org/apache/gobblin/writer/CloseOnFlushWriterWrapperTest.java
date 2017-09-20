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
package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.records.ControlMessageHandler;
import org.apache.gobblin.records.FlushControlMessageHandler;
import org.apache.gobblin.stream.ControlMessage;
import org.apache.gobblin.stream.FlushControlMessage;
import org.apache.gobblin.stream.RecordEnvelope;

public class CloseOnFlushWriterWrapperTest {

  @Test
  public void testCloseOnFlushDisabled()
      throws IOException {
    WorkUnitState state = new WorkUnitState();
    List<DummyWriter> dummyWriters = new ArrayList<>();
    CloseOnFlushWriterWrapper<byte[]> writer = getCloseOnFlushWriter(dummyWriters, state);

    byte[] record = new byte[]{'a', 'b', 'c', 'd'};

    writer.writeEnvelope(new RecordEnvelope(record));

    writer.getMessageHandler().handleMessage(FlushControlMessage.builder().build());

    Assert.assertEquals(dummyWriters.get(0).recordsWritten(), 1);
    Assert.assertEquals(dummyWriters.get(0).flushCount, 1);
    Assert.assertEquals(dummyWriters.get(0).closeCount, 0);
    Assert.assertFalse(dummyWriters.get(0).committed);
    Assert.assertEquals(dummyWriters.get(0).handlerCalled, 1);
  }

  @Test
  public void testCloseOnFlushEnabled()
      throws IOException {
    WorkUnitState state = new WorkUnitState();
    state.getJobState().setProp(CloseOnFlushWriterWrapper.WRITER_CLOSE_ON_FLUSH_KEY, "true");
    List<DummyWriter> dummyWriters = new ArrayList<>();
    CloseOnFlushWriterWrapper<byte[]> writer = getCloseOnFlushWriter(dummyWriters, state);

    byte[] record = new byte[]{'a', 'b', 'c', 'd'};

    writer.writeEnvelope(new RecordEnvelope(record));
    writer.getMessageHandler().handleMessage(FlushControlMessage.builder().build());

    Assert.assertEquals(dummyWriters.get(0).recordsWritten(), 1);
    Assert.assertEquals(dummyWriters.get(0).flushCount, 1);
    Assert.assertEquals(dummyWriters.get(0).closeCount, 1);
    Assert.assertTrue(dummyWriters.get(0).committed);
    Assert.assertEquals(dummyWriters.get(0).handlerCalled, 1);
  }

  @Test
  public void testWriteAfterFlush()
      throws IOException {
    WorkUnitState state = new WorkUnitState();
    state.getJobState().setProp(CloseOnFlushWriterWrapper.WRITER_CLOSE_ON_FLUSH_KEY, "true");
    List<DummyWriter> dummyWriters = new ArrayList<>();
    CloseOnFlushWriterWrapper<byte[]> writer = getCloseOnFlushWriter(dummyWriters, state);

    byte[] record = new byte[]{'a', 'b', 'c', 'd'};

    writer.writeEnvelope(new RecordEnvelope(record));
    writer.getMessageHandler().handleMessage(FlushControlMessage.builder().build());

    Assert.assertEquals(dummyWriters.size(), 1);
    Assert.assertEquals(dummyWriters.get(0).recordsWritten(), 1);
    Assert.assertEquals(dummyWriters.get(0).flushCount, 1);
    Assert.assertEquals(dummyWriters.get(0).closeCount, 1);
    Assert.assertTrue(dummyWriters.get(0).committed);
    Assert.assertEquals(dummyWriters.get(0).handlerCalled, 1);

    writer.writeEnvelope(new RecordEnvelope(record));
    writer.getMessageHandler().handleMessage(FlushControlMessage.builder().build());

    Assert.assertEquals(dummyWriters.size(), 2);
    Assert.assertEquals(dummyWriters.get(1).recordsWritten(), 1);
    Assert.assertEquals(dummyWriters.get(1).flushCount, 1);
    Assert.assertEquals(dummyWriters.get(1).closeCount, 1);
    Assert.assertTrue(dummyWriters.get(1).committed);
    Assert.assertEquals(dummyWriters.get(1).handlerCalled, 1);
  }

  @Test
  public void testCloseAfterFlush()
      throws IOException {
    WorkUnitState state = new WorkUnitState();
    state.getJobState().setProp(CloseOnFlushWriterWrapper.WRITER_CLOSE_ON_FLUSH_KEY, "true");
    List<DummyWriter> dummyWriters = new ArrayList<>();
    CloseOnFlushWriterWrapper<byte[]> writer = getCloseOnFlushWriter(dummyWriters, state);

    byte[] record = new byte[]{'a', 'b', 'c', 'd'};

    writer.writeEnvelope(new RecordEnvelope(record));
    writer.getMessageHandler().handleMessage(FlushControlMessage.builder().build());

    Assert.assertEquals(dummyWriters.get(0).recordsWritten(), 1);
    Assert.assertEquals(dummyWriters.get(0).flushCount, 1);
    Assert.assertEquals(dummyWriters.get(0).closeCount, 1);
    Assert.assertTrue(dummyWriters.get(0).committed);
    Assert.assertEquals(dummyWriters.get(0).handlerCalled, 1);

    writer.close();

    // writer should not be closed multiple times
    Assert.assertEquals(dummyWriters.get(0).closeCount, 1);
  }


  private CloseOnFlushWriterWrapper getCloseOnFlushWriter(List<DummyWriter> dummyWriters, WorkUnitState state) {
    return new CloseOnFlushWriterWrapper<>(new Supplier<DataWriter<byte[]>>() {
      @Override
      public DataWriter<byte[]> get() {
        DummyWriter writer = new DummyWriter();
        dummyWriters.add(writer);
        return writer;
      }
    }, state.getJobState());
  }

  private static class DummyWriter implements DataWriter<byte[]> {
    private int recordsSeen = 0;
    private byte[] lastWrittenRecord;
    private int flushCount = 0;
    private int closeCount = 0;
    private boolean committed = false;
    private int handlerCalled = 0;

    DummyWriter() {
    }

    @Override
    public void write(byte[] record)
        throws IOException {
      this.recordsSeen++;
      this.lastWrittenRecord = record;
    }

    @Override
    public void commit()
        throws IOException {
      this.committed = true;
    }

    @Override
    public void cleanup()
        throws IOException {
    }

    @Override
    public long recordsWritten() {
      return this.recordsSeen;
    }

    @Override
    public long bytesWritten()
        throws IOException {
      return 0;
    }

    @Override
    public void close()
        throws IOException {
      this.closeCount++;
    }

    @Override
    public ControlMessageHandler getMessageHandler() {
      return new FlushControlMessageHandler(this) {
        @Override
        public void handleMessage(ControlMessage message) {
          handlerCalled++;
          if (message instanceof FlushControlMessage) {
            flush();
          }
        }
      };
    }

    @Override
    public void flush() {
      this.flushCount++;
    }
  }
}
