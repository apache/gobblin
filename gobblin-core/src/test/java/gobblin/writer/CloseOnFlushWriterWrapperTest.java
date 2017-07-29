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
package gobblin.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.stream.RecordEnvelope;

public class CloseOnFlushWriterWrapperTest {

  @Test
  public void testCloseOnFlushDisabled()
      throws IOException {
    WorkUnitState state = new WorkUnitState();
    List<DummyWriter> dummyWriters = new ArrayList<>();
    CloseOnFlushWriterWrapper<byte[]> writer = getCloseOnFlushWriter(dummyWriters, state);

    byte[] record = new byte[]{'a', 'b', 'c', 'd'};

    writer.writeEnvelope(new RecordEnvelope(record));
    writer.flush();

    Assert.assertEquals(dummyWriters.get(0).recordsWritten(), 1);
    Assert.assertEquals(dummyWriters.get(0).flushCount, 1);
    Assert.assertEquals(dummyWriters.get(0).closed, false);
    Assert.assertEquals(dummyWriters.get(0).committed, false);
  }

  @Test
  public void testCloseOnFlushEnabled()
      throws IOException {
    WorkUnitState state = new WorkUnitState();
    state.getJobState().setProp(ConfigurationKeys.WRITER_CLOSE_ON_FLUSH_KEY, "true");
    List<DummyWriter> dummyWriters = new ArrayList<>();
    CloseOnFlushWriterWrapper<byte[]> writer = getCloseOnFlushWriter(dummyWriters, state);

    byte[] record = new byte[]{'a', 'b', 'c', 'd'};

    writer.writeEnvelope(new RecordEnvelope(record));
    writer.flush();

    Assert.assertEquals(dummyWriters.get(0).recordsWritten(), 1);
    Assert.assertEquals(dummyWriters.get(0).flushCount, 1);
    Assert.assertEquals(dummyWriters.get(0).closed, true);
    Assert.assertEquals(dummyWriters.get(0).committed, true);
  }

  @Test
  public void testWriteAfterFlush()
      throws IOException {
    WorkUnitState state = new WorkUnitState();
    state.getJobState().setProp(ConfigurationKeys.WRITER_CLOSE_ON_FLUSH_KEY, "true");
    List<DummyWriter> dummyWriters = new ArrayList<>();
    CloseOnFlushWriterWrapper<byte[]> writer = getCloseOnFlushWriter(dummyWriters, state);

    byte[] record = new byte[]{'a', 'b', 'c', 'd'};

    writer.writeEnvelope(new RecordEnvelope(record));
    writer.flush();

    Assert.assertEquals(dummyWriters.size(), 1);
    Assert.assertEquals(dummyWriters.get(0).recordsWritten(), 1);
    Assert.assertEquals(dummyWriters.get(0).flushCount, 1);
    Assert.assertEquals(dummyWriters.get(0).closed, true);
    Assert.assertEquals(dummyWriters.get(0).committed, true);

    writer.writeEnvelope(new RecordEnvelope(record));
    writer.flush();

    Assert.assertEquals(dummyWriters.size(), 2);
    Assert.assertEquals(dummyWriters.get(1).recordsWritten(), 1);
    Assert.assertEquals(dummyWriters.get(1).flushCount, 1);
    Assert.assertEquals(dummyWriters.get(1).closed, true);
    Assert.assertEquals(dummyWriters.get(1).committed, true);
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
    private boolean committed = false;
    private boolean closed = false;

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
      this.closed = true;
    }

    @Override
    public void flush() {
      this.flushCount++;
    }
  }
}
