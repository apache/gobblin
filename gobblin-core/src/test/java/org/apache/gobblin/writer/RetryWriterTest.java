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

import static org.mockito.Mockito.*;

import java.io.IOException;

import gobblin.configuration.State;
import gobblin.stream.RecordEnvelope;
import gobblin.writer.exception.NonTransientException;
import gobblin.util.FinalState;

import org.junit.Assert;
import org.testng.annotations.Test;

@Test(groups = { "gobblin.writer" })
public class RetryWriterTest {

  public void retryTest() throws IOException {
    DataWriter<Void> writer = mock(DataWriter.class);
    doThrow(new RuntimeException()).when(writer).writeEnvelope(any(RecordEnvelope.class));

    DataWriterWrapperBuilder<Void> builder = new DataWriterWrapperBuilder<>(writer, new State());
    DataWriter<Void> retryWriter = builder.build();
    try {
      retryWriter.writeEnvelope(new RecordEnvelope<>(null));
      Assert.fail("Should have failed.");
    } catch (Exception e) { }

    verify(writer, times(5)).writeEnvelope(any(RecordEnvelope.class));
  }

  public void retryTestNonTransientException() throws IOException {
    DataWriter<Void> writer = mock(DataWriter.class);
    doThrow(new NonTransientException()).when(writer).writeEnvelope(any(RecordEnvelope.class));

    DataWriterWrapperBuilder<Void> builder = new DataWriterWrapperBuilder<>(writer, new State());
    DataWriter<Void> retryWriter = builder.build();
    try {
      retryWriter.writeEnvelope(new RecordEnvelope<>(null));
      Assert.fail("Should have failed.");
    } catch (Exception e) { }

    verify(writer, atMost(1)).writeEnvelope(any(RecordEnvelope.class));
  }

  public void retryTestSuccess() throws IOException {
    DataWriter<Void> writer = mock(DataWriter.class);

    DataWriterWrapperBuilder<Void> builder = new DataWriterWrapperBuilder<>(writer, new State());
    DataWriter<Void> retryWriter = builder.build();
    retryWriter.writeEnvelope(new RecordEnvelope<>(null));

    verify(writer, times(1)).writeEnvelope(any(RecordEnvelope.class));
  }

  public void retryGetFinalState() throws IOException {
    PartitionedDataWriter writer = mock(PartitionedDataWriter.class);
    when(writer.getFinalState()).thenReturn(new State());

    DataWriterWrapperBuilder<Void> builder = new DataWriterWrapperBuilder<>(writer, new State());
    DataWriter<Void> retryWriter = builder.build();
    State state = ((FinalState) retryWriter).getFinalState();

    verify(writer, times(1)).getFinalState();
    Assert.assertTrue(state.contains(RetryWriter.FAILED_WRITES_KEY));
  }
}
