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
import java.util.concurrent.TimeUnit;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.writer.ThrottleWriter.ThrottleType;
import gobblin.util.FinalState;

import org.apache.commons.lang3.mutable.MutableLong;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;


@Test(groups = { "gobblin.writer" })
public class ThrottleWriterTest {

  public void testThrottleQps() throws IOException {
    DataWriter<Void> writer = mock(DataWriter.class);
    int parallelism = 2;
    int qps = 4;
    DataWriter<Void> throttleWriter = setup(writer, parallelism, qps, ThrottleType.QPS);

    int count = 0;
    long duration = 10L;
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (stopwatch.elapsed(TimeUnit.SECONDS) <= duration) {
      throttleWriter.writeEnvelope(null);
      count++;
    }

    int expected = (int) (qps * duration);
    Assert.assertTrue(count <= expected + qps * 2, "Request too high " + count + " , QPS " + qps + " for duration " + duration + " second ");
    Assert.assertTrue(count >= expected - qps * 2, "Request too low " + count + " , QPS " + qps + " for duration " + duration + " second ");
  }

  public void testThrottleBytes() throws IOException {
    DataWriter<Void> writer = mock(DataWriter.class);
    final MutableLong mockBytes = new MutableLong();
    when(writer.bytesWritten()).thenAnswer(new Answer<Long>() {
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        mockBytes.add(1L); //Delta bytes
        return mockBytes.getValue();
      }
    });

    int parallelism = 2;
    int bps = 2;
    DataWriter<Void> throttleWriter = setup(writer, parallelism, bps, ThrottleType.Bytes);

    int count = 0;
    long duration = 10L;
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (stopwatch.elapsed(TimeUnit.SECONDS) <= duration) {
      throttleWriter.writeEnvelope(null);
      count++;
    }

    int expected = (int) (bps * duration);
    Assert.assertTrue(count <= expected + bps * 2);
    Assert.assertTrue(count >= expected - bps * 2);
  }

  public void testGetFinalState() throws IOException {
    PartitionedDataWriter writer = mock(PartitionedDataWriter.class);
    when(writer.getFinalState()).thenReturn(new State());

    int parallelism = 2;
    int qps = 4;
    DataWriter<Void> throttleWriter = setup(writer, parallelism, qps, ThrottleType.QPS);

    State state = ((FinalState) throttleWriter).getFinalState();

    verify(writer, times(1)).getFinalState();
    Assert.assertTrue(state.contains(ThrottleWriter.THROTTLED_TIME_KEY));
  }

  private DataWriter<Void> setup(DataWriter<Void> writer, int parallelism, int rate, ThrottleType type) throws IOException {
    State state = new State();

    state.appendToSetProp(ThrottleWriter.WRITER_LIMIT_RATE_LIMIT_KEY, Integer.toString(rate * parallelism));
    state.appendToSetProp(ThrottleWriter.WRITER_THROTTLE_TYPE_KEY, type.name());

    state.appendToSetProp(ConfigurationKeys.TASK_EXECUTOR_THREADPOOL_SIZE_KEY, Integer.toString(parallelism));
    state.appendToSetProp(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS, Integer.toString(parallelism));

    DataWriterWrapperBuilder<Void> builder = new DataWriterWrapperBuilder<>(writer, state);
    return builder.build();
  }
}
