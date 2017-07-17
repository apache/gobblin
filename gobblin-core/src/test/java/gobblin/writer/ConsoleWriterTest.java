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

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.source.extractor.DefaultCheckpointableWatermark;
import gobblin.stream.RecordEnvelope;
import gobblin.source.extractor.extract.LongWatermark;

import static org.mockito.Mockito.mock;


public class ConsoleWriterTest {

  @Test
  public void testNull()
  {
    ConsoleWriter<String> consoleWriter = new ConsoleWriter<>();
    String foo = null;
    try {
      consoleWriter.write(foo);
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw an exception on null");
    }
  }

  @Test
  public void testInteger()
  {
    ConsoleWriter<Integer> consoleWriter = new ConsoleWriter<>();
    Integer foo = 1;
    try {
      consoleWriter.write(foo);
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw an exception on writing an Integer");
    }
  }

  private class TestObject {

  };

  @Test
  public void testObject()
  {

    TestObject testObject = new TestObject();
    ConsoleWriter<TestObject> consoleWriter = new ConsoleWriter<>();
    try {
      consoleWriter.write(testObject);
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw an exception on writing an object that doesn't explicitly implement toString");
    }

    testObject = null;
    try {
      consoleWriter.write(testObject);
    }
    catch (Exception e)
    {
      Assert.fail("Should not throw an exception on writing a null object");
    }

  }

  private void writeEnvelope(ConsoleWriter consoleWriter, String content, String source, long value)
      throws IOException {
    CheckpointableWatermark watermark =
        new DefaultCheckpointableWatermark(source, new LongWatermark(value));
    AcknowledgableWatermark ackable = new AcknowledgableWatermark(watermark);
    RecordEnvelope<String> mockEnvelope = new RecordEnvelope<>(content).withAckableWatermark(ackable);
    consoleWriter.writeEnvelope(mockEnvelope);
    Assert.assertTrue(ackable.isAcked());
  }

  @Test
  public void testWatermarkWrite()
      throws IOException {
    ConsoleWriter<TestObject> consoleWriter = new ConsoleWriter<>();
    writeEnvelope(consoleWriter, "hello 1", "dataset1", 1);
    writeEnvelope(consoleWriter, "hello 2", "dataset1", 2);
    writeEnvelope(consoleWriter, "hello 2", "dataset2", 1);
  }

}
