/*
 * (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer;

import java.io.IOException;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.MetricsHelper;
import gobblin.configuration.State;


public class InstrumentedDataWriterTest {

  public class TestInstrumentedDataWriter extends InstrumentedDataWriter<String> {

    public TestInstrumentedDataWriter(State state) {
      super(state);
    }

    @Override
    public void writeImpl(String record)
        throws IOException {

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
  }

  @Test
  public void test() throws IOException {

    State state = new State();
    TestInstrumentedDataWriter writer = new TestInstrumentedDataWriter(state);

    writer.write("test");

    Map<String, Long> metrics = MetricsHelper.dumpMetrics(writer.instrumented);
    Assert.assertEquals(metrics.get("gobblin.writer.records.in"), Long.valueOf(1));
    Assert.assertEquals(metrics.get("gobblin.writer.records.written"), Long.valueOf(1));
    Assert.assertEquals(metrics.get("gobblin.writer.records.failed"), Long.valueOf(0));
    Assert.assertEquals(metrics.get("gobblin.writer.timer"), Long.valueOf(1));

    Assert.assertEquals(MetricsHelper.dumpTags(writer.instrumented).get("component"), "writer");

  }

}
