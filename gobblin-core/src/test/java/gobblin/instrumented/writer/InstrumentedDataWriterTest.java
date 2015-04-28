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

package gobblin.instrumented.writer;

import java.io.IOException;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.MetricsHelper;
import gobblin.configuration.State;
import gobblin.constructs.Constructs;
import gobblin.instrumented.writer.InstrumentedDataWriter;
import gobblin.metrics.MetricNames;
import gobblin.writer.DataWriter;


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

  public class TestDataWriter implements DataWriter<String> {

    @Override
    public void close()
        throws IOException {

    }

    @Override
    public void write(String record)
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
    testBase(writer);
  }

  @Test
  public void testDecorated() throws IOException {
    State state = new State();
    InstrumentedDataWriterBase instrumentedWriter = new InstrumentedDataWriterDecorator(
        new TestInstrumentedDataWriter(state), state
    );
    testBase(instrumentedWriter);

    InstrumentedDataWriterBase notInstrumentedWriter = new InstrumentedDataWriterDecorator(
        new TestDataWriter(), state);
    testBase(notInstrumentedWriter);
  }

  public void testBase(InstrumentedDataWriterBase writer) throws IOException {

    writer.write("test");

    Map<String, Long> metrics = MetricsHelper.dumpMetrics(writer.getMetricContext());
    Assert.assertEquals(metrics.get(MetricNames.DataWriter.RECORDS_IN), Long.valueOf(1));
    Assert.assertEquals(metrics.get(MetricNames.DataWriter.RECORDS_WRITTEN), Long.valueOf(1));
    Assert.assertEquals(metrics.get(MetricNames.DataWriter.RECORDS_FAILED), Long.valueOf(0));
    Assert.assertEquals(metrics.get(MetricNames.DataWriter.WRITE_TIME), Long.valueOf(1));

    Assert.assertEquals(MetricsHelper.dumpTags(writer.getMetricContext()).get("construct"),
        Constructs.WRITER.toString());

  }

}
