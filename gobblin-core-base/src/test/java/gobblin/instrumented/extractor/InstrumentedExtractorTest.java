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

package gobblin.instrumented.extractor;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.MetricsHelper;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.Constructs;
import gobblin.metrics.MetricNames;
import gobblin.records.RecordStreamWithMetadata;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.stream.RecordEnvelope;


public class InstrumentedExtractorTest {

  public class TestInstrumentedExtractor extends InstrumentedExtractor<String, String> {

    public TestInstrumentedExtractor(WorkUnitState workUnitState) {
      super(workUnitState);
    }

    @Override
    public String readRecordImpl(String reuse)
        throws DataRecordException, IOException {
      return "test";
    }

    @Override
    public String getSchema() {
      return null;
    }

    @Override
    public long getExpectedRecordCount() {
      return 0;
    }

    @Override
    public long getHighWatermark() {
      return 0;
    }
  }

  public class TestExtractor implements Extractor<String, String> {

    @Override
    public String readRecord(String reuse)
        throws DataRecordException, IOException {
      return "test";
    }

    @Override
    public String getSchema() {
      return null;
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
    public void close()
        throws IOException {

    }
  }

  @Test
  public void test() throws DataRecordException, IOException {
    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.METRICS_ENABLED_KEY, Boolean.toString(true));
    TestInstrumentedExtractor extractor = new TestInstrumentedExtractor(state);
    testBase(extractor);
  }

  @Test
  public void testDecorated() throws DataRecordException, IOException {
    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.METRICS_ENABLED_KEY, Boolean.toString(true));
    InstrumentedExtractorBase instrumentedExtractor = new InstrumentedExtractorDecorator(state,
        new TestInstrumentedExtractor(state)
    );
    testBase(instrumentedExtractor);

    InstrumentedExtractorBase nonInstrumentedExtractor = new InstrumentedExtractorDecorator(state,
        new TestExtractor());
    testBase(nonInstrumentedExtractor);
  }

  public void testBase(InstrumentedExtractorBase<String, String> extractor)
      throws DataRecordException, IOException {

    RecordStreamWithMetadata<String, String> stream = extractor.recordStream(new AtomicBoolean(false));
    RecordEnvelope<String> r = (RecordEnvelope<String>) stream.getRecordStream().firstOrError().blockingGet();

    Map<String, Long> metrics = MetricsHelper.dumpMetrics(extractor.getMetricContext());
    Assert.assertEquals(metrics.get(MetricNames.ExtractorMetrics.RECORDS_READ_METER), Long.valueOf(1));
    Assert.assertEquals(metrics.get(MetricNames.ExtractorMetrics.RECORDS_FAILED_METER), Long.valueOf(0));
    Assert.assertEquals(metrics.get(MetricNames.ExtractorMetrics.EXTRACT_TIMER), Long.valueOf(1));

    Assert.assertEquals(MetricsHelper.dumpTags(extractor.getMetricContext()).get("construct"),
        Constructs.EXTRACTOR.toString());
  }

}
