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

package gobblin.source.extractor;

import java.io.IOException;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.MetricsHelper;
import gobblin.configuration.WorkUnitState;


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

  @Test
  public void test() throws DataRecordException, IOException {
    WorkUnitState state = new WorkUnitState();
    TestInstrumentedExtractor extractor = new TestInstrumentedExtractor(state);

    extractor.readRecord("");

    Map<String, Long> metrics = MetricsHelper.dumpMetrics(extractor.instrumented);
    Assert.assertEquals(metrics.get("gobblin.extractor.records.read"), Long.valueOf(1));
    Assert.assertEquals(metrics.get("gobblin.extractor.records.failed"), Long.valueOf(0));
    Assert.assertEquals(metrics.get("gobblin.extractor.extract.time"), Long.valueOf(1));

    Assert.assertEquals(MetricsHelper.dumpTags(extractor.instrumented).get("component"), "extractor");

  }

}
