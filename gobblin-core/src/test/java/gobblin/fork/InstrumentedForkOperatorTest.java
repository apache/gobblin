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

package gobblin.fork;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import gobblin.MetricsHelper;
import gobblin.configuration.WorkUnitState;


public class InstrumentedForkOperatorTest {

  public class TestInstrumentedForkOperator extends InstrumentedForkOperator<String, String> {

    @Override
    public List<Boolean> forkDataRecordImpl(WorkUnitState workUnitState, String input) {
      List<Boolean> output = new ArrayList<Boolean>();
      output.add(true);
      output.add(false);
      output.add(true);
      return output;
    }

    @Override
    public int getBranches(WorkUnitState workUnitState) {
      return 0;
    }

    @Override
    public List<Boolean> forkSchema(WorkUnitState workUnitState, String input) {
      return null;
    }

    @Override
    public void close()
        throws IOException {

    }
  }

  @Test
  public void test() throws Exception {
    TestInstrumentedForkOperator fork = new TestInstrumentedForkOperator();
    fork.init(new WorkUnitState());

    fork.forkDataRecord(new WorkUnitState(), "in");

    Map<String, Long> metrics = MetricsHelper.dumpMetrics(fork.metricContext);
    Assert.assertEquals(metrics.get("gobblin.fork.operator.records.in"), Long.valueOf(1));
    Assert.assertEquals(metrics.get("gobblin.fork.operator.forks.out"), Long.valueOf(2));
    Assert.assertEquals(metrics.get("gobblin.fork.operator.timer"), Long.valueOf(1));

    Assert.assertEquals(MetricsHelper.dumpTags(fork.metricContext).get("component"), "forkOperator");

  }

}
