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

package org.apache.gobblin.instrumented.fork;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.MetricsHelper;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.Constructs;
import org.apache.gobblin.fork.ForkOperator;
import org.apache.gobblin.metrics.MetricNames;


public class InstrumentedForkOperatorTest {

  public class TestInstrumentedForkOperator extends InstrumentedForkOperator<String, String> {

    @Override
    public List<Boolean> forkDataRecordImpl(WorkUnitState workUnitState, String input) {
      List<Boolean> output = new ArrayList<>();
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
    public void close() throws IOException {

    }
  }

  public class TestForkOperator implements ForkOperator<String, String> {

    @Override
    public List<Boolean> forkDataRecord(WorkUnitState workUnitState, String input) {
      List<Boolean> output = new ArrayList<>();
      output.add(true);
      output.add(false);
      output.add(true);
      return output;
    }

    @Override
    public void init(WorkUnitState workUnitState) throws Exception {}

    @Override
    public int getBranches(WorkUnitState workUnitState) {
      return 0;
    }

    @Override
    public List<Boolean> forkSchema(WorkUnitState workUnitState, String input) {
      return null;
    }

    @Override
    public void close() throws IOException {

    }
  }

  @Test
  public void test() throws Exception {
    TestInstrumentedForkOperator fork = new TestInstrumentedForkOperator();
    testBase(fork);
  }

  @Test
  public void testDecorated() throws Exception {
    InstrumentedForkOperatorBase instrumentedFork =
        new InstrumentedForkOperatorDecorator(new TestInstrumentedForkOperator());
    testBase(instrumentedFork);

    InstrumentedForkOperatorBase notInstrumentedFork = new InstrumentedForkOperatorDecorator(new TestForkOperator());
    testBase(notInstrumentedFork);
  }

  public void testBase(InstrumentedForkOperatorBase<String, String> fork) throws Exception {
    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.METRICS_ENABLED_KEY, Boolean.toString(true));
    fork.init(state);

    fork.forkDataRecord(new WorkUnitState(), "in");

    Map<String, Long> metrics = MetricsHelper.dumpMetrics(fork.getMetricContext());
    Assert.assertEquals(metrics.get(MetricNames.ForkOperatorMetrics.RECORDS_IN_METER), Long.valueOf(1));
    Assert.assertEquals(metrics.get(MetricNames.ForkOperatorMetrics.FORKS_OUT_METER), Long.valueOf(2));
    Assert.assertEquals(metrics.get(MetricNames.ForkOperatorMetrics.FORK_TIMER), Long.valueOf(1));

    Assert.assertEquals(MetricsHelper.dumpTags(fork.getMetricContext()).get("construct"),
        Constructs.FORK_OPERATOR.toString());

  }

}
