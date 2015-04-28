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

package gobblin.instrumented.qualitychecker;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.MetricsHelper;
import gobblin.configuration.State;
import gobblin.constructs.Constructs;
import gobblin.instrumented.qualitychecker.InstrumentedRowLevelPolicy;
import gobblin.metrics.MetricNames;
import gobblin.qualitychecker.row.RowLevelPolicy;


public class InstrumentedRowLevelPolicyTest {

  public class TestInstrumentedRowLevelPolicy extends InstrumentedRowLevelPolicy {

    public TestInstrumentedRowLevelPolicy(State state, Type type) {
      super(state, type);
    }

    @Override
    public Result executePolicyImpl(Object record) {
      return Result.PASSED;
    }
  }

  @Test
  public void test() {
    State state = new State();
    TestInstrumentedRowLevelPolicy policy = new TestInstrumentedRowLevelPolicy(state, null);
    testBase(policy);
  }

  @Test
  public void testDecorated() {
    State state = new State();
    InstrumentedRowLevelPolicyBase instrumentedPolicy = new InstrumentedRowLevelPolicyDecorator(
        new TestInstrumentedRowLevelPolicy(state, null)
    );
    testBase(instrumentedPolicy);

    InstrumentedRowLevelPolicyBase notInstrumentedPolicy = new InstrumentedRowLevelPolicyDecorator(
        new RowLevelPolicy(state, null) {
          @Override
          public Result executePolicy(Object record) {
            return Result.PASSED;
          }
        });
    testBase(notInstrumentedPolicy);
  }

  public void testBase(InstrumentedRowLevelPolicyBase policy) {
    policy.executePolicy("test");

    Map<String, Long> metrics = MetricsHelper.dumpMetrics(policy.getMetricContext());

    Assert.assertEquals(metrics.get(MetricNames.RowLevelPolicy.RECORDS_IN), Long.valueOf(1));
    Assert.assertEquals(metrics.get(MetricNames.RowLevelPolicy.RECORDS_PASSED), Long.valueOf(1));
    Assert.assertEquals(metrics.get(MetricNames.RowLevelPolicy.RECORDS_FAILED), Long.valueOf(0));
    Assert.assertEquals(metrics.get(MetricNames.RowLevelPolicy.CHECK_TIME), Long.valueOf(1));

    Assert.assertEquals(MetricsHelper.dumpTags(policy.getMetricContext()).get("construct"),
        Constructs.ROW_QUALITY_CHECKER.toString());

  }

}
