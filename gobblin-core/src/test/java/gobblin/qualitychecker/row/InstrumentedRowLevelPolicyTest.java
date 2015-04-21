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

package gobblin.qualitychecker.row;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.MetricsHelper;
import gobblin.configuration.State;


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

    policy.executePolicy("test");

    Map<String, Long> metrics = MetricsHelper.dumpMetrics(policy.instrumented);

    Assert.assertEquals(metrics.get("gobblin.qualitychecker.records.in"), Long.valueOf(1));
    Assert.assertEquals(metrics.get("gobblin.qualitychecker.records.passed"), Long.valueOf(1));
    Assert.assertEquals(metrics.get("gobblin.qualitychecker.records.failed"), Long.valueOf(0));
    Assert.assertEquals(metrics.get("gobblin.qualitychecker.policy.timer"), Long.valueOf(1));

    Assert.assertEquals(MetricsHelper.dumpTags(policy.instrumented).get("component"), "rowLevelPolicy");

  }

}
