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

package gobblin.instrumented.qualitychecker;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.MetricsHelper;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.Constructs;
import gobblin.configuration.WorkUnitState;
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
    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.METRICS_ENABLED_KEY, Boolean.toString(true));
    TestInstrumentedRowLevelPolicy policy = new TestInstrumentedRowLevelPolicy(state, null);
    testBase(policy);
  }

  @Test
  public void testDecorated() {
    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.METRICS_ENABLED_KEY, Boolean.toString(true));
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

    Assert.assertEquals(metrics.get(MetricNames.RowLevelPolicyMetrics.RECORDS_IN_METER), Long.valueOf(1));
    Assert.assertEquals(metrics.get(MetricNames.RowLevelPolicyMetrics.RECORDS_PASSED_METER), Long.valueOf(1));
    Assert.assertEquals(metrics.get(MetricNames.RowLevelPolicyMetrics.RECORDS_FAILED_METER), Long.valueOf(0));
    Assert.assertEquals(metrics.get(MetricNames.RowLevelPolicyMetrics.CHECK_TIMER), Long.valueOf(1));

    Assert.assertEquals(MetricsHelper.dumpTags(policy.getMetricContext()).get("construct"),
        Constructs.ROW_QUALITY_CHECKER.toString());

  }

}
