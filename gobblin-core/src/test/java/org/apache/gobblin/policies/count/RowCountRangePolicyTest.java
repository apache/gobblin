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

package org.apache.gobblin.policies.count;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicy;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicy.Result;

import org.testng.Assert;
import org.testng.annotations.Test;


public class RowCountRangePolicyTest {

  @Test
  public void testRangePolicyFailure() {
    RowCountRangePolicy rangePolicy = new RowCountRangePolicy(getTestState(4, 1, 0.5), TaskLevelPolicy.Type.FAIL);
    Assert.assertEquals(rangePolicy.executePolicy(), Result.FAILED);

    rangePolicy = new RowCountRangePolicy(getTestState(20, 8, 0.2), TaskLevelPolicy.Type.FAIL);
    Assert.assertEquals(rangePolicy.executePolicy(), Result.FAILED);
  }

  @Test
  public void testRangePolicySuccess() {
    RowCountRangePolicy rangePolicy = new RowCountRangePolicy(getTestState(4, 3, 0.8), TaskLevelPolicy.Type.FAIL);
    Assert.assertEquals(rangePolicy.executePolicy(), Result.PASSED);

    rangePolicy = new RowCountRangePolicy(getTestState(20, 12, 0.5), TaskLevelPolicy.Type.FAIL);
    Assert.assertEquals(rangePolicy.executePolicy(), Result.PASSED);
  }

  private State getTestState(long recordsRead, long recordsWritten, double range) {
    State state = new State();
    state.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, recordsRead);
    state.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN, recordsWritten);
    state.setProp(ConfigurationKeys.ROW_COUNT_RANGE, range);
    return state;
  }
}
