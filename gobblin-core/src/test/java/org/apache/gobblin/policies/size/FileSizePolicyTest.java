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

package org.apache.gobblin.policies.size;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicy;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FileSizePolicyTest {

  @Test
  public void testPolicyPass() {
    State state = new State();
    state.setProp(FileSizePolicy.BYTES_READ_KEY, 1000L);
    state.setProp(FileSizePolicy.BYTES_WRITTEN_KEY, 1000L);

    FileSizePolicy policy = new FileSizePolicy(state, TaskLevelPolicy.Type.FAIL);
    Assert.assertTrue(policy.executePolicy().equals(TaskLevelPolicy.Result.PASSED));
  }

  @Test
  public void testPolicyFail() {
    State state = new State();
    state.setProp(FileSizePolicy.BYTES_READ_KEY, 1000L);
    state.setProp(FileSizePolicy.BYTES_WRITTEN_KEY, 900L);

    FileSizePolicy policy = new FileSizePolicy(state, TaskLevelPolicy.Type.FAIL);
    Assert.assertTrue(policy.executePolicy().equals(TaskLevelPolicy.Result.FAILED));
  }

  @Test
  public void testPolicyWithTolerance() {
    State state = new State();
    state.setProp(FileSizePolicy.BYTES_READ_KEY, 1000L);
    state.setProp(FileSizePolicy.BYTES_WRITTEN_KEY, 999L);

    FileSizePolicy policy = new FileSizePolicy(state, TaskLevelPolicy.Type.FAIL);
    Assert.assertTrue(policy.executePolicy().equals(TaskLevelPolicy.Result.FAILED));
  }
}