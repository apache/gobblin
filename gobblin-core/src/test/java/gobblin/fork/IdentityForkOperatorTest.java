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

package gobblin.fork;

import gobblin.configuration.ConfigurationKeys;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.WorkUnitState;


/**
 * Unit tests for {@link IdentityForkOperator}.
 *
 * @author Yinan Li
 */
@Test(groups = {"gobblin.fork"})
public class IdentityForkOperatorTest {

  @Test
  public void testForkMethods() {
    ForkOperator<String, String> dummyForkOperator = new IdentityForkOperator<String, String>();
    WorkUnitState workUnitState = new WorkUnitState();

    workUnitState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, 2);
    List<Boolean> schemas = dummyForkOperator.forkSchema(workUnitState, "");
    Assert.assertEquals(schemas, Arrays.asList(true, true));
    List<Boolean> records = dummyForkOperator.forkDataRecord(workUnitState, "");
    Assert.assertEquals(records, Arrays.asList(true, true));
    Assert.assertEquals(dummyForkOperator.getBranches(workUnitState), 2);

    workUnitState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, 0);
    schemas = dummyForkOperator.forkSchema(workUnitState, "");
    Assert.assertTrue(schemas.isEmpty());
    records = dummyForkOperator.forkDataRecord(workUnitState, "");
    Assert.assertTrue(records.isEmpty());
    Assert.assertEquals(dummyForkOperator.getBranches(workUnitState), 0);
  }
}
