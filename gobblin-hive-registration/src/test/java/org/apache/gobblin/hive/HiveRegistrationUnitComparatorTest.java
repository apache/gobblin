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

package org.apache.gobblin.hive;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;

public class HiveRegistrationUnitComparatorTest {

  @Test
  public void testCheckExistingIsSuperstate()
      throws Exception {

    String key1 = "key1";
    String value1 = "value1";
    String key2 = "key2";
    String value2 = "value2";

    State existingState = new State();
    State newState = new State();

    HiveRegistrationUnitComparator comparator =
        new HiveRegistrationUnitComparator<>(null, null);
    comparator.checkExistingIsSuperstate(existingState, newState);
    Assert.assertFalse(comparator.result);

    newState.setProp(key1, value1);
    comparator =
        new HiveRegistrationUnitComparator<>(null, null);
    comparator.checkExistingIsSuperstate(existingState, newState);
    Assert.assertTrue(comparator.result);

    existingState.setProp(key1, value2);
    comparator =
        new HiveRegistrationUnitComparator<>(null, null);
    comparator.checkExistingIsSuperstate(existingState, newState);
    Assert.assertTrue(comparator.result);

    existingState.setProp(key1, value1);
    existingState.setProp(key2, value2);
    comparator =
        new HiveRegistrationUnitComparator<>(null, null);
    comparator.checkExistingIsSuperstate(existingState, newState);
    Assert.assertFalse(comparator.result);
  }
}
