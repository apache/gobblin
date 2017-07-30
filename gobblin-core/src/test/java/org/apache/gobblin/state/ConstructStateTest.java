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

package gobblin.state;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import gobblin.Constructs;
import gobblin.configuration.WorkUnitState;


public class ConstructStateTest {

  @Test
  public void test() {
    ConstructState constructState = new ConstructState();
    WorkUnitState workUnitState = new WorkUnitState();

    String overrideKey = "overrideMe";
    String nonOverrideKey = "overrideMe.not";

    String workUnitToken = "workUnit";
    String constructToken = "construct";

    workUnitState.setProp(overrideKey, workUnitToken);
    workUnitState.setProp(nonOverrideKey, workUnitToken);
    constructState.addOverwriteProperties(ImmutableMap.<String, String>builder().put(overrideKey, constructToken).build());
    constructState.setProp(nonOverrideKey, constructToken);

    constructState.mergeIntoWorkUnitState(workUnitState);

    Assert.assertEquals(workUnitState.getProp(overrideKey), constructToken);
    Assert.assertEquals(workUnitState.getProp(nonOverrideKey), workUnitToken);
    Assert.assertEquals(workUnitState.getPropertyNames().size(), 3);

  }

  @Test
  public void testCombineConstructStates() {
    ConstructState constructState = new ConstructState();
    ConstructState constructState2 = new ConstructState();

    String overrideKey = "overrideMe";
    String overrideKey2 = "overrideMe2";
    String nonOverrideKey = "overrideMe.not";

    String str1 = "str1";
    String str2 = "str2";

    constructState.addOverwriteProperties(ImmutableMap.<String, String>builder().put(overrideKey, str1).build());
    constructState.addOverwriteProperties(ImmutableMap.<String, String>builder().put(overrideKey2, str1).build());
    constructState.setProp(nonOverrideKey, str1);
    constructState2.addOverwriteProperties(ImmutableMap.<String, String>builder().put(overrideKey, str2).build());
    constructState2.setProp(nonOverrideKey, str2);

    constructState.addConstructState(Constructs.CONVERTER, constructState2);

    Properties overrideProperties = constructState.getOverwriteProperties();

    Assert.assertEquals(overrideProperties.getProperty(overrideKey), str2);
    Assert.assertEquals(overrideProperties.getProperty(overrideKey2), str1);
    Assert.assertEquals(constructState.getPropertyNames().size(), 3);

  }

}
