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

package gobblin.broker;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class GobblinBrokerCreationTest {

  @Test
  public void testCreationOfBrokers() throws Exception {

    // Correct creation behavior
    Config config = ConfigFactory.empty();

    SharedResourcesBrokerImpl<GobblinScopes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config);
    SharedResourcesBrokerImpl<GobblinScopes> jobBroker = topBroker.newSubscopedBuilder(GobblinScopes.JOB, "job123").build();
    SharedResourcesBrokerImpl<GobblinScopes>
        containerBroker = topBroker.newSubscopedBuilder(GobblinScopes.CONTAINER, "thisContainer").build();
    SharedResourcesBrokerImpl<GobblinScopes> taskBroker = jobBroker.newSubscopedBuilder(GobblinScopes.TASK, "taskabc")
        .withAdditionalParentBroker(containerBroker).build();

    Assert.assertEquals(taskBroker.selfScope().getType(), GobblinScopes.TASK);
    Assert.assertEquals(taskBroker.selfScope().getScopeId(), "taskabc");

    Assert.assertEquals(taskBroker.getScope(GobblinScopes.CONTAINER).getType(), GobblinScopes.CONTAINER);
    Assert.assertEquals(taskBroker.getScope(GobblinScopes.CONTAINER).getScopeId(), "thisContainer");
  }

  @Test
  public void testFailIfSubBrokerAtHigherScope() throws Exception {
    Config config = ConfigFactory.empty();
    SharedResourcesBrokerImpl<GobblinScopes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config);
    SharedResourcesBrokerImpl<GobblinScopes> jobBroker = topBroker.newSubscopedBuilder(GobblinScopes.JOB, "job123").build();

    try {
      jobBroker.newSubscopedBuilder(GobblinScopes.GLOBAL, "multitask").build();
      Assert.fail();
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  @Test
  public void testFailIfIntermediateScopeHasNoDefault() throws Exception {
    Config config = ConfigFactory.empty();
    SharedResourcesBrokerImpl<GobblinScopes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config);

    // should trow error if an intermediate scope does not have a default
    try {
      topBroker.newSubscopedBuilder(GobblinScopes.TASK, "taskxyz").build();
      Assert.fail();
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  @Test
  public void testFailIfDifferentAncestors() throws Exception {
    // Correct creation behavior
    Config config = ConfigFactory.empty();

    SharedResourcesBrokerImpl<GobblinScopes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config);
    SharedResourcesBrokerImpl<GobblinScopes> jobBroker = topBroker.newSubscopedBuilder(GobblinScopes.JOB, "job123").build();

    SharedResourcesBrokerImpl<GobblinScopes> topBroker2 = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config);
    SharedResourcesBrokerImpl<GobblinScopes>
        containerBroker = topBroker2.newSubscopedBuilder(GobblinScopes.CONTAINER, "thisContainer").build();

    try {
      jobBroker.newSubscopedBuilder(GobblinScopes.TASK, "taskxyz").withAdditionalParentBroker(containerBroker).build();
      Assert.fail();
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

}
