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

package org.apache.gobblin.broker;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeInstance;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.gobblin_scopes.TaskScopeInstance;


public class GobblinBrokerCreationTest {

  @Test
  public void testCreationOfBrokers() throws Exception {

    // Correct creation behavior
    Config config = ConfigFactory.empty();

    SharedResourcesBrokerImpl<GobblinScopeTypes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config,
        new SimpleScope<GobblinScopeTypes>(GobblinScopeTypes.GLOBAL, "myGlobalScope"));
    SharedResourcesBrokerImpl<GobblinScopeTypes> jobBroker =
        topBroker.newSubscopedBuilder(new JobScopeInstance("myJob", "job123")).build();
    SharedResourcesBrokerImpl<GobblinScopeTypes>
        containerBroker = topBroker.newSubscopedBuilder(GobblinScopeTypes.CONTAINER.defaultScopeInstance()).build();
    SharedResourcesBrokerImpl<GobblinScopeTypes> taskBroker = jobBroker.newSubscopedBuilder(new TaskScopeInstance("taskabc"))
        .withAdditionalParentBroker(containerBroker).build();

    Assert.assertEquals(taskBroker.selfScope().getType(), GobblinScopeTypes.TASK);
    Assert.assertEquals(((TaskScopeInstance) taskBroker.selfScope()).getTaskId(), "taskabc");

    Assert.assertEquals(taskBroker.getScope(GobblinScopeTypes.CONTAINER).getType(), GobblinScopeTypes.CONTAINER);
    Assert.assertEquals(((GobblinScopeInstance) taskBroker.getScope(GobblinScopeTypes.CONTAINER)).getScopeId(), "container");
  }

  @Test
  public void testFailIfSubBrokerAtHigherScope() throws Exception {
    Config config = ConfigFactory.empty();
    SharedResourcesBrokerImpl<GobblinScopeTypes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config,
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    SharedResourcesBrokerImpl<GobblinScopeTypes> jobBroker =
        topBroker.newSubscopedBuilder(new JobScopeInstance("myJob", "job123")).build();

    try {
      jobBroker.newSubscopedBuilder(new GobblinScopeInstance(GobblinScopeTypes.INSTANCE, "instance")).build();
      Assert.fail();
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  @Test
  public void testFailIfIntermediateScopeHasNoDefault() throws Exception {
    Config config = ConfigFactory.empty();
    SharedResourcesBrokerImpl<GobblinScopeTypes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config,
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());

    // should trow error if an intermediate scope does not have a default
    try {
      topBroker.newSubscopedBuilder(new TaskScopeInstance("taskxyz")).build();
      Assert.fail();
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

  @Test
  public void testFailIfDifferentAncestors() throws Exception {
    // Correct creation behavior
    Config config = ConfigFactory.empty();

    SharedResourcesBrokerImpl<GobblinScopeTypes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config,
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    SharedResourcesBrokerImpl<GobblinScopeTypes> jobBroker =
        topBroker.newSubscopedBuilder(new JobScopeInstance("myJob", "job123")).build();

    SharedResourcesBrokerImpl<GobblinScopeTypes> topBroker2 = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config,
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    SharedResourcesBrokerImpl<GobblinScopeTypes>
        containerBroker = topBroker2.newSubscopedBuilder(GobblinScopeTypes.CONTAINER.defaultScopeInstance()).build();

    try {
      jobBroker.newSubscopedBuilder(new TaskScopeInstance("taskxyz")).withAdditionalParentBroker(containerBroker).build();
      Assert.fail();
    } catch (IllegalArgumentException iae) {
      // expected
    }
  }

}
