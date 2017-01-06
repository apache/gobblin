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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AutoscopedFactoryTest {

  @Test
  public void testAutoscoping() throws Exception {
    Config config = ConfigFactory.empty();

    config = TestFactory.setAutoScopeLevel(config, GobblinScopes.JOB);

    SharedResourcesBrokerImpl<GobblinScopes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config);
    SharedResourcesBrokerImpl<GobblinScopes> jobBroker = topBroker.newSubscopedBuilder(GobblinScopes.JOB, "job123").build();
    SharedResourcesBrokerImpl<GobblinScopes>
        containerBroker = topBroker.newSubscopedBuilder(GobblinScopes.CONTAINER, "thisContainer").build();
    SharedResourcesBrokerImpl<GobblinScopes> taskBroker = jobBroker.newSubscopedBuilder(GobblinScopes.TASK, "taskabc")
        .withAdditionalParentBroker(containerBroker).build();

    TestFactory.SharedResource jobScopedResource =
        taskBroker.getSharedResourceAtScope(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"), GobblinScopes.JOB);
    TestFactory.SharedResource taskScopedResource =
        taskBroker.getSharedResourceAtScope(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"), GobblinScopes.TASK);
    TestFactory.SharedResource autoscopedResource =
        taskBroker.getSharedResource(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"));

    Assert.assertEquals(jobScopedResource, autoscopedResource);
    Assert.assertNotEquals(taskScopedResource, autoscopedResource);
  }

  @Test
  public void testAutoscopedResourcesOnlyClosedInCorrectScope() throws Exception {
    Config config = ConfigFactory.empty();

    config = TestFactory.setAutoScopeLevel(config, GobblinScopes.JOB);

    SharedResourcesBrokerImpl<GobblinScopes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config);
    SharedResourcesBrokerImpl<GobblinScopes> jobBroker = topBroker.newSubscopedBuilder(GobblinScopes.JOB, "job123").build();
    SharedResourcesBrokerImpl<GobblinScopes>
        containerBroker = topBroker.newSubscopedBuilder(GobblinScopes.CONTAINER, "thisContainer").build();
    SharedResourcesBrokerImpl<GobblinScopes> taskBroker = jobBroker.newSubscopedBuilder(GobblinScopes.TASK, "taskabc")
        .withAdditionalParentBroker(containerBroker).build();

    TestFactory.SharedResource autoscopedResource =
        taskBroker.getSharedResource(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"));

    // since object autoscopes at job level, it should not be closed if we close the task broker
    taskBroker.close();
    Assert.assertFalse(autoscopedResource.isClosed());

    // however, when closing job broker, resource should be closed
    jobBroker.close();
    Assert.assertTrue(autoscopedResource.isClosed());

  }

}
