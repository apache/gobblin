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
package org.apache.gobblin.data.management.retention.action;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.data.management.policy.SelectAfterTimeBasedPolicy;


@Test(groups = { "gobblin.data.management.retention" })
public class RetentionActionTest {

  @Test
  public void testSelectionPolicyInit() throws Exception {

    // Using alias
    AccessControlAction testRetentionAction =
        new AccessControlAction(ConfigFactory.parseMap(ImmutableMap.<String, String> of("selection.policy.class",
            "SelectAfterTimeBasedPolicy", "selection.timeBased.lookbackTime", "7d")), null, ConfigFactory.empty());

    Assert.assertEquals(testRetentionAction.getSelectionPolicy().getClass(), SelectAfterTimeBasedPolicy.class);

    // Using complete class name
    testRetentionAction =
        new AccessControlAction(ConfigFactory.parseMap(ImmutableMap.<String, String> of("selection.policy.class",
            SelectAfterTimeBasedPolicy.class.getName(), "selection.timeBased.lookbackTime", "7d")), null,
            ConfigFactory.empty());

    Assert.assertEquals(testRetentionAction.getSelectionPolicy().getClass(), SelectAfterTimeBasedPolicy.class);
  }
}
