/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.data.management.retention.action;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;

import gobblin.data.management.policy.SelectAfterTimeBasedPolicy;


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
