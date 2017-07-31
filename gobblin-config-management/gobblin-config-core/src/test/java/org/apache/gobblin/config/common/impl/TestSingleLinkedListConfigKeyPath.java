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

package org.apache.gobblin.config.common.impl;

import java.util.HashSet;
import java.util.Set;

import org.apache.gobblin.config.store.api.ConfigKeyPath;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = { "gobblin.config.common.impl" })
public class TestSingleLinkedListConfigKeyPath {
  @Test
  public void testRootPath() {
    Assert.assertEquals(SingleLinkedListConfigKeyPath.ROOT.getAbsolutePathString(), "/");
    Assert.assertEquals(SingleLinkedListConfigKeyPath.ROOT.getOwnPathName(), "");
  }

  @Test(expectedExceptions = java.lang.UnsupportedOperationException.class)
  public void testGetParentOfRoot() {
    SingleLinkedListConfigKeyPath.ROOT.getParent();
  }

  @Test
  public void testNonRoot() {
    ConfigKeyPath data = SingleLinkedListConfigKeyPath.ROOT.createChild("data");
    Assert.assertEquals(data.getAbsolutePathString(), "/data");

    ConfigKeyPath profile = data.createChild("databases").createChild("identity").createChild("profile");
    Assert.assertEquals(profile.toString(), "/data/databases/identity/profile");
  }

  @Test
  public void testHash() {
    ConfigKeyPath data = SingleLinkedListConfigKeyPath.ROOT.createChild("data");

    ConfigKeyPath profile1 = data.createChild("databases").createChild("identity").createChild("profile");
    ConfigKeyPath profile2 = data.createChild("databases").createChild("identity").createChild("profile");

    Assert.assertFalse(profile1 == profile2);
    Assert.assertTrue(profile1.equals(profile2));
    Assert.assertEquals(profile1.hashCode(), profile2.hashCode());

    Set<ConfigKeyPath> testSet = new HashSet<ConfigKeyPath>();
    testSet.add(profile1);
    Assert.assertTrue(testSet.contains(profile2));
  }
}
