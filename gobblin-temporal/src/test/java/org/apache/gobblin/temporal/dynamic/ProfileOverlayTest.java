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

package org.apache.gobblin.temporal.dynamic;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.testng.annotations.Test;
import org.testng.Assert;


public class ProfileOverlayTest {

  @Test
  public void testAddingApplyOverlay() {
    Config config = ConfigFactory.parseString("key1=value1A, key4=value4");
    ProfileOverlay.Adding adding = new ProfileOverlay.Adding(
        Lists.newArrayList(new ProfileOverlay.KVPair("key1", "value1B"), new ProfileOverlay.KVPair("key2", "value2")));
    Config updatedConfig = adding.applyOverlay(config);
    Assert.assertEquals(updatedConfig.getString("key1"), "value1B");
    Assert.assertEquals(updatedConfig.getString("key2"), "value2");
    Assert.assertEquals(updatedConfig.getString("key4"), "value4");
  }

  @Test
  public void testRemovingApplyOverlay() {
    Config config = ConfigFactory.parseString("key1=value1, key2=value2");
    ProfileOverlay.Removing removing = new ProfileOverlay.Removing(Lists.newArrayList("key1"));
    Config updatedConfig = removing.applyOverlay(config);
    Assert.assertFalse(updatedConfig.hasPath("key1"));
    Assert.assertEquals(updatedConfig.getString("key2"), "value2");
  }

  @Test
  public void testComboApplyOverlay() {
    Config config = ConfigFactory.parseString("key1=value1, key2=value2, key3=value3");
    ProfileOverlay.Adding adding = new ProfileOverlay.Adding(
        Lists.newArrayList(new ProfileOverlay.KVPair("key4", "value4"), new ProfileOverlay.KVPair("key5", "value5")));
    ProfileOverlay.Removing removing = new ProfileOverlay.Removing(Lists.newArrayList("key2", "key4"));
    ProfileOverlay.Combo combo = ProfileOverlay.Combo.normalize(adding, removing);
    Config updatedConfig = combo.applyOverlay(config);
    Assert.assertEquals(updatedConfig.getString("key1"), "value1");
    Assert.assertEquals(updatedConfig.hasPath("key2"), false);
    Assert.assertEquals(updatedConfig.getString("key3"), "value3");
    Assert.assertEquals(updatedConfig.hasPath("key4"), false);
    Assert.assertEquals(updatedConfig.getString("key5"), "value5");

    // validate `Combo::normalize` works too:
    Assert.assertEquals(combo.getAdding().getAdditionPairs().size(), 1);
    Assert.assertEquals(combo.getAdding().getAdditionPairs().get(0), new ProfileOverlay.KVPair("key5", "value5"));
    Assert.assertEquals(combo.getRemoving().getRemovalKeys().size(), 2);
    Assert.assertEqualsNoOrder(combo.getRemoving().getRemovalKeys().toArray(), removing.getRemovalKeys().toArray());
  }

  @Test
  public void testAddingOver() {
    ProfileOverlay.Adding adding1 = new ProfileOverlay.Adding(
        Lists.newArrayList(new ProfileOverlay.KVPair("key1", "value1"), new ProfileOverlay.KVPair("key2", "value2A")));
    ProfileOverlay.Adding adding2 = new ProfileOverlay.Adding(
        Lists.newArrayList(new ProfileOverlay.KVPair("key2", "value2B"), new ProfileOverlay.KVPair("key3", "value3")));
    ProfileOverlay result = adding1.over(adding2);
    Config config = result.applyOverlay(ConfigFactory.empty());
    Assert.assertEquals(config.getString("key1"), "value1");
    Assert.assertEquals(config.getString("key2"), "value2A");
    Assert.assertEquals(config.getString("key3"), "value3");
  }

  @Test
  public void testRemovingOver() {
    ProfileOverlay.Removing removing1 = new ProfileOverlay.Removing(Lists.newArrayList("key1", "key2"));
    ProfileOverlay.Removing removing2 = new ProfileOverlay.Removing(Lists.newArrayList("key2", "key3"));
    ProfileOverlay result = removing1.over(removing2);
    Assert.assertTrue(result instanceof ProfileOverlay.Removing);
    ProfileOverlay.Removing removingResult = (ProfileOverlay.Removing) result;
    Assert.assertEqualsNoOrder(removingResult.getRemovalKeys().toArray(), new String[]{"key1", "key2", "key3"});

    Config config =
        result.applyOverlay(ConfigFactory.parseString("key1=value1, key2=value2, key3=value3, key4=value4"));
    Assert.assertFalse(config.hasPath("key1"));
    Assert.assertFalse(config.hasPath("key2"));
    Assert.assertFalse(config.hasPath("key3"));
    Assert.assertTrue(config.hasPath("key4"));
  }
}
