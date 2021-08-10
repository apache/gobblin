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

package org.apache.gobblin.multistage.util;

import com.google.gson.JsonObject;
import org.apache.gobblin.multistage.util.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class JsonUtilsTest {
  @Test
  public void testDeepCopy() {
    JsonObject source = new JsonObject();
    source.addProperty("name", "value");

    JsonObject replica = JsonUtils.deepCopy(source).getAsJsonObject();
    JsonObject same = source;

    source.remove("name");
    source.addProperty("name", "newValue");

    Assert.assertEquals(source.get("name").getAsString(), same.get("name").getAsString());
    Assert.assertNotEquals(source.get("name").getAsString(), replica.get("name").getAsString());
  }

  @Test
  public void testContains() {
    JsonObject a = new JsonObject();
    JsonObject b = new JsonObject();

    a.addProperty("name1", "value1");
    a.addProperty("name2", "value2");
    b.addProperty("name1", "value1");

    Assert.assertTrue(JsonUtils.contains(a, b));
    Assert.assertTrue(JsonUtils.contains("{\"name1\": \"value1\", \"name2\": \"value2\"}", b));

    b.addProperty("name2", "value2x");
    Assert.assertFalse(JsonUtils.contains(a, b));
    Assert.assertFalse(JsonUtils.contains("{\"name1\": \"value1\", \"name2\": \"value2\"}", b));

    b.addProperty("name3", "value3");
    Assert.assertFalse(JsonUtils.contains(a, b));
  }

  @Test
  public void testReplace() {
    JsonObject a = new JsonObject();
    JsonObject b = new JsonObject();

    a.addProperty("name1", "value1");
    b.addProperty("name1", "newValue1");

    Assert.assertEquals(JsonUtils.replace(a, b).toString(), "{\"name1\":\"newValue1\"}");

    a.addProperty("name2", "value1");
    Assert.assertEquals(JsonUtils.replace(a, b).toString(), "{\"name1\":\"newValue1\",\"name2\":\"value1\"}");
  }
}
