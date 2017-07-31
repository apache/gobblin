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

package org.apache.gobblin.configuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;


public class StateTest {

  @Test
  public void testState()
      throws IOException {
    State state = new State();

    Assert.assertEquals(state.getProp("string", "some string"), "some string");
    Assert.assertEquals(state.getPropAsList("list", "item1,item2").get(0), "item1");
    Assert.assertEquals(state.getPropAsList("list", "item1,item2").get(1), "item2");
    Assert.assertEquals(state.getPropAsLong("long", Long.MAX_VALUE), Long.MAX_VALUE);
    Assert.assertEquals(state.getPropAsInt("int", Integer.MAX_VALUE), Integer.MAX_VALUE);
    Assert.assertEquals(state.getPropAsDouble("double", Double.MAX_VALUE), Double.MAX_VALUE);
    Assert.assertEquals(state.getPropAsBoolean("boolean", true), true);

    state.setProp("string", "some string");
    state.setProp("list", "item1,item2");
    state.setProp("long", Long.MAX_VALUE);
    state.setProp("int", Integer.MAX_VALUE);
    state.setProp("double", Double.MAX_VALUE);
    state.setProp("boolean", true);

    Assert.assertEquals(state.getProp("string"), "some string");
    Assert.assertEquals(state.getPropAsList("list").get(0), "item1");
    Assert.assertEquals(state.getPropAsList("list").get(1), "item2");
    Assert.assertEquals(state.getPropAsLong("long"), Long.MAX_VALUE);
    Assert.assertEquals(state.getPropAsInt("int"), Integer.MAX_VALUE);
    Assert.assertEquals(state.getPropAsDouble("double"), Double.MAX_VALUE);
    Assert.assertEquals(state.getPropAsBoolean("boolean"), true);

    state.setProp("string", "some other string");
    state.setProp("list", "item3,item4");
    state.setProp("long", Long.MIN_VALUE);
    state.setProp("int", Integer.MIN_VALUE);
    state.setProp("double", Double.MIN_VALUE);
    state.setProp("boolean", false);

    Assert.assertNotEquals(state.getProp("string", "some string"), "some string");
    Assert.assertNotEquals(state.getPropAsList("list", "item1,item2").get(0), "item1");
    Assert.assertNotEquals(state.getPropAsList("list", "item1,item2").get(1), "item2");
    Assert.assertNotEquals(state.getPropAsLong("long", Long.MAX_VALUE), Long.MAX_VALUE);
    Assert.assertNotEquals(state.getPropAsInt("int", Integer.MAX_VALUE), Integer.MAX_VALUE);
    Assert.assertNotEquals(state.getPropAsDouble("double", Double.MAX_VALUE), Double.MAX_VALUE);
    Assert.assertNotEquals(state.getPropAsBoolean("boolean", true), true);

    Assert.assertNotEquals(state.getProp("string"), "some string");
    Assert.assertNotEquals(state.getPropAsList("list").get(0), "item1");
    Assert.assertNotEquals(state.getPropAsList("list").get(1), "item2");
    Assert.assertNotEquals(state.getPropAsLong("long"), Long.MAX_VALUE);
    Assert.assertNotEquals(state.getPropAsInt("int"), Integer.MAX_VALUE);
    Assert.assertNotEquals(state.getPropAsDouble("double"), Double.MAX_VALUE);
    Assert.assertNotEquals(state.getPropAsBoolean("boolean"), true);

    Assert.assertEquals(state.getProp("string"), "some other string");
    Assert.assertEquals(state.getPropAsList("list").get(0), "item3");
    Assert.assertEquals(state.getPropAsList("list").get(1), "item4");
    Assert.assertEquals(state.getPropAsLong("long"), Long.MIN_VALUE);
    Assert.assertEquals(state.getPropAsInt("int"), Integer.MIN_VALUE);
    Assert.assertEquals(state.getPropAsDouble("double"), Double.MIN_VALUE);
    Assert.assertEquals(state.getPropAsBoolean("boolean"), false);

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(1024);
    DataOutputStream out = new DataOutputStream(byteStream);

    state.write(out);

    DataInputStream in = new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));

    state = new State();

    Assert.assertEquals(state.getProp("string"), null);
    Assert.assertEquals(state.getProp("list"), null);
    Assert.assertEquals(state.getProp("long"), null);
    Assert.assertEquals(state.getProp("int"), null);
    Assert.assertEquals(state.getProp("double"), null);
    Assert.assertEquals(state.getProp("boolean"), null);

    state.readFields(in);

    Assert.assertEquals(state.getProp("string"), "some other string");
    Assert.assertEquals(state.getPropAsList("list").get(0), "item3");
    Assert.assertEquals(state.getPropAsList("list").get(1), "item4");
    Assert.assertEquals(state.getPropAsLong("long"), Long.MIN_VALUE);
    Assert.assertEquals(state.getPropAsInt("int"), Integer.MIN_VALUE);
    Assert.assertEquals(state.getPropAsDouble("double"), Double.MIN_VALUE);
    Assert.assertEquals(state.getPropAsBoolean("boolean"), false);

    State state2 = new State();
    state2.addAll(state);

    Assert.assertEquals(state2.getProp("string"), "some other string");
    Assert.assertEquals(state2.getPropAsList("list").get(0), "item3");
    Assert.assertEquals(state2.getPropAsList("list").get(1), "item4");
    Assert.assertEquals(state2.getPropAsLong("long"), Long.MIN_VALUE);
    Assert.assertEquals(state2.getPropAsInt("int"), Integer.MIN_VALUE);
    Assert.assertEquals(state2.getPropAsDouble("double"), Double.MIN_VALUE);
    Assert.assertEquals(state2.getPropAsBoolean("boolean"), false);
  }

  @Test
  public void testInterningOfKeyValues() throws Exception {

    // Prove we can identify interned keys
    String nonInterned = new String("myKey"); // not interned
    String interned = new String("myInternedKey").intern(); // interned

    Assert.assertFalse(isInterned(nonInterned));
    Assert.assertTrue(isInterned(interned));

    State state = new State();
    state.setProp(new String("someKey"), new String("someValue"));

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DataOutput dataOutput = new DataOutputStream(outputStream);
    state.write(dataOutput);
    outputStream.flush();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    DataInput dataInput = new DataInputStream(inputStream);
    State readState = new State();
    readState.readFields(dataInput);
    inputStream.close();

    Assert.assertEquals(state, readState);

    assertInterned(state.getProperties(), false);
    assertInterned(readState.getProperties(), true);
  }

  public static void assertInterned(Map<Object, Object> map, boolean interned) {
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      if (entry.getKey() instanceof String) {
        Assert.assertEquals(isInterned((String) entry.getKey()), interned);
      }
      if (entry.getValue() instanceof String) {
        Assert.assertEquals(isInterned((String) entry.getValue()), interned);
      }
    }
  }

  public static boolean isInterned(String str) {
    return str == str.intern();
  }
}
