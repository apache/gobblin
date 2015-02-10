/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.configuration;

import gobblin.source.workunit.Extract;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.source.workunit.WorkUnit;


public class WorkUnitStateTest {

  @Test
  public void WorkUnitState()
      throws IOException {
    SourceState sourceState = new SourceState();

    Extract extract = sourceState.createExtract(Extract.TableType.APPEND_ONLY, "test", "test");

    WorkUnit wu = sourceState.createWorkUnit(extract);

    WorkUnitState state = new WorkUnitState(wu);

    Assert.assertEquals(state.getProp("string", "some string"), "some string");
    Assert.assertEquals(state.getPropAsList("list", "item1,item2").get(0), "item1");
    Assert.assertEquals(state.getPropAsList("list", "item1,item2").get(1), "item2");
    Assert.assertEquals(state.getPropAsLong("long", Long.MAX_VALUE), Long.MAX_VALUE);
    Assert.assertEquals(state.getPropAsInt("int", Integer.MAX_VALUE), Integer.MAX_VALUE);
    Assert.assertEquals(state.getPropAsDouble("double", Double.MAX_VALUE), Double.MAX_VALUE);
    Assert.assertEquals(state.getPropAsBoolean("boolean", true), true);

    wu.setProp("string", "some string");
    wu.setProp("list", "item1,item2");
    wu.setProp("long", Long.MAX_VALUE);
    wu.setProp("int", Integer.MAX_VALUE);
    wu.setProp("double", Double.MAX_VALUE);
    wu.setProp("boolean", true);

    state = new WorkUnitState(wu);

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

    state = new WorkUnitState();

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

    WorkUnitState state2 = new WorkUnitState();
    state2.addAll(state);

    Assert.assertEquals(state2.getProp("string"), "some other string");
    Assert.assertEquals(state2.getPropAsList("list").get(0), "item3");
    Assert.assertEquals(state2.getPropAsList("list").get(1), "item4");
    Assert.assertEquals(state2.getPropAsLong("long"), Long.MIN_VALUE);
    Assert.assertEquals(state2.getPropAsInt("int"), Integer.MIN_VALUE);
    Assert.assertEquals(state2.getPropAsDouble("double"), Double.MIN_VALUE);
    Assert.assertEquals(state2.getPropAsBoolean("boolean"), false);
  }
}
