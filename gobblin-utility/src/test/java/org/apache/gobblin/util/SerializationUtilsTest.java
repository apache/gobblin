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

package gobblin.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.source.workunit.WorkUnit;


/**
 * Unit tests for {@link SerializationUtils}.
 *
 * @author Yinan Li
 */
@Test(groups = { "gobblin.util" })
public class SerializationUtilsTest {

  private FileSystem fs;
  private Path outputPath;

  @BeforeClass
  public void setUp() throws IOException {
    this.fs = FileSystem.getLocal(new Configuration());
    this.outputPath = new Path(SerializationUtilsTest.class.getSimpleName());
  }

  @Test
  public void testSerializeState() throws IOException {
    WorkUnit workUnit1 = WorkUnit.createEmpty();
    workUnit1.setProp("foo", "bar");
    workUnit1.setProp("a", 10);
    SerializationUtils.serializeState(this.fs, new Path(this.outputPath, "wu1"), workUnit1);

    WorkUnit workUnit2 = WorkUnit.createEmpty();
    workUnit2.setProp("foo", "baz");
    workUnit2.setProp("b", 20);
    SerializationUtils.serializeState(this.fs, new Path(this.outputPath, "wu2"), workUnit2);
  }

  @Test(dependsOnMethods = "testSerializeState")
  public void testDeserializeState() throws IOException {
    WorkUnit workUnit1 = WorkUnit.createEmpty();
    WorkUnit workUnit2 = WorkUnit.createEmpty();

    SerializationUtils.deserializeState(this.fs, new Path(this.outputPath, "wu1"), workUnit1);
    SerializationUtils.deserializeState(this.fs, new Path(this.outputPath, "wu2"), workUnit2);

    Assert.assertEquals(workUnit1.getPropertyNames().size(), 2);
    Assert.assertEquals(workUnit1.getProp("foo"), "bar");
    Assert.assertEquals(workUnit1.getPropAsInt("a"), 10);

    Assert.assertEquals(workUnit2.getPropertyNames().size(), 2);
    Assert.assertEquals(workUnit2.getProp("foo"), "baz");
    Assert.assertEquals(workUnit2.getPropAsInt("b"), 20);
  }

  @AfterClass
  public void tearDown() throws IOException {
    if (this.fs != null && this.outputPath != null) {
      this.fs.delete(this.outputPath, true);
    }
  }
}
