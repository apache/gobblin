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

package gobblin.filesystem;

import java.net.URI;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InstrumentedHDFSFileSystemTest {

  /**
   * This test is disabled because it requires a local hdfs cluster at localhost:8020, which requires installation and setup.
   * Changes to {@link InstrumentedHDFSFileSystem} should be followed by a manual run of this tests.
   *
   * TODO: figure out how to fully automate this test.
   * @throws Exception
   */
  @Test(enabled = false)
  public void test() throws Exception {

    String uri = "instrumented-hdfs://localhost:8020";

    FileSystem fs = FileSystem.get(new URI(uri), new Configuration());

    String name = UUID.randomUUID().toString();
    fs.mkdirs(new Path("/tmp"));

    // Test absolute paths
    Path absolutePath = new Path("/tmp", name);
    Assert.assertFalse(fs.exists(absolutePath));
    fs.createNewFile(absolutePath);
    Assert.assertTrue(fs.exists(absolutePath));
    Assert.assertEquals(fs.getFileStatus(absolutePath).getLen(), 0);
    fs.delete(absolutePath, false);
    Assert.assertFalse(fs.exists(absolutePath));


    // Test fully qualified paths
    Path fqPath = new Path(uri + "/tmp", name);
    Assert.assertFalse(fs.exists(fqPath));
    fs.createNewFile(fqPath);
    Assert.assertTrue(fs.exists(fqPath));
    Assert.assertEquals(fs.getFileStatus(fqPath).getLen(), 0);
    fs.delete(fqPath, false);
    Assert.assertFalse(fs.exists(fqPath));
  }

}
