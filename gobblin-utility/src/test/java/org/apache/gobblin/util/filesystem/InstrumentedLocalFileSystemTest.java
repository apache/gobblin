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

package org.apache.gobblin.util.filesystem;

import java.io.File;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import org.apache.gobblin.util.DecoratorUtils;


public class InstrumentedLocalFileSystemTest {

  @Test
  public void testFromInstrumentedScheme() throws Exception {

    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    FileSystem fs = FileSystem.get(new URI(InstrumentedLocalFileSystem.SCHEME + ":///"), new Configuration());

    Assert.assertTrue(fs instanceof InstrumentedLocalFileSystem);
    Assert.assertTrue(DecoratorUtils.resolveUnderlyingObject(fs) instanceof LocalFileSystem);
    Assert.assertEquals(fs.getFileStatus(new Path("/tmp")).getPath(), new Path("instrumented-file:///tmp"));
    Assert.assertEquals(fs.getUri().getScheme(), "instrumented-file");

    Path basePath = new Path(tmpDir.getAbsolutePath());

    Assert.assertTrue(fs.exists(basePath));

    Path file = new Path(basePath, "file");

    Assert.assertFalse(fs.exists(file));
    fs.create(new Path(basePath, "file"));
    Assert.assertTrue(fs.exists(file));

    Assert.assertEquals(fs.getFileStatus(file).getLen(), 0);
    Assert.assertEquals(fs.listStatus(basePath).length, 1);

    fs.delete(file, false);
    Assert.assertFalse(fs.exists(file));
  }

  @Test
  public void testFromConfigurationOverride() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set("fs.file.impl", InstrumentedLocalFileSystem.class.getName());
    FileSystem fs = FileSystem.newInstance(new URI("file:///"), configuration);
    Assert.assertTrue(fs instanceof InstrumentedLocalFileSystem);
    Assert.assertTrue(DecoratorUtils.resolveUnderlyingObject(fs) instanceof LocalFileSystem);
    Assert.assertEquals(fs.getFileStatus(new Path("/tmp")).getPath(), new Path("file:///tmp"));
    Assert.assertEquals(fs.getUri().getScheme(), "file");
  }

}
