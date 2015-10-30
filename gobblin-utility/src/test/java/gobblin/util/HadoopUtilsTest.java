/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import gobblin.configuration.State;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = { "gobblin.util" })
public class HadoopUtilsTest {

  private final Path hadoopUtilsTestDir = new Path(this.getClass().getClassLoader().getResource("").getFile(), "HadoopUtilsTestDir");

  @BeforeClass
  public void setup() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.mkdirs(hadoopUtilsTestDir);
  }

  @AfterClass
  public void cleanup() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.delete(hadoopUtilsTestDir, true);
  }

  @Test
  public void fsShortSerializationTest() {
    State state = new State();
    short mode = 420;
    FsPermission perms = new FsPermission(mode);

    HadoopUtils.serializeWriterFilePermissions(state, 0, 0, perms);
    FsPermission deserializedPerms = HadoopUtils.deserializeWriterFilePermissions(state, 0, 0);
    Assert.assertEquals(mode, deserializedPerms.toShort());
  }

  @Test
  public void fsOctalSerializationTest() {
    State state = new State();
    String mode = "0755";

    HadoopUtils.setWriterFileOctalPermissions(state, 0, 0, mode);
    FsPermission deserializedPerms = HadoopUtils.deserializeWriterFilePermissions(state, 0, 0);
    Assert.assertEquals(Integer.parseInt(mode, 8), deserializedPerms.toShort());
  }

  @Test
  public void testRenameRecursively() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.mkdirs(new Path(hadoopUtilsTestDir, "testRename/a/b/c"));

    fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c"));
    fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c/e"));
    fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c/t1.txt"));
    fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c/e/t2.txt"));

    HadoopUtils.renameRecursively(fs, new Path(hadoopUtilsTestDir, "testRenameStaging"), new Path(hadoopUtilsTestDir, "testRename"));

    Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testRename/a/b/c/t1.txt")));
    Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testRename/a/b/c/e/t2.txt")));

  }

  @Test
  public void testSafeRenameRecursively() throws Exception {
    final FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.mkdirs(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c"));

    fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging1/a/b/c"));
    fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging1/a/b/c/e"));
    fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging1/a/b/c/t1.txt"));
    fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging1/a/b/c/e/t2.txt"));

    fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging2/a/b/c"));
    fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging2/a/b/c/e"));
    fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging2/a/b/c/t3.txt"));
    fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging2/a/b/c/e/t4.txt"));

    ExecutorService executorService = Executors.newFixedThreadPool(2);

    executorService.submit(new Runnable() {

      @Override
      public void run() {
        try {
          HadoopUtils.renameRecursively(fs, new Path(hadoopUtilsTestDir, "testRenameStaging1"), new Path(
              hadoopUtilsTestDir, "testSafeRename"));
        } catch (IOException e) {
          Assert.fail("Failed to rename", e);
        }
      }
    });

    executorService.submit(new Runnable() {

      @Override
      public void run() {
        try {
          HadoopUtils.safeRenameRecursively(fs, new Path(hadoopUtilsTestDir, "testRenameStaging2"), new Path(
              hadoopUtilsTestDir, "testSafeRename"));
        } catch (IOException e) {
          Assert.fail("Failed to rename", e);
        }
      }
    });

    executorService.awaitTermination(2, TimeUnit.SECONDS);
    executorService.shutdownNow();

    Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/t1.txt")));
    Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/t3.txt")));
    Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/e/t2.txt")));
    Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/e/t4.txt")));

  }
}
