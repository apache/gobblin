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

package gobblin.util;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import gobblin.configuration.State;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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

  @Test(groups = { "performance" })
  public void testRenamePerformance() throws Exception {

    FileSystem fs = Mockito.mock(FileSystem.class);

    Path sourcePath = new Path("/source");
    Path s1 = new Path(sourcePath, "d1");

    FileStatus[] sourceStatuses = new FileStatus[10000];
    FileStatus[] targetStatuses = new FileStatus[1000];

    for (int i = 0; i < sourceStatuses.length; i++) {
      sourceStatuses[i] = getFileStatus(new Path(s1, "path" + i), false);
    }
    for (int i = 0; i < targetStatuses.length; i++) {
      targetStatuses[i] = getFileStatus(new Path(s1, "path" + i), false);
    }

    Mockito.when(fs.getUri()).thenReturn(new URI("file:///"));
    Mockito.when(fs.getFileStatus(sourcePath)).thenAnswer(getDelayedAnswer(getFileStatus(sourcePath, true)));
    Mockito.when(fs.exists(sourcePath)).thenAnswer(getDelayedAnswer(true));
    Mockito.when(fs.listStatus(sourcePath)).thenAnswer(getDelayedAnswer(new FileStatus[]{getFileStatus(s1, true)}));
    Mockito.when(fs.exists(s1)).thenAnswer(getDelayedAnswer(true));
    Mockito.when(fs.listStatus(s1)).thenAnswer(getDelayedAnswer(sourceStatuses));

    Path target = new Path("/target");
    Path s1Target = new Path(target, "d1");
    Mockito.when(fs.exists(target)).thenAnswer(getDelayedAnswer(true));
    Mockito.when(fs.exists(s1Target)).thenAnswer(getDelayedAnswer(true));

    Mockito.when(fs.mkdirs(Mockito.any(Path.class))).thenAnswer(getDelayedAnswer(true));
    Mockito.when(fs.rename(Mockito.any(Path.class), Mockito.any(Path.class))).thenAnswer(getDelayedAnswer(true));

    HadoopUtils.renameRecursively(fs, sourcePath, target);
  }

  private <T> Answer<T> getDelayedAnswer(final T result) throws Exception {
    return new Answer<T>() {
      @Override
      public T answer(InvocationOnMock invocation)
          throws Throwable {
        Thread.sleep(50);
        return result;
      }
    };
  }

  private FileStatus getFileStatus(Path path, boolean dir) {
    return new FileStatus(1, dir, 1, 1, 1, path);
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

  @Test
  public void testSanitizePath() throws Exception {
    Assert.assertEquals(HadoopUtils.sanitizePath("/A:B/::C:::D\\", "abc"), "/AabcB/abcabcCabcabcabcDabc");
    Assert.assertEquals(HadoopUtils.sanitizePath(":\\:\\/", ""), "/");
    try {
      HadoopUtils.sanitizePath("/A:B/::C:::D\\", "a:b");
      throw new RuntimeException();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("substitute contains illegal characters"));
    }
  }
}
