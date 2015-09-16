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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;


/**
 * Unit tests for the job configuration file monitor in {@link gobblin.util.FileListUtils}.
 */
@Test(groups = { "gobblin.util" })
public class FileListUtilsTest {

  private static final String FILE_UTILS_TEST_DIR = "gobblin-utility/src/test/resources/";
  private static final String TEST_FILE_NAME1 = "test1";
  private static final String TEST_FILE_NAME2 = "test2";

  @Test
  public void testListFilesRecursively() throws IOException {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    Path baseDir = new Path(FILE_UTILS_TEST_DIR, "fileListTestDir1");
    try {
      if (localFs.exists(baseDir)) {
        localFs.delete(baseDir, true);
      }
      localFs.mkdirs(baseDir);
      localFs.create(new Path(baseDir, TEST_FILE_NAME1));
      Path subDir = new Path(baseDir, "subDir");
      localFs.mkdirs(subDir);
      localFs.create(new Path(subDir, TEST_FILE_NAME2));
      List<FileStatus> testFiles = FileListUtils.listFilesRecursively(localFs, baseDir);

      Assert.assertEquals(2, testFiles.size());

      Set<String> fileNames = Sets.newHashSet();
      for (FileStatus testFileStatus : testFiles) {
        fileNames.add(testFileStatus.getPath().getName());
      }
      Assert.assertTrue(fileNames.contains(TEST_FILE_NAME1) && fileNames.contains(TEST_FILE_NAME2));
    } finally {
      localFs.delete(baseDir, true);
    }
  }

  @Test
  public void testListPathsRecursively() throws IOException {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    Path baseDir = new Path(FILE_UTILS_TEST_DIR, "fileListTestDir2");
    try {
      if (localFs.exists(baseDir)) {
        localFs.delete(baseDir, true);
      }
      localFs.mkdirs(baseDir);
      localFs.create(new Path(baseDir, TEST_FILE_NAME1));
      Path subDir = new Path(baseDir, "subDir");
      localFs.mkdirs(subDir);
      localFs.create(new Path(subDir, TEST_FILE_NAME2));
      List<FileStatus> testFiles = FileListUtils.listPathsRecursively(localFs, baseDir, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return true;
        }
      });
      Assert.assertEquals(4, testFiles.size());

      Set<String> fileNames = Sets.newHashSet();
      for (FileStatus testFileStatus : testFiles) {
        fileNames.add(testFileStatus.getPath().getName());
      }

      Set<String> expectedFileNames = Sets.newHashSet();
      expectedFileNames.add(baseDir.getName());
      expectedFileNames.add(subDir.getName());
      expectedFileNames.add(TEST_FILE_NAME1);
      expectedFileNames.add(TEST_FILE_NAME2);

      Assert.assertEquals(fileNames, expectedFileNames);
    } finally {
      localFs.delete(baseDir, true);
    }
  }

  @Test
  public void testListMostNestedPathRecursively() throws IOException {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    Path baseDir = new Path(FILE_UTILS_TEST_DIR, "fileListTestDir3");
    String emptyDir1 = "emptyDir1";
    String emptyDir2 = "emptyDir2";
    try {
      if (localFs.exists(baseDir)) {
        localFs.delete(baseDir, true);
      }
      localFs.mkdirs(baseDir);
      localFs.create(new Path(baseDir, TEST_FILE_NAME1));
      localFs.mkdirs(new Path(baseDir, emptyDir1));
      Path subDir = new Path(baseDir, "subDir");
      localFs.mkdirs(subDir);
      localFs.create(new Path(subDir, TEST_FILE_NAME2));
      localFs.mkdirs(new Path(subDir, emptyDir2));

      List<FileStatus> testFiles = FileListUtils.listMostNestedPathRecursively(localFs, baseDir);

      Assert.assertEquals(4, testFiles.size());

      Set<String> fileNames = Sets.newHashSet();
      for (FileStatus testFileStatus : testFiles) {
        fileNames.add(testFileStatus.getPath().getName());
      }

      Set<String> expectedFileNames = Sets.newHashSet();
      expectedFileNames.add(emptyDir1);
      expectedFileNames.add(emptyDir2);
      expectedFileNames.add(TEST_FILE_NAME1);
      expectedFileNames.add(TEST_FILE_NAME2);

      Assert.assertEquals(fileNames, expectedFileNames);
    } finally {
      localFs.delete(baseDir, true);
    }
  }

}
