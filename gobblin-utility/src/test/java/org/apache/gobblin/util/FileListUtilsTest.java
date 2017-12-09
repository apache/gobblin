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

package org.apache.gobblin.util;

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
 * Unit tests for the job configuration file monitor in {@link org.apache.gobblin.util.FileListUtils}.
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

  public void testListAllFiles () throws IOException {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    Path baseDir = new Path(FILE_UTILS_TEST_DIR, "listAllFiles");
    System.out.println (baseDir);
    try {
      if (localFs.exists(baseDir)) {
        localFs.delete(baseDir, true);
      }
      localFs.mkdirs(baseDir);

      // Empty root directory
      List<FileStatus> testFiles = FileListUtils.listFilesRecursively(localFs, baseDir, FileListUtils.NO_OP_PATH_FILTER);
      Assert.assertTrue(testFiles.size() == 0);

      // With two avro files (1.avro, 2.avro)
      Path file1 = new Path(baseDir, "1.avro");
      localFs.create(file1);
      Path file2 = new Path(baseDir, "2.avro");
      localFs.create(file2);
      testFiles = FileListUtils.listFilesRecursively(localFs, baseDir, FileListUtils.NO_OP_PATH_FILTER);
      Assert.assertTrue(testFiles.size() == 2);

      // With an avro schema file (part.avsc)
      Path avsc = new Path(baseDir, "part.avsc");
      localFs.create(avsc);
      testFiles = FileListUtils.listFilesRecursively(localFs, baseDir, FileListUtils.NO_OP_PATH_FILTER);
      Assert.assertTrue(testFiles.size() == 3);
      testFiles = FileListUtils.listFilesRecursively(localFs, baseDir, (path)->path.getName().endsWith(".avro"));
      Assert.assertTrue(testFiles.size() == 2);

      // A complicated hierarchy
      // baseDir ____ 1.avro
      //        |____ 2.avro
      //        |____ part.avsc
      //        |____ subDir ____ 3.avro
      //                    |____ subDir2 ____ 4.avro
      //                                 |____ part2.avsc
      Path subDir = new Path(baseDir, "subDir");
      localFs.mkdirs(subDir);
      Path file3 = new Path(subDir, "3.avro");
      localFs.create(file3);
      Path subDir2 = new Path(subDir, "subDir2");
      localFs.mkdirs(subDir2);
      Path file4 = new Path(subDir2, "4.avro");
      localFs.create(file4);
      Path avsc2 = new Path(subDir2, "part2.avsc");
      localFs.create(avsc2);

      testFiles = FileListUtils.listFilesRecursively(localFs, baseDir, (path)->path.getName().endsWith(".avro"));
      Assert.assertTrue(testFiles.size() == 4);
      testFiles = FileListUtils.listFilesRecursively(localFs, baseDir, FileListUtils.NO_OP_PATH_FILTER);
      Assert.assertTrue(testFiles.size() == 6);
    } finally {
      localFs.delete(baseDir, true);
    }
  }

  public void testListFilesToCopyAtPath() throws IOException {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    Path baseDir = new Path(FILE_UTILS_TEST_DIR, "fileListTestDir4");
    try {
      if (localFs.exists(baseDir)) {
        localFs.delete(baseDir, true);
      }
      localFs.mkdirs(baseDir);

      // Empty root directory
      List<FileStatus> testFiles = FileListUtils.listFilesToCopyAtPath(localFs, baseDir, FileListUtils.NO_OP_PATH_FILTER, true);
      Assert.assertEquals(testFiles.size(), 1);
      Assert.assertEquals(testFiles.get(0).getPath().getName(), baseDir.getName());

      // With an empty sub directory
      Path subDir = new Path(baseDir, "subDir");
      localFs.mkdirs(subDir);
      testFiles = FileListUtils.listFilesToCopyAtPath(localFs, baseDir, FileListUtils.NO_OP_PATH_FILTER, true);
      Assert.assertEquals(testFiles.size(), 1);
      Assert.assertEquals(testFiles.get(0).getPath().getName(), subDir.getName());

      // Disable include empty directories
      testFiles = FileListUtils.listFilesToCopyAtPath(localFs, baseDir, FileListUtils.NO_OP_PATH_FILTER, false);
      Assert.assertEquals(testFiles.size(), 0);

      // With file subDir/tes1
      Path test1Path = new Path(subDir, TEST_FILE_NAME1);
      localFs.create(test1Path);
      testFiles = FileListUtils.listFilesToCopyAtPath(localFs, baseDir, FileListUtils.NO_OP_PATH_FILTER, true);
      Assert.assertEquals(testFiles.size(), 1);
      Assert.assertEquals(testFiles.get(0).getPath().getName(), test1Path.getName());

      // With file subDir/test2
      Path test2Path = new Path(subDir, TEST_FILE_NAME2);
      localFs.create(test2Path);
      testFiles = FileListUtils.listFilesToCopyAtPath(localFs, baseDir, FileListUtils.NO_OP_PATH_FILTER, true);
      Assert.assertEquals(testFiles.size(), 2);
      Set<String> fileNames = Sets.newHashSet();
      for (FileStatus testFileStatus : testFiles) {
        fileNames.add(testFileStatus.getPath().getName());
      }
      Assert.assertTrue(fileNames.contains(TEST_FILE_NAME1) && fileNames.contains(TEST_FILE_NAME2));
    } finally {
      localFs.delete(baseDir, true);
    }
  }

}
