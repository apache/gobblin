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

package org.apache.gobblin.runtime.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class DistributedClasspathManagerTest {

  private Path baseDir;
  private FileSystem localFs;

  @BeforeClass
  private void setup()
      throws IOException {
    localFs = FileSystem.getLocal(new Configuration());
    baseDir = new Path("distributedClasspathManagerTest");
    if (localFs.exists(baseDir)) {
      localFs.delete(baseDir, true);
    }
  }

  @Test
  public void testAddToDistributedClasspath()
      throws Exception {

    Path testDir = new Path(baseDir, "addToDistributedClasspath");

    if (localFs.exists(testDir)) {
      localFs.delete(testDir, true);
    }

    Path jarDir0 = new Path(baseDir, "jarDir0");
    Path jarDir1 = new Path(baseDir, "jarDir1");

    // Create public jars
    List<Path> files = new ArrayList<>();
    files.add(localFs.makeQualified(new Path(jarDir0, "common.jar")));
    files.add(localFs.makeQualified(new Path(jarDir0, "public-01.jar")));
    files.add(localFs.makeQualified(new Path(jarDir1, "public-10.jar")));
    files.add(localFs.makeQualified(new Path(jarDir1, "private-SNAPSHOT-11.jar")));
    for (Path file : files) {
      createLocalFile(file);
    }

    Path rootPath = new Path(baseDir, "publicJobJars");

    // Map dest file system to local
    FileSystem fs = spy(FileSystem.getLocal(new Configuration()));

    String jarDirs = jarDir0.toString() + "/*," + jarDir1.toString() + "/*,";
    Path privateJarsDir = new Path(baseDir, "privateJobJars0");
    Job job = mock(Job.class);

    Set<Object> filesInClassPath = new HashSet<>();
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation)
          throws Throwable {
        Object[] args = invocation.getArguments();
        filesInClassPath.add(args[0]);
        return null;
      }
    }).when(job).addFileToClassPath(any());

    when(fs.getScheme()).thenReturn("file");
    DistributedClasspathManager classpathManager = new DistributedClasspathManager(fs, rootPath);
    classpathManager.addToDistributedClasspath(job, jarDirs, privateJarsDir, 1);

    // Case 1: Nothing to copy as file system is local
    Assert.assertEquals(filesInClassPath.size(), 0);

    // Case 2: All 4 files copied to hdfs
    when(fs.getScheme()).thenReturn("hdfs");
    classpathManager.addToDistributedClasspath(job, jarDirs, privateJarsDir, 1);
    Assert.assertEquals(filesInClassPath.size(), 4);
    // Check public files
    List<FileStatus> copiedPublicFiles = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      Path file = new Path(rootPath, files.get(i).getName());
      Assert.assertTrue(fs.exists(file));
      copiedPublicFiles.add(fs.getFileStatus(file));
    }
    // Check private files
    Assert.assertTrue(fs.exists(new Path(privateJarsDir, files.get(3).getName())));

    // Case 3: Only new files copied
    filesInClassPath.clear();
    List<Path> newFiles = new ArrayList<>();
    Path jarDir3 = new Path(baseDir, "jarDir3");
    jarDirs += jarDir3.toString() + "/*";

    newFiles.add(localFs.makeQualified(new Path(jarDir3, "common.jar")));
    newFiles.add(localFs.makeQualified(new Path(jarDir3, "private-SNAPSHOT-2.jar")));
    for (Path file : newFiles) {
      createLocalFile(file);
    }

    privateJarsDir = new Path(baseDir, "privateJobJars1");

    int[] trigger = new int[]{0};
    String failureMessage = "scheduled fail";
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation)
          throws Throwable {
        Object[] args = invocation.getArguments();
        if (trigger[0] == 0) {
          trigger[0]++;
          throw new IOException(failureMessage);
        }
        localFs.copyFromLocalFile((Path) args[0], (Path) args[1]);
        return null;
      }
    }).when(fs).copyFromLocalFile(any(), any());

    /// Failed with 1 attempts
    boolean scheduledFailed = false;
    try {
      classpathManager.addToDistributedClasspath(job, jarDirs, privateJarsDir, 1);
    } catch (IOException e) {
      if (e.getCause().getMessage().equals(failureMessage)) {
        scheduledFailed = true;
      }
    }
    Assert.assertTrue(scheduledFailed);

    // Rest trigger
    trigger[0] = 0;
    classpathManager.addToDistributedClasspath(job, jarDirs, privateJarsDir, 2);

    // Succeeded with 2 attempts
    Assert.assertEquals(filesInClassPath.size(), 5);
    // Check public files not copied
    for (int i = 0; i < 3; i++) {
      Path file = new Path(rootPath, files.get(i).getName());
      Assert.assertTrue(fs.exists(file));
      Assert.assertEquals(fs.getFileStatus(file), copiedPublicFiles.get(i));
    }
    // Check private files copied
    Assert.assertTrue(fs.exists(new Path(privateJarsDir, files.get(3).getName())));
    Assert.assertTrue(fs.exists(new Path(privateJarsDir, newFiles.get(1).getName())));
  }

  private void createLocalFile(Path file)
      throws Exception {
    FSDataOutputStream output = localFs.create(file);
    output.writeByte(1);
    output.close();
  }

  @AfterClass
  private void cleanup()
      throws IOException {
    if (localFs != null) {
      localFs.delete(baseDir, true);
    }
  }
}
