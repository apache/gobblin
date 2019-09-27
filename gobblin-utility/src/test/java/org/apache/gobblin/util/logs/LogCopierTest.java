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
package org.apache.gobblin.util.logs;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;


public class LogCopierTest {
  private FileSystem srcFs;
  private FileSystem destFs;
  private Path srcLogDir;
  private Path destLogDir;
  private LogCopier logCopier;
  private String testString = "Test Log line.";

  @BeforeClass
  public void setUp() throws IOException {
    this.srcFs = FileSystem.getLocal(new Configuration());
    this.destFs = FileSystem.getLocal(new Configuration());
    this.srcLogDir = new Path("/tmp/LogCopierTest/srcLogDir");
    if (!srcFs.exists(srcLogDir)) {
      srcFs.mkdirs(srcLogDir);
    }
    this.destLogDir = new Path("/tmp/LogCopierTest/destLogDir");
    if (!destFs.exists(destLogDir)) {
      destFs.mkdirs(destLogDir);
    }
    this.logCopier = LogCopier.newBuilder()
        .readFrom(this.srcLogDir)
        .useCurrentLogFileName("testLog")
        .useSrcFileSystem(srcFs)
        .useDestFileSystem(destFs)
        .writeTo(destLogDir)
        .acceptsLogFileExtensions(ImmutableSet.of()).build();
  }

  private void createFileHelper(FileSystem fs, Path path) throws IOException {
    FSDataOutputStream fsDataOutputStream = fs.create(path);
    fsDataOutputStream.writeBytes(testString);
    fsDataOutputStream.close();
  }

  @Test
  public void testCheckSrcLogFiles() throws Exception {
    //Create test log files on the srcFs
    createFileHelper(srcFs, new Path(srcLogDir, "testLog.log"));
    createFileHelper(srcFs, new Path(srcLogDir, "testLog.log.1"));
    //Run one iteration of the LogCopier. 1st rolled log file should be copied over.
    this.logCopier.runOneIteration();
    FileStatus[] destLogFiles = this.destFs.listStatus(destLogDir);
    Assert.assertEquals(destLogFiles.length, 1);
    Assert.assertEquals(destLogFiles[0].getLen(), testString.length() + 1);

    createFileHelper(srcFs, new Path(srcLogDir, "testLog.log.2"));
    //Run the 2nd iteration of LogCopier. 2nd rolled log file should be copied over.
    this.logCopier.runOneIteration();
    destLogFiles = this.destFs.listStatus(destLogDir);
    Assert.assertEquals(destLogFiles.length, 2);
    Assert.assertEquals(destLogFiles[0].getLen(), testString.length() + 1);
    Assert.assertEquals(destLogFiles[1].getLen(), testString.length() + 1);

    //Shutdown the LogCopier. The current log file (i.e. testLog.log) should be copied over.
    this.logCopier.shutDown();
    destLogFiles = this.destFs.listStatus(destLogDir);
    Assert.assertEquals(destLogFiles.length, 3);
    Assert.assertEquals(destLogFiles[0].getLen(), testString.length() + 1);
    Assert.assertEquals(destLogFiles[1].getLen(), testString.length() + 1);
    Assert.assertEquals(destLogFiles[2].getLen(), testString.length() + 1);
  }

  @Test (dependsOnMethods = "testCheckSrcLogFiles")
  public void testPruneCopiedFileNames() throws Exception {
    Set<String> srcLogFileNames = Sets.newHashSet("testLog.log");

    Assert.assertEquals(logCopier.getCopiedFileNames().size(), 3);
    logCopier.pruneCopiedFileNames(srcLogFileNames);
    Assert.assertEquals(logCopier.getCopiedFileNames().size(), 1);
  }

  @AfterClass
  public void cleanUp() throws IOException {
    this.srcFs.delete(srcLogDir, true);
    this.destFs.delete(destLogDir, true);
  }
}