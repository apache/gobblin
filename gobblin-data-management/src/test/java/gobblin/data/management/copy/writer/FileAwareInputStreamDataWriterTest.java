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
package gobblin.data.management.copy.writer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableDatasetMetadata;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.CopyableFileUtils;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.data.management.copy.OwnerAndPermission;
import gobblin.data.management.copy.TestCopyableDataset;
import gobblin.util.io.StreamUtils;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FileAwareInputStreamDataWriterTest {

  FileSystem fs;
  Path testTempPath;

  @BeforeClass
  public void setup() throws Exception {
    fs = FileSystem.getLocal(new Configuration());
    testTempPath = new Path(this.getClass().getClassLoader().getResource("").getFile(), "InputStreamDataWriterTest");
    fs.mkdirs(testTempPath);
  }

  @Test
  public void testWrite() throws Exception {
    String streamString = "testContents";

    WorkUnitState state = new WorkUnitState();
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(testTempPath, "staging").toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(testTempPath, "output").toString());
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, RandomStringUtils.randomAlphabetic(5));
    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")),
        new Path("/"));
    CopySource.serializeCopyableDataset(state, metadata);

    FileStatus status = fs.getFileStatus(testTempPath);

    FileAwareInputStreamDataWriter dataWriter = new FileAwareInputStreamDataWriter(state, 1, 0);

    OwnerAndPermission ownerAndPermission =
        new OwnerAndPermission(status.getOwner(), status.getGroup(), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

    CopyableFile cf = CopyableFileUtils.getTestCopyableFile(ownerAndPermission);

    FileAwareInputStream fileAwareInputStream = new FileAwareInputStream(cf, StreamUtils.convertStream(IOUtils.toInputStream(streamString)));
    dataWriter.write(fileAwareInputStream);
    dataWriter.commit();
    Path writtenFilePath = new Path(new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
        cf.getDatasetAndPartition(metadata).identifier()), cf.getDestination());
    Assert.assertEquals(IOUtils.toString(new FileInputStream(writtenFilePath.toString())), streamString);
  }

  @AfterClass
  public void cleanup() {
    try {
      fs.delete(testTempPath, true);
    } catch (IOException e) {
      // ignore
    }
  }
}
