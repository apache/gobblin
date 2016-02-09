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

package gobblin.data.management.partition;

import lombok.Data;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import gobblin.data.management.dataset.DummyDataset;


public class FileSetTest {

  @Data
  private class TestFile implements File {
    private final FileStatus fileStatus;
    @Override public FileStatus getFileStatus() {
      return this.fileStatus;
    }
  }

  @Test public void testPartitionBuilder() throws Exception {

    String file1 = "file1";
    String file2 = "file2";

    FileSet<TestFile> fileSet = new FileSet.Builder<TestFile>("test", new DummyDataset(new Path("/path"))).
        add(new TestFile(createFileStatus(file1))).
        add(Lists.newArrayList(new TestFile(createFileStatus(file2)))).
        build();

    Assert.assertEquals(fileSet.getFiles().size(), 2);
    Assert.assertEquals(fileSet.getName(), "test");
    Assert.assertEquals(fileSet.getFiles().get(0).getFileStatus().getPath().toString(), file1);
    Assert.assertEquals(fileSet.getFiles().get(1).getFileStatus().getPath().toString(), file2);

  }

  private static FileStatus createFileStatus(String path) {
    return new FileStatus(0, false, 0, 0, 0, new Path(path));
  }
}
