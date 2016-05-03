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

package gobblin.data.management.copy.hive;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.metrics.event.MultiTimingEvent;


public class HiveCopyEntityHelperTest {

  @Test public void testResolvePath() throws Exception {
    Assert.assertEquals(HiveCopyEntityHelper.resolvePath("/data/$DB/$TABLE", "db", "table"), new Path("/data/db/table"));
    Assert.assertEquals(HiveCopyEntityHelper.resolvePath("/data/$TABLE", "db", "table"), new Path("/data/table"));
    Assert.assertEquals(HiveCopyEntityHelper.resolvePath("/data", "db", "table"), new Path("/data/table"));

  }

  @Test
  public void testFullPathDiff() throws Exception {

    List<FileStatus> sourceFileStatuses = Lists.newArrayList();
    List<FileStatus> desiredTargetStatuses = Lists.newArrayList();

    final Path sourceRoot = new Path("/source");
    final Path targetRoot = new Path("/target");

    // already exists in target
    Path path1 = new Path("path1");
    Path sourcePath1 = new Path(sourceRoot, path1);
    Path targetPath1 = new Path(targetRoot, path1);
    sourceFileStatuses.add(getFileStatus(sourcePath1, 0, 0));
    desiredTargetStatuses.add(getFileStatus(targetPath1, 0, 10));

    // not exists in target
    Path path2 = new Path("path2");
    Path sourcePath2 = new Path(sourceRoot, path2);
    Path targetPath2 = new Path(targetRoot, path2);
    sourceFileStatuses.add(getFileStatus(sourcePath2, 0, 0));

    // exists in target, different length
    Path path3 = new Path("path3");
    Path sourcePath3 = new Path(sourceRoot, path3);
    Path targetPath3 = new Path(targetRoot, path3);
    sourceFileStatuses.add(getFileStatus(sourcePath3, 0, 0));
    desiredTargetStatuses.add(getFileStatus(targetPath3, 10, 0));

    // exists in target, newer modtime
    Path path4 = new Path("path4");
    Path sourcePath4 = new Path(sourceRoot, path4);
    Path targetPath4 = new Path(targetRoot, path4);
    sourceFileStatuses.add(getFileStatus(sourcePath4, 0, 10));
    desiredTargetStatuses.add(getFileStatus(targetPath4, 0, 0));

    // only on target, expect delete
    Path path5 = new Path("path5");
    Path sourcePath5 = new Path(sourceRoot, path5);
    Path targetPath5 = new Path(targetRoot, path5);
    desiredTargetStatuses.add(getFileStatus(targetPath5, 0, 10));

    Map<Path, FileStatus> sourceMap = Maps.newHashMap();
    for(FileStatus status : sourceFileStatuses) {
      sourceMap.put(status.getPath(), status);
    }
    TestLocationDescriptor sourceLocation = new TestLocationDescriptor(sourceMap);

    Map<Path, FileStatus> targetDesiredMap = Maps.newHashMap();
    for(FileStatus status : desiredTargetStatuses) {
      targetDesiredMap.put(status.getPath(), status);
    }
    TestLocationDescriptor targetDesiredLocation = new TestLocationDescriptor(targetDesiredMap);

    TestLocationDescriptor existingTargetLocation = new TestLocationDescriptor(Maps.newHashMap(targetDesiredMap));


    MultiTimingEvent timer = Mockito.mock(MultiTimingEvent.class);
    HiveCopyEntityHelper helper = Mockito.mock(HiveCopyEntityHelper.class);
    Mockito.when(helper.getTargetPath(Mockito.any(Path.class), Mockito.any(FileSystem.class), Mockito.any(Optional.class), Mockito.anyBoolean())).then(
        new Answer<Path>() {
          @Override
          public Path answer(InvocationOnMock invocation)
              throws Throwable {
            Path path = (Path)invocation.getArguments()[0];
            return new Path(path.toString().replace(sourceRoot.toString(), targetRoot.toString()));
          }
        });

    HiveCopyEntityHelper.DiffPathSet diff =
        HiveCopyEntityHelper.fullPathDiff(sourceLocation, targetDesiredLocation, Optional.<HiveLocationDescriptor>of(existingTargetLocation),
        Optional.<Partition>absent(), timer, helper);

    Assert.assertEquals(diff.filesToCopy.size(), 3);
    Assert.assertFalse(containsPath(diff.filesToCopy, sourcePath1));
    Assert.assertTrue(containsPath(diff.filesToCopy, sourcePath2));
    Assert.assertTrue(containsPath(diff.filesToCopy, sourcePath3));
    Assert.assertTrue(containsPath(diff.filesToCopy, sourcePath4));
    Assert.assertFalse(containsPath(diff.filesToCopy, sourcePath5));

    Assert.assertEquals(diff.pathsToDelete.size(), 3);
    Assert.assertFalse(diff.pathsToDelete.contains(targetPath1));
    Assert.assertFalse(diff.pathsToDelete.contains(targetPath2));
    Assert.assertTrue(diff.pathsToDelete.contains(targetPath3));
    Assert.assertTrue(diff.pathsToDelete.contains(targetPath4));
    Assert.assertTrue(diff.pathsToDelete.contains(targetPath5));

  }

  private boolean containsPath(Collection<FileStatus> statuses, Path path) {
    for (FileStatus status : statuses) {
      if (status.getPath().equals(path)) {
        return true;
      }
    }
    return false;
  }

  private FileStatus getFileStatus(Path path, long len, long modtime) {
    return new FileStatus(len, false, 0, 0, modtime, path);
  }

  public class TestLocationDescriptor extends HiveLocationDescriptor {
    Map<Path, FileStatus> paths;

    public TestLocationDescriptor(Map<Path, FileStatus> paths) {
      super(null, null, null, null);
      this.paths = paths;
    }

    @Override
    public Map<Path, FileStatus> getPaths()
        throws IOException {
      return this.paths;
    }
  }

}
