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

package gobblin.data.management.copy;

import junit.framework.Assert;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Splitter;

import gobblin.data.management.dataset.DummyDataset;
import gobblin.data.management.partition.FileSet;
import gobblin.source.workunit.WorkUnit;


public class ConcurrentBoundedWorkUnitListTest {

  private static final String ORIGIN_PATH = "/path/origin";
  private static final String TARGET_PATH = "/path/target";

  @Test
  public void testBoundedAdd() throws IOException {

    ConcurrentBoundedWorkUnitList list = new ConcurrentBoundedWorkUnitList(10,
        new AllEqualComparator<FileSet<CopyableFile>>());

    Assert.assertTrue(addFiles(list, "fs", 6));
    Assert.assertFalse(list.hasRejectedFileSet());
    Assert.assertFalse(addFiles(list, "fs", 5));
    Assert.assertTrue(list.hasRejectedFileSet());
    Assert.assertTrue(addFiles(list, "fs", 4));
    Assert.assertFalse(addFiles(list, "fs", 1));

  }

  @Test
  public void testPriority() throws IOException {

    ConcurrentBoundedWorkUnitList list = new ConcurrentBoundedWorkUnitList(10,
        new NameComparator());

    // Fill container
    Assert.assertTrue(addFiles(list, "z-1", 10));
    Assert.assertEquals(list.getWorkUnits().size(), 10);
    // Reject because same priority
    Assert.assertFalse(addFiles(list, "z-2", 5));
    // Higher priority, so accept new work units
    Assert.assertTrue(addFiles(list, "y-1", 5));
    Assert.assertEquals(list.getWorkUnits().size(), 5);

    // Lower priority fits, so accept new work units
    Assert.assertTrue(addFiles(list, "z-3", 2));
    Assert.assertEquals(list.getWorkUnits().size(), 7);

    // Lower priority fits, so accept new work units
    Assert.assertTrue(addFiles(list, "z-4", 2));
    Assert.assertEquals(list.getWorkUnits().size(), 9);

    // Higher priority, evict lowest priority
    Assert.assertTrue(addFiles(list, "y-2", 4));
    Assert.assertEquals(list.getWorkUnits().size(), 9);

    // Highest priority, evict lowest priority
    Assert.assertTrue(addFiles(list, "x-1", 4));
    Assert.assertEquals(list.getWorkUnits().size(), 9);

  }

  // Compares names of partitions, but only parts of the name before first "-" character. For example, "a-foo" = "a-bar",
  // and "a-foo" < "b-bar".
  private class NameComparator implements Comparator<FileSet<CopyableFile>> {
    @Override public int compare(FileSet<CopyableFile> o1, FileSet<CopyableFile> o2) {
      String o1Token = Splitter.on("-").limit(1).split(o1.getName()).iterator().next();
      String o2Token = Splitter.on("-").limit(1).split(o2.getName()).iterator().next();

      return o1Token.compareTo(o2Token);
    }

  }

  public boolean addFiles(ConcurrentBoundedWorkUnitList list, String fileSetName, int fileNumber) throws IOException {
    FileSet.Builder<CopyableFile> partitionBuilder =
        new FileSet.Builder<>(fileSetName, new DummyDataset(new Path("/path")));
    List<WorkUnit> workUnits = Lists.newArrayList();

    for (int i = 0; i < fileNumber; i++) {
      CopyableFile cf = createCopyableFile(i);
      partitionBuilder.add(cf);
      WorkUnit workUnit = new WorkUnit();
      CopySource.serializeCopyableFile(workUnit, cf);
      workUnits.add(workUnit);
    }

    return list.addFileSet(partitionBuilder.build(), workUnits);
  }

  public CopyableFile createCopyableFile(int fileNumber) throws IOException {

    Path originPath = new Path(ORIGIN_PATH, fileNumber + ".file");
    FileStatus origin = new FileStatus(0, false, 0, 0, 0, originPath);
    Path targetPath = new Path(TARGET_PATH, fileNumber + ".file");

    return new CopyableFile(origin, targetPath, targetPath, new OwnerAndPermission(null, null, null),
        Lists.<OwnerAndPermission>newArrayList(), null, PreserveAttributes.fromMnemonicString(""), "", 0, 0);

  }

}
