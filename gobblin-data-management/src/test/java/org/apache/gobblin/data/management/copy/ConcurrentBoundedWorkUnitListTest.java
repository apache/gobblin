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

package org.apache.gobblin.data.management.copy;


import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

import org.apache.gobblin.data.management.dataset.DummyDataset;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.source.workunit.WorkUnit;


public class ConcurrentBoundedWorkUnitListTest {

  private static final String ORIGIN_PATH = "/path/origin";
  private static final String TARGET_PATH = "/path/target";

  @Test
  public void testBoundedAdd() throws IOException {

    ConcurrentBoundedWorkUnitList list = new ConcurrentBoundedWorkUnitList(10,
        new AllEqualComparator<FileSet<CopyEntity>>(), 1);

    Assert.assertTrue(addFiles(list, "fs", 6));
    Assert.assertFalse(list.hasRejectedFileSet());
    Assert.assertFalse(addFiles(list, "fs", 5));
    Assert.assertTrue(list.hasRejectedFileSet());
    Assert.assertTrue(addFiles(list, "fs", 4));
    Assert.assertFalse(addFiles(list, "fs", 1));

  }

  @Test
  public void testNonStrictBoundAdd() throws IOException {

    ConcurrentBoundedWorkUnitList list = new ConcurrentBoundedWorkUnitList(10,
        new AllEqualComparator<FileSet<CopyEntity>>(), 2.0);

    Assert.assertTrue(addFiles(list, "fs", 6));
    Assert.assertFalse(list.hasRejectedFileSet());
    Assert.assertFalse(list.isFull());
    Assert.assertTrue(addFiles(list, "fs", 5));
    Assert.assertFalse(list.hasRejectedFileSet());
    Assert.assertTrue(list.isFull());
    Assert.assertTrue(addFiles(list, "fs", 4));
    Assert.assertFalse(list.hasRejectedFileSet());
    Assert.assertFalse(addFiles(list, "fs", 6));
    Assert.assertTrue(list.hasRejectedFileSet());
    Assert.assertTrue(list.isFull());

  }

  @Test
  public void testPriority() throws IOException {

    ConcurrentBoundedWorkUnitList list = new ConcurrentBoundedWorkUnitList(10,
        new NameComparator(), 1);

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
  private class NameComparator implements Comparator<FileSet<CopyEntity>> {
    @Override public int compare(FileSet<CopyEntity> o1, FileSet<CopyEntity> o2) {
      String o1Token = Splitter.on("-").limit(1).split(o1.getName()).iterator().next();
      String o2Token = Splitter.on("-").limit(1).split(o2.getName()).iterator().next();

      return o1Token.compareTo(o2Token);
    }

  }

  public boolean addFiles(ConcurrentBoundedWorkUnitList list, String fileSetName, int fileNumber) throws IOException {
    FileSet.Builder<CopyEntity> partitionBuilder =
        new FileSet.Builder<>(fileSetName, new DummyDataset(new Path("/path")));
    List<WorkUnit> workUnits = Lists.newArrayList();

    for (int i = 0; i < fileNumber; i++) {
      CopyEntity cf = createCopyableFile(i);
      partitionBuilder.add(cf);
      WorkUnit workUnit = new WorkUnit();
      CopySource.serializeCopyEntity(workUnit, cf);
      workUnits.add(workUnit);
    }

    return list.addFileSet(partitionBuilder.build(), workUnits);
  }

  public CopyEntity createCopyableFile(int fileNumber) throws IOException {

    Path originPath = new Path(ORIGIN_PATH, fileNumber + ".file");
    FileStatus origin = new FileStatus(0, false, 0, 0, 0, originPath);
    Path targetPath = new Path(TARGET_PATH, fileNumber + ".file");

    return new CopyableFile(origin, targetPath, new OwnerAndPermission(null, null, null),
        Lists.<OwnerAndPermission>newArrayList(), null, PreserveAttributes.fromMnemonicString(""), "", 0, 0, Maps
        .<String, String>newHashMap(), "");

  }

}
