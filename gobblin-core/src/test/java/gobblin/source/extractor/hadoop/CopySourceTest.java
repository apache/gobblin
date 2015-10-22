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

package gobblin.source.extractor.hadoop;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.OwnerAndPermission;
import gobblin.data.management.dataset.DatasetUtils;
import gobblin.source.extractor.hadoop.test.TestCopyableDataset;
import gobblin.source.extractor.hadoop.test.TestCopyableDatasetFinder;
import gobblin.source.extractor.hadoop.test.TestCopyablePartitionableDatasedFinder;
import gobblin.source.extractor.hadoop.test.TestCopyablePartitionableDataset;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ForkOperatorUtils;

import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Lists;


public class CopySourceTest {

  @Test public void testCopySource() throws Exception {

    SourceState state = new SourceState();

    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp(DatasetUtils.DATASET_PROFILE_CLASS_KEY, TestCopyableDatasetFinder.class.getCanonicalName());

    CopySource source = new CopySource();

    List<WorkUnit> workunits = source.getWorkunits(state);

    Assert.assertEquals(workunits.size(), TestCopyableDataset.FILE_COUNT);

    Extract extract = workunits.get(0).getExtract();

    for(WorkUnit workUnit : workunits) {
      WorkUnitState workUnitState = new WorkUnitState(workUnit);
      Assert.assertTrue(CopySource.getOriginPath(workUnitState).toString().startsWith(TestCopyableDataset.ORIGIN_PREFIX));
      Assert.assertTrue(workUnitState.getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, 0, 0)).
          startsWith(TestCopyableDataset.DESTINATION_PREFIX));
      Assert.assertEquals(CopySource.getCopyableFile(workUnitState).getDestinationOwnerAndPermission(),
          TestCopyableDataset.OWNER_AND_PERMISSION);
      Assert.assertEquals(workUnit.getExtract(), extract);
    }

  }

  @Test public void testPartitionableDataset() throws Exception {

    SourceState state = new SourceState();

    state.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///");
    state.setProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, "file:///");
    state.setProp(DatasetUtils.DATASET_PROFILE_CLASS_KEY, TestCopyablePartitionableDatasedFinder.class.getCanonicalName());

    CopySource source = new CopySource();

    List<WorkUnit> workunits = source.getWorkunits(state);

    Assert.assertEquals(workunits.size(), TestCopyableDataset.FILE_COUNT);

    Extract extractAbove = null;
    Extract extractBelow = null;

    for(WorkUnit workUnit : workunits) {
      WorkUnitState workUnitState = new WorkUnitState(workUnit);
      Assert.assertTrue(CopySource.getOriginPath(workUnitState).toString().startsWith(TestCopyableDataset.ORIGIN_PREFIX));
      Assert.assertTrue(workUnitState.getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, 0, 0)).
          startsWith(TestCopyableDataset.DESTINATION_PREFIX));
      Assert.assertEquals(CopySource.getCopyableFile(workUnitState).getDestinationOwnerAndPermission(),
          TestCopyableDataset.OWNER_AND_PERMISSION);
      if(Integer.parseInt(CopySource.getOriginPath(workUnitState).getName()) <
          TestCopyablePartitionableDataset.THRESHOLD) {
        // should be in extractBelow
        if(extractBelow == null) {
          extractBelow = workUnit.getExtract();
        }
        Assert.assertEquals(workUnit.getExtract(), extractBelow);
      } else {
        // should be in extractAbove
        if(extractAbove == null) {
          extractAbove = workUnit.getExtract();
        }
        Assert.assertEquals(workUnit.getExtract(), extractAbove);
      }

    }

    Assert.assertNotNull(extractAbove);
    Assert.assertNotNull(extractBelow);

  }

  @Test public void testCopyableFileSerialization() throws Exception {

    WorkUnitState state = new WorkUnitState();
    FileStatus origin = new FileStatus(10, false, 1, 56, 0, new Path("/some/path"));
    CopyableFile copyableFile = new CopyableFile(origin, new Path("/destination/path"),
        new OwnerAndPermission("owner", "group", FsPermission.getDefault()),
        Lists.newArrayList(new OwnerAndPermission("owner2", "group2", FsPermission.getDefault())),
        "checksum".getBytes());

    state.setProp(CopySource.SERIALIZED_COPYABLE_FILE, CopySource.serializeCopyableFile(copyableFile));

    CopyableFile output = CopySource.getCopyableFile(state);

    Assert.assertEquals(copyableFile, output);

  }
}
