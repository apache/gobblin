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

package gobblin.data.management.copy.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.configuration.State;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.entities.PostPublishStep;
import gobblin.data.management.copy.hive.HiveCopyEntityHelper.DeregisterFileDeleteMethod;
import gobblin.hive.HiveRegProps;
import gobblin.metrics.event.MultiTimingEvent;


public class HiveCopyEntityHelperTest {

  private final Path sourceRoot = new Path("/source");
  private final Path targetRoot = new Path("/target");

  @Test public void testResolvePath() throws Exception {
    Assert.assertEquals(HiveTargetPathHelper.resolvePath("/data/$DB/$TABLE", "db", "table"), new Path("/data/db/table"));
    Assert.assertEquals(HiveTargetPathHelper.resolvePath("/data/$TABLE", "db", "table"), new Path("/data/table"));
    Assert.assertEquals(HiveTargetPathHelper.resolvePath("/data", "db", "table"), new Path("/data/table"));
  }

  @Test
  public void testFullPathDiff() throws Exception {

    Map<Path, FileStatus> sourceMap = Maps.newHashMap();
    Map<Path, FileStatus> targetDesiredMap = Maps.newHashMap();
    List<Path> expectedFilesToCopy = Lists.newArrayList();
    List<Path> expectedFilesToSkipCopy = Lists.newArrayList();
    List<Path> expectedFilesToDelete = Lists.newArrayList();
    List<Path> expectedFilesToSkipDelete = Lists.newArrayList();

    populateSourceAndTargetEntities(sourceMap, targetDesiredMap, expectedFilesToCopy,
        expectedFilesToSkipCopy, expectedFilesToDelete, expectedFilesToSkipDelete);

    TestLocationDescriptor sourceLocation = new TestLocationDescriptor(sourceMap);
    TestLocationDescriptor targetDesiredLocation = new TestLocationDescriptor(targetDesiredMap);
    TestLocationDescriptor existingTargetLocation = new TestLocationDescriptor(Maps.newHashMap(targetDesiredMap));

    MultiTimingEvent timer = Mockito.mock(MultiTimingEvent.class);
    HiveCopyEntityHelper helper = Mockito.mock(HiveCopyEntityHelper.class);
    HiveTargetPathHelper targetPathHelper = Mockito.mock(HiveTargetPathHelper.class);
    Mockito.when(targetPathHelper
        .getTargetPath(Mockito.any(Path.class), Mockito.any(FileSystem.class), Mockito.any(Optional.class),
            Mockito.anyBoolean())).then(new Answer<Path>() {
      @Override
      public Path answer(InvocationOnMock invocation)
          throws Throwable {
        Path path = (Path) invocation.getArguments()[0];
        return new Path(path.toString().replace(sourceRoot.toString(), targetRoot.toString()));
      }
    });
    Mockito.when(helper.getTargetPathHelper()).thenReturn(targetPathHelper);

    HiveCopyEntityHelper.DiffPathSet diff =
        HiveCopyEntityHelper.fullPathDiff(sourceLocation, targetDesiredLocation,
            Optional.<HiveLocationDescriptor>of(existingTargetLocation), Optional.<Partition>absent(), timer, helper);

    Assert.assertEquals(diff.filesToCopy.size(), expectedFilesToCopy.size());
    for (Path expectedFileToCopy : expectedFilesToCopy) {
      Assert.assertTrue(containsPath(diff.filesToCopy, expectedFileToCopy));
    }
    for (Path expectedFileToSkipCopy : expectedFilesToSkipCopy) {
      Assert.assertFalse(containsPath(diff.filesToCopy, expectedFileToSkipCopy));
    }

    Assert.assertEquals(diff.pathsToDelete.size(), expectedFilesToDelete.size());
    for (Path expectedFileToDelete : expectedFilesToDelete) {
      Assert.assertTrue(diff.pathsToDelete.contains(expectedFileToDelete));
    }
    for (Path expectedFileToSkipDelete : expectedFilesToSkipDelete) {
      Assert.assertFalse(diff.pathsToDelete.contains(expectedFileToSkipDelete));
    }
  }

  @Test
  public void testFullPathDiffWithUnmanagedPathsWithoutDeletePolicy() throws Exception {

    Map<Path, FileStatus> sourceMap = Maps.newHashMap();
    Map<Path, FileStatus> targetDesiredMap = Maps.newHashMap();
    List<Path> expectedFilesToCopy = Lists.newArrayList();
    List<Path> expectedFilesToSkipCopy = Lists.newArrayList();
    List<Path> expectedFilesToDelete = Lists.newArrayList();
    List<Path> expectedFilesToSkipDelete = Lists.newArrayList();

    populateSourceAndTargetEntities(sourceMap, targetDesiredMap, expectedFilesToCopy,
        expectedFilesToSkipCopy, expectedFilesToDelete, expectedFilesToSkipDelete);

    // add un-managed files to the target path
    Path path6 = new Path("path6");
    Path targetPath6 = new Path(targetRoot, path6);
    Map<Path, FileStatus> targetDesiredMapWithExtraFile = Maps.newHashMap(targetDesiredMap);
    targetDesiredMapWithExtraFile.put(targetPath6, getFileStatus(targetPath6, 0, 10));
    expectedFilesToDelete.add(targetPath6);

    TestLocationDescriptor sourceLocation = new TestLocationDescriptor(sourceMap);
    TestLocationDescriptor targetDesiredLocation = new TestLocationDescriptor(targetDesiredMapWithExtraFile);
    TestLocationDescriptor existingTargetLocation = new TestLocationDescriptor(Maps.newHashMap(targetDesiredMap));

    Table table = Mockito.mock(Table.class);
    HiveDataset hiveDataset = Mockito.mock(HiveDataset.class);
    MultiTimingEvent timer = Mockito.mock(MultiTimingEvent.class);
    HiveCopyEntityHelper helper = Mockito.mock(HiveCopyEntityHelper.class);
    HiveTargetPathHelper targetPathHelper = Mockito.mock(HiveTargetPathHelper.class);
    Mockito.when(helper.getDataset()).thenReturn(hiveDataset);
    Mockito.when(hiveDataset.getTable()).thenReturn(table);
    Mockito.when(table.getCompleteName()).thenReturn("table1");
    Mockito.when(targetPathHelper
        .getTargetPath(Mockito.any(Path.class), Mockito.any(FileSystem.class), Mockito.any(Optional.class),
            Mockito.anyBoolean())).then(new Answer<Path>() {
      @Override
      public Path answer(InvocationOnMock invocation)
          throws Throwable {
        Path path = (Path) invocation.getArguments()[0];
        return new Path(path.toString().replace(sourceRoot.toString(), targetRoot.toString()));
      }
    });
    Mockito.when(helper.getTargetPathHelper()).thenReturn(targetPathHelper);

    // Add policy to not delete un-managed data
    Mockito.when(helper.getUnmanagedDataPolicy()).thenReturn(HiveCopyEntityHelper.UnmanagedDataPolicy.ABORT);

    // We should receive an exception that un-managed files are detected
    try {
      HiveCopyEntityHelper.DiffPathSet diff =
          HiveCopyEntityHelper.fullPathDiff(sourceLocation, targetDesiredLocation,
              Optional.<HiveLocationDescriptor>of(existingTargetLocation), Optional.<Partition>absent(), timer, helper);
      Assert.fail("Expected an IOException but did not receive any");
    } catch (IOException ex) {
      // Ignore IOException if message is what we expect
      String expectedExceptionMessage = "New table / partition would pick up existing, undesired files in target file "
          + "system. table1, files [/target/path6].";
      Assert.assertEquals(ex.getMessage(), expectedExceptionMessage);
    }
  }

  @Test
  public void testFullPathDiffWithUnmanagedPathsWithDeletePolicy() throws Exception {

    Map<Path, FileStatus> sourceMap = Maps.newHashMap();
    Map<Path, FileStatus> targetDesiredMap = Maps.newHashMap();
    List<Path> expectedFilesToCopy = Lists.newArrayList();
    List<Path> expectedFilesToSkipCopy = Lists.newArrayList();
    List<Path> expectedFilesToDelete = Lists.newArrayList();
    List<Path> expectedFilesToSkipDelete = Lists.newArrayList();

    populateSourceAndTargetEntities(sourceMap, targetDesiredMap, expectedFilesToCopy,
        expectedFilesToSkipCopy, expectedFilesToDelete, expectedFilesToSkipDelete);

    // add un-managed files to the target path
    Path path6 = new Path("path6");
    Path targetPath6 = new Path(targetRoot, path6);
    Map<Path, FileStatus> targetDesiredMapWithExtraFile = Maps.newHashMap(targetDesiredMap);
    targetDesiredMapWithExtraFile.put(targetPath6, getFileStatus(targetPath6, 0, 10));
    expectedFilesToDelete.add(targetPath6);

    TestLocationDescriptor sourceLocation = new TestLocationDescriptor(sourceMap);
    TestLocationDescriptor targetDesiredLocation = new TestLocationDescriptor(targetDesiredMapWithExtraFile);
    TestLocationDescriptor existingTargetLocation = new TestLocationDescriptor(Maps.newHashMap(targetDesiredMap));

    Table table = Mockito.mock(Table.class);
    HiveDataset hiveDataset = Mockito.mock(HiveDataset.class);
    MultiTimingEvent timer = Mockito.mock(MultiTimingEvent.class);
    HiveCopyEntityHelper helper = Mockito.mock(HiveCopyEntityHelper.class);
    HiveTargetPathHelper targetPathHelper = Mockito.mock(HiveTargetPathHelper.class);
    Mockito.when(helper.getDataset()).thenReturn(hiveDataset);
    Mockito.when(hiveDataset.getTable()).thenReturn(table);
    Mockito.when(table.getCompleteName()).thenReturn("table1");
    Mockito.when(targetPathHelper
        .getTargetPath(Mockito.any(Path.class), Mockito.any(FileSystem.class), Mockito.any(Optional.class),
            Mockito.anyBoolean())).then(new Answer<Path>() {
      @Override
      public Path answer(InvocationOnMock invocation)
          throws Throwable {
        Path path = (Path) invocation.getArguments()[0];
        return new Path(path.toString().replace(sourceRoot.toString(), targetRoot.toString()));
      }
    });
    Mockito.when(helper.getTargetPathHelper()).thenReturn(targetPathHelper);

    // Add policy to delete un-managed data
    Mockito.when(helper.getUnmanagedDataPolicy()).thenReturn(HiveCopyEntityHelper.UnmanagedDataPolicy.DELETE_UNMANAGED_DATA);

    // Since policy is specified to delete un-managed data, this should not throw exception and un-managed file should
    // .. show up in pathsToDelete in the diff
    HiveCopyEntityHelper.DiffPathSet diff =
        HiveCopyEntityHelper.fullPathDiff(sourceLocation, targetDesiredLocation,
            Optional.<HiveLocationDescriptor>of(existingTargetLocation), Optional.<Partition>absent(), timer, helper);

    Assert.assertEquals(diff.filesToCopy.size(), expectedFilesToCopy.size());
    for (Path expectedFileToCopy : expectedFilesToCopy) {
      Assert.assertTrue(containsPath(diff.filesToCopy, expectedFileToCopy));
    }
    for (Path expectedFileToSkipCopy : expectedFilesToSkipCopy) {
      Assert.assertFalse(containsPath(diff.filesToCopy, expectedFileToSkipCopy));
    }

    Assert.assertEquals(diff.pathsToDelete.size(), expectedFilesToDelete.size());
    for (Path expectedFileToDelete : expectedFilesToDelete) {
      Assert.assertTrue(diff.pathsToDelete.contains(expectedFileToDelete));
    }
    for (Path expectedFileToSkipDelete : expectedFilesToSkipDelete) {
      Assert.assertFalse(diff.pathsToDelete.contains(expectedFileToSkipDelete));
    }
  }

  private void populateSourceAndTargetEntities(Map<Path, FileStatus> sourceMap, Map<Path, FileStatus> targetDesiredMap,
      List<Path> expectedFilesToCopy, List<Path> expectedFilesToSkipCopy,
      List<Path> expectedFilesToDelete, List<Path> expectedFilesToSkipDelete) {
    List<FileStatus> sourceFileStatuses = Lists.newArrayList();
    List<FileStatus> desiredTargetStatuses = Lists.newArrayList();

    // already exists in target
    Path path1 = new Path("path1");
    Path sourcePath1 = new Path(sourceRoot, path1);
    Path targetPath1 = new Path(targetRoot, path1);
    sourceFileStatuses.add(getFileStatus(sourcePath1, 0, 0));
    desiredTargetStatuses.add(getFileStatus(targetPath1, 0, 10));
    expectedFilesToSkipCopy.add(sourcePath1);
    expectedFilesToSkipDelete.add(targetPath1);

    // not exists in target
    Path path2 = new Path("path2");
    Path sourcePath2 = new Path(sourceRoot, path2);
    Path targetPath2 = new Path(targetRoot, path2);
    sourceFileStatuses.add(getFileStatus(sourcePath2, 0, 0));
    expectedFilesToCopy.add(sourcePath2);
    expectedFilesToSkipDelete.add(targetPath2);

    // exists in target, different length
    Path path3 = new Path("path3");
    Path sourcePath3 = new Path(sourceRoot, path3);
    Path targetPath3 = new Path(targetRoot, path3);
    sourceFileStatuses.add(getFileStatus(sourcePath3, 0, 0));
    desiredTargetStatuses.add(getFileStatus(targetPath3, 10, 0));
    expectedFilesToCopy.add(sourcePath3);
    expectedFilesToDelete.add(targetPath3);

    // exists in target, newer modtime
    Path path4 = new Path("path4");
    Path sourcePath4 = new Path(sourceRoot, path4);
    Path targetPath4 = new Path(targetRoot, path4);
    sourceFileStatuses.add(getFileStatus(sourcePath4, 0, 10));
    desiredTargetStatuses.add(getFileStatus(targetPath4, 0, 0));
    expectedFilesToCopy.add(sourcePath4);
    expectedFilesToDelete.add(targetPath4);

    // only on target, expect delete
    Path path5 = new Path("path5");
    Path sourcePath5 = new Path(sourceRoot, path5);
    Path targetPath5 = new Path(targetRoot, path5);
    desiredTargetStatuses.add(getFileStatus(targetPath5, 0, 10));
    expectedFilesToSkipCopy.add(sourcePath5);
    expectedFilesToDelete.add(targetPath5);

    for(FileStatus status : sourceFileStatuses) {
      sourceMap.put(status.getPath(), status);
    }
    for(FileStatus status : desiredTargetStatuses) {
      targetDesiredMap.put(status.getPath(), status);
    }
  }

  @Test
  public void testAddTableDeregisterSteps() throws Exception {
    HiveDataset dataset = Mockito.mock(HiveDataset.class);
    Mockito.when(dataset.getProperties()).thenReturn(new Properties());

    HiveCopyEntityHelper helper = Mockito.mock(HiveCopyEntityHelper.class);
    Mockito.when(helper.getDeleteMethod()).thenReturn(DeregisterFileDeleteMethod.NO_DELETE);
    Mockito.when(helper.getTargetURI()).thenReturn(Optional.of("/targetURI"));
    Mockito.when(helper.getHiveRegProps()).thenReturn(new HiveRegProps(new State()));
    Mockito.when(helper.getDataset()).thenReturn(dataset);

    Mockito.when(helper.addTableDeregisterSteps(Mockito.any(List.class), Mockito.any(String.class), Mockito.anyInt(),
        Mockito.any(org.apache.hadoop.hive.ql.metadata.Table.class))).thenCallRealMethod();

    org.apache.hadoop.hive.ql.metadata.Table meta_table = Mockito.mock(org.apache.hadoop.hive.ql.metadata.Table.class);
    org.apache.hadoop.hive.metastore.api.Table api_table =
        Mockito.mock(org.apache.hadoop.hive.metastore.api.Table.class);
    Mockito.when(api_table.getDbName()).thenReturn("TestDB");
    Mockito.when(api_table.getTableName()).thenReturn("TestTable");
    Mockito.when(meta_table.getTTable()).thenReturn(api_table);

    List<CopyEntity> copyEntities = new ArrayList<CopyEntity>();
    String fileSet = "testFileSet";
    int initialPriority = 0;
    int priority = helper.addTableDeregisterSteps(copyEntities, fileSet, initialPriority, meta_table);
    Assert.assertTrue(priority == 1);
    Assert.assertTrue(copyEntities.size() == 1);

    Assert.assertTrue(copyEntities.get(0) instanceof PostPublishStep);
    PostPublishStep p = (PostPublishStep) (copyEntities.get(0));
    Assert
        .assertTrue(p.getStep().toString().contains("Deregister table TestDB.TestTable on Hive metastore /targetURI"));
  }

  @Test public void testReplacedPrefix() throws Exception {
    Path sourcePath = new Path("/data/databases/DB1/Table1/SS1/part1.avro");
    Path prefixTobeReplaced = new Path("/data/databases");
    Path prefixReplacement = new Path("/data/databases/_parallel");
    Path expected = new Path("/data/databases/_parallel/DB1/Table1/SS1/part1.avro");
    Assert.assertEquals(HiveCopyEntityHelper.replacedPrefix(sourcePath, prefixTobeReplaced, prefixReplacement), expected);
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
