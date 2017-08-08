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

package org.apache.gobblin.data.management.retention;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import static org.mockito.Mockito.*;

import org.apache.gobblin.data.management.retention.dataset.CleanableDatasetBase;
import org.apache.gobblin.data.management.retention.policy.RetentionPolicy;
import org.apache.gobblin.data.management.retention.version.DatasetVersion;
import org.apache.gobblin.data.management.retention.version.StringDatasetVersion;
import org.apache.gobblin.data.management.retention.version.finder.DatasetVersionFinder;
import org.apache.gobblin.data.management.retention.version.finder.VersionFinder;
import org.apache.gobblin.data.management.trash.TestTrash;
import org.apache.gobblin.data.management.trash.Trash;


public class CleanableDatasetBaseTest {

  @Test
  public void test() throws IOException {

    FileSystem fs = mock(FileSystem.class);

    Path datasetRoot = new Path("/test/dataset");

    DatasetVersion dataset1Version1 = new StringDatasetVersion("version1", new Path(datasetRoot, "version1"));
    DatasetVersion dataset1Version2 = new StringDatasetVersion("version2", new Path(datasetRoot, "version2"));

    when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
    when(fs.exists(any(Path.class))).thenReturn(true);

    DatasetImpl dataset = new DatasetImpl(fs, false, false, false, false, datasetRoot);

    when(dataset.versionFinder.findDatasetVersions(dataset)).
        thenReturn(Lists.newArrayList(dataset1Version1, dataset1Version2));

    dataset.clean();

    Assert.assertEquals(dataset.getTrash().getDeleteOperations().size(), 1);
    Assert.assertTrue(dataset.getTrash().getDeleteOperations().get(0).getPath()
        .equals(dataset1Version2.getPathsToDelete().iterator().next()));

  }

  @Test
  public void testSkipTrash() throws IOException {

    FileSystem fs = mock(FileSystem.class);
    Trash trash = mock(Trash.class);

    Path datasetRoot = new Path("/test/dataset");

    DatasetVersion dataset1Version1 = new StringDatasetVersion("version1", new Path(datasetRoot, "version1"));
    DatasetVersion dataset1Version2 = new StringDatasetVersion("version2", new Path(datasetRoot, "version2"));

    when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
    when(trash.moveToTrash(any(Path.class))).thenReturn(true);
    when(fs.exists(any(Path.class))).thenReturn(true);
    DatasetImpl dataset = new DatasetImpl(fs, false, true, false, false, datasetRoot);

    when(dataset.versionFinder.findDatasetVersions(dataset)).
        thenReturn(Lists.newArrayList(dataset1Version1, dataset1Version2));

    dataset.clean();

    Assert.assertEquals(dataset.getTrash().getDeleteOperations().size(), 1);
    Assert.assertTrue(dataset.getTrash().getDeleteOperations().get(0).getPath()
        .equals(dataset1Version2.getPathsToDelete().iterator().next()));

    Assert.assertTrue(dataset.getTrash().isSkipTrash());

  }

  @Test
  public void testSimulate() throws IOException {

    FileSystem fs = mock(FileSystem.class);
    Trash trash = mock(Trash.class);

    Path datasetRoot = new Path("/test/dataset");

    DatasetVersion dataset1Version1 = new StringDatasetVersion("version1", new Path(datasetRoot, "version1"));
    DatasetVersion dataset1Version2 = new StringDatasetVersion("version2", new Path(datasetRoot, "version2"));

    when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
    when(trash.moveToTrash(any(Path.class))).thenReturn(true);
    when(fs.exists(any(Path.class))).thenReturn(true);
    DatasetImpl dataset = new DatasetImpl(fs, true, false, false, false, datasetRoot);

    when(dataset.versionFinder.findDatasetVersions(dataset)).
        thenReturn(Lists.newArrayList(dataset1Version1, dataset1Version2));

    dataset.clean();

    Assert.assertEquals(dataset.getTrash().getDeleteOperations().size(), 1);
    Assert.assertTrue(dataset.getTrash().getDeleteOperations().get(0).getPath()
        .equals(dataset1Version2.getPathsToDelete().iterator().next()));

    Assert.assertTrue(dataset.getTrash().isSimulate());

  }

  @Test
  public void testDeleteEmptyDirectories() throws IOException {
    FileSystem fs = mock(FileSystem.class);
    Trash trash = mock(Trash.class);

    Path datasetRoot = new Path("/test/dataset");

    DatasetVersion dataset1Version1 = new StringDatasetVersion("version1", new Path(datasetRoot, "parent/version1"));
    DatasetVersion dataset1Version2 = new StringDatasetVersion("version2", new Path(datasetRoot, "parent/version2"));

    when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
    when(trash.moveToTrash(any(Path.class))).thenReturn(true);
    when(fs.exists(any(Path.class))).thenReturn(true);
    DatasetImpl dataset = new DatasetImpl(fs, false, false, true, false, datasetRoot);

    when(dataset.versionFinder.findDatasetVersions(dataset)).
        thenReturn(Lists.newArrayList(dataset1Version1, dataset1Version2));

    when(fs.listStatus(any(Path.class))).thenReturn(new FileStatus[]{});

    dataset.clean();

    Assert.assertEquals(dataset.getTrash().getDeleteOperations().size(), 1);
    Assert.assertTrue(dataset.getTrash().getDeleteOperations().get(0).getPath()
        .equals(dataset1Version2.getPathsToDelete().iterator().next()));
    verify(fs).listStatus(dataset1Version2.getPathsToDelete().iterator().next().getParent());
    verify(fs, times(1)).listStatus(any(Path.class));
    verify(fs).delete(dataset1Version2.getPathsToDelete().iterator().next().getParent(), false);
    verify(fs, times(1)).delete(any(Path.class), eq(false));
    verify(fs, never()).delete(any(Path.class), eq(true));

  }

  private class DeleteFirstRetentionPolicy implements RetentionPolicy<StringDatasetVersion> {
    @Override
    public Class<? extends DatasetVersion> versionClass() {
      return StringDatasetVersion.class;
    }

    @Override
    public Collection<StringDatasetVersion> listDeletableVersions(List<StringDatasetVersion> allVersions) {
      return Lists.newArrayList(allVersions.get(0));
    }
  }

  private class DatasetImpl extends CleanableDatasetBase {

    public DatasetVersionFinder versionFinder = mock(DatasetVersionFinder.class);
    public RetentionPolicy retentionPolicy = new DeleteFirstRetentionPolicy();
    public Path path;

    public DatasetImpl(FileSystem fs, boolean simulate, boolean skipTrash,
        boolean deleteEmptyDirectories, boolean deleteAsOwner, Path path) throws IOException {
      super(fs, TestTrash.propertiesForTestTrash(), simulate, skipTrash, deleteEmptyDirectories, deleteAsOwner,
          LoggerFactory.getLogger(DatasetImpl.class));
      when(versionFinder.versionClass()).thenReturn(StringDatasetVersion.class);
      this.path = path;
    }

    @Override
    public VersionFinder getVersionFinder() {
      return this.versionFinder;
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
      return this.retentionPolicy;
    }

    @Override
    public Path datasetRoot() {
      return this.path;
    }

    public TestTrash getTrash() {
      return (TestTrash) this.trash;
    }
  }

}
