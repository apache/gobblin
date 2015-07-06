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

package gobblin.data.management.retention;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import static org.mockito.Mockito.*;

import gobblin.data.management.retention.dataset.DatasetBase;
import gobblin.data.management.retention.policy.RetentionPolicy;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.StringDatasetVersion;
import gobblin.data.management.retention.version.finder.DatasetVersionFinder;
import gobblin.data.management.retention.version.finder.VersionFinder;
import gobblin.data.management.trash.Trash;


public class DatasetBaseTest {

  @Test
  public void test() throws IOException {

    FileSystem fs = mock(FileSystem.class);
    Trash trash = mock(Trash.class);

    Path datasetRoot = new Path("/test/dataset");

    DatasetVersion dataset1Version1 = new StringDatasetVersion("version1", new Path(datasetRoot, "version1"));
    DatasetVersion dataset1Version2 = new StringDatasetVersion("version2", new Path(datasetRoot, "version2"));

    when(fs.delete(any(Path.class), anyBoolean())).thenReturn(true);
    when(trash.moveToTrash(any(Path.class))).thenReturn(true);

    DatasetImpl dataset = new DatasetImpl(fs, trash, false, false, false, datasetRoot);

    when(dataset.versionFinder.findDatasetVersions(dataset)).
        thenReturn(Lists.newArrayList(dataset1Version1, dataset1Version2));

    dataset.clean();

    verify(fs, never()).delete(any(Path.class), anyBoolean());
    verify(trash).moveToTrash(dataset1Version2.getPathsToDelete().iterator().next());
    verifyNoMoreInteractions(trash);

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

    DatasetImpl dataset = new DatasetImpl(fs, trash, false, true, false, datasetRoot);

    when(dataset.versionFinder.findDatasetVersions(dataset)).
        thenReturn(Lists.newArrayList(dataset1Version1, dataset1Version2));

    dataset.clean();

    verify(trash, never()).moveToTrash(any(Path.class));
    verify(fs).delete(dataset1Version2.getPathsToDelete().iterator().next(), true);
    verify(fs, times(1)).delete(any(Path.class), anyBoolean());

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

    DatasetImpl dataset = new DatasetImpl(fs, trash, true, false, false, datasetRoot);

    when(dataset.versionFinder.findDatasetVersions(dataset)).
        thenReturn(Lists.newArrayList(dataset1Version1, dataset1Version2));

    dataset.clean();

    verify(trash, never()).moveToTrash(any(Path.class));
    verify(fs, never()).delete(any(Path.class), anyBoolean());

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

    DatasetImpl dataset = new DatasetImpl(fs, trash, false, false, true, datasetRoot);

    when(dataset.versionFinder.findDatasetVersions(dataset)).
        thenReturn(Lists.newArrayList(dataset1Version1, dataset1Version2));

    when(fs.listStatus(any(Path.class))).thenReturn(new FileStatus[]{});

    dataset.clean();

    verify(trash).moveToTrash(dataset1Version2.getPathsToDelete().iterator().next());
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
    public List<StringDatasetVersion> preserveDeletableVersions(List<StringDatasetVersion> allVersions) {
      return Lists.newArrayList(allVersions.get(0));
    }
  }

  private class DatasetImpl extends DatasetBase {

    public DatasetVersionFinder versionFinder = mock(DatasetVersionFinder.class);
    public RetentionPolicy retentionPolicy = new DeleteFirstRetentionPolicy();
    public Path path;

    public DatasetImpl(FileSystem fs, Trash trash, boolean simulate, boolean skipTrash,
        boolean deleteEmptyDirectories, Path path) {
      super(fs, trash, simulate, skipTrash, deleteEmptyDirectories);
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
  }

}
