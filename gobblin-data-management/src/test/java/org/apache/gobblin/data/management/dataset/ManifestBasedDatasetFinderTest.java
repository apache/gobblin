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

package org.apache.gobblin.data.management.dataset;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.ManifestBasedDataset;
import org.apache.gobblin.data.management.copy.ManifestBasedDatasetFinder;
import org.apache.gobblin.data.management.copy.entities.PostPublishStep;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.util.commit.CreateAndSetDirectoryPermissionCommitStep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import static org.mockito.Mockito.*;


public class ManifestBasedDatasetFinderTest {
  private FileSystem localFs;
  private File tmpDir;

  public ManifestBasedDatasetFinderTest() throws IOException {
    localFs = FileSystem.getLocal(new Configuration());
    tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
  }

  @Test
  public void testFindDataset() throws IOException {

    //Get manifest Path
    String manifestPath = getClass().getClassLoader().getResource("manifestBasedDistcpTest/sampleManifest.json").getPath();

    // Test manifestDatasetFinder
    Properties props = new Properties();
    props.setProperty("gobblin.copy.manifestBased.manifest.location", manifestPath);
    ManifestBasedDatasetFinder finder = new ManifestBasedDatasetFinder(localFs, props);
    List<ManifestBasedDataset> datasets = finder.findDatasets();
    Assert.assertEquals(datasets.size(), 1);
  }

  @Test
  public void testFindFiles() throws IOException, URISyntaxException {

    //Get manifest Path
    Path manifestPath = new Path(getClass().getClassLoader().getResource("manifestBasedDistcpTest/sampleManifest.json").getPath());

    // Test manifestDatasetFinder
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/");

    try (FileSystem sourceFs = Mockito.mock(FileSystem.class);
        FileSystem manifestReadFs = Mockito.mock(FileSystem.class);
        FileSystem destFs = Mockito.mock(FileSystem.class);) {
      setSourceAndDestFsMocks(sourceFs, destFs, manifestPath, manifestReadFs);

      Iterator<FileSet<CopyEntity>> fileSets =
          new ManifestBasedDataset(sourceFs, manifestReadFs, manifestPath, props).getFileSetIterator(destFs,
              CopyConfiguration.builder(destFs, props).build());
      Assert.assertTrue(fileSets.hasNext());
      FileSet<CopyEntity> fileSet = fileSets.next();
      Assert.assertEquals(fileSet.getFiles().size(), 3);  // 2 files to copy + 1 post publish step
      Assert.assertTrue(((PostPublishStep) fileSet.getFiles().get(2)).getStep() instanceof CreateAndSetDirectoryPermissionCommitStep);
      CreateAndSetDirectoryPermissionCommitStep
          step = (CreateAndSetDirectoryPermissionCommitStep) ((PostPublishStep) fileSet.getFiles().get(2)).getStep();

      Assert.assertEquals(step.getPathAndPermissions().keySet().size(), 1); // SetPermissionCommitStep only applies to ancestors
      Mockito.verify(manifestReadFs, Mockito.times(1)).exists(manifestPath);
      Mockito.verify(manifestReadFs, Mockito.times(1)).getFileStatus(manifestPath);
      Mockito.verify(manifestReadFs, Mockito.times(1)).open(manifestPath);
      Mockito.verifyNoMoreInteractions(manifestReadFs);
      Mockito.verify(sourceFs, Mockito.times(2)).exists(any(Path.class));
    }
  }

  @Test
  public void testFindFilesWithDifferentPermissions() throws IOException, URISyntaxException {

    //Get manifest Path
    Path manifestPath = new Path(getClass().getClassLoader().getResource("manifestBasedDistcpTest/sampleManifest.json").getPath());
    // Test manifestDatasetFinder
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/");
    props.setProperty("gobblin.copy.preserved.attributes", "rbugpvta");
    try (
        FileSystem sourceFs = Mockito.mock(FileSystem.class);
        FileSystem manifestReadFs = Mockito.mock(FileSystem.class);
        FileSystem destFs = Mockito.mock(FileSystem.class)
    ) {
      setSourceAndDestFsMocks(sourceFs, destFs, manifestPath, manifestReadFs);
      Mockito.when(destFs.exists(new Path("/tmp/dataset/test1.txt"))).thenReturn(true);
      Mockito.when(destFs.exists(new Path("/tmp/dataset/test2.txt"))).thenReturn(false);
      Mockito.when(destFs.getFileStatus(any(Path.class))).thenReturn(localFs.getFileStatus(new Path(tmpDir.toString())));

      List<AclEntry> aclEntrySource = AclEntry.parseAclSpec("user::rwx,group::rwx,other::rwx", true);
      AclStatus aclStatusSource = new AclStatus.Builder().group("group").owner("owner").addEntries(aclEntrySource).build();
      Mockito.when(sourceFs.getAclStatus(any(Path.class))).thenReturn(aclStatusSource);
      // Specify a different acl for the destination file so that it is recopied even though the modification time is the same
      List<AclEntry> aclEntryDest = AclEntry.parseAclSpec("user::rwx,group::rw-,other::r--", true);
      AclStatus aclStatusDest = new AclStatus.Builder().group("groupDest").owner("owner").addEntries(aclEntryDest).build();
      Mockito.when(destFs.getAclStatus(any(Path.class))).thenReturn(aclStatusDest);

      Iterator<FileSet<CopyEntity>> fileSets =
          new ManifestBasedDataset(sourceFs, manifestReadFs, manifestPath, props).getFileSetIterator(destFs, CopyConfiguration.builder(destFs, props).build());
      Assert.assertTrue(fileSets.hasNext());
      FileSet<CopyEntity> fileSet = fileSets.next();
      Assert.assertEquals(fileSet.getFiles().size(), 3);  // 2 files to copy + 1 post publish step
      Assert.assertTrue(((PostPublishStep)fileSet.getFiles().get(2)).getStep() instanceof CreateAndSetDirectoryPermissionCommitStep);

    }
  }

  @Test
  public void testIgnoreFilesWithSamePermissions() throws IOException, URISyntaxException {
    //Get manifest Path
    Path manifestPath = new Path(getClass().getClassLoader().getResource("manifestBasedDistcpTest/sampleManifest.json").getPath());
    // Test manifestDatasetFinder
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/");
    props.setProperty("gobblin.copy.preserved.attributes", "rbugpvta");
    try (
        FileSystem sourceFs = Mockito.mock(FileSystem.class);
        FileSystem manifestReadFs = Mockito.mock(FileSystem.class);
        FileSystem destFs = Mockito.mock(FileSystem.class)
    ) {
      setSourceAndDestFsMocks(sourceFs, destFs, manifestPath, manifestReadFs);
      Mockito.when(destFs.exists(new Path("/tmp/dataset/test1.txt"))).thenReturn(true);
      Mockito.when(destFs.exists(new Path("/tmp/dataset/test2.txt"))).thenReturn(true);
      Mockito.when(destFs.getFileStatus(any(Path.class))).thenReturn(localFs.getFileStatus(new Path(tmpDir.toString())));

      List<AclEntry> aclEntrySource = AclEntry.parseAclSpec("user::rwx,group::rwx,other::rwx", true);
      AclStatus aclStatusSource = new AclStatus.Builder().group("group").owner("owner").addEntries(aclEntrySource).build();
      Mockito.when(sourceFs.getAclStatus(any(Path.class))).thenReturn(aclStatusSource);
      // Same as source acls, files should not be copied
      AclStatus aclStatusDest = new AclStatus.Builder().group("groupDest").owner("owner").addEntries(aclEntrySource).build();
      Mockito.when(destFs.getAclStatus(any(Path.class))).thenReturn(aclStatusDest);

      Iterator<FileSet<CopyEntity>> fileSets =
          new ManifestBasedDataset(sourceFs, manifestReadFs, manifestPath, props).getFileSetIterator(destFs, CopyConfiguration.builder(destFs, props).build());
      Assert.assertTrue(fileSets.hasNext());
      FileSet<CopyEntity> fileSet = fileSets.next();
      Assert.assertEquals(fileSet.getFiles().size(), 1); // Post publish step
    }
  }

  @Test
  public void testFindDatasetEmptyRoot() throws Exception {
    //Get manifest Path
    String manifestLocation = getClass().getClassLoader().getResource("manifestBasedDistcpTest/manifestRootDirEmpty.json").getPath();

    // Test manifestDatasetFinder
    Properties props = new Properties();
    props.setProperty("gobblin.copy.manifestBased.manifest.location", manifestLocation);
    props.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/");
    ManifestBasedDatasetFinder finder = new ManifestBasedDatasetFinder(localFs, props);
    List<ManifestBasedDataset> datasets = finder.findDatasets();
    Assert.assertEquals(datasets.size(), 1);
    FileSystem sourceFs = Mockito.mock(FileSystem.class);
    FileSystem manifestReadFs = Mockito.mock(FileSystem.class);
    FileSystem destFs = Mockito.mock(FileSystem.class);
    Path manifestPath = new Path(manifestLocation);
    setSourceAndDestFsMocks(sourceFs, destFs, manifestPath, manifestReadFs);
    Iterator<FileSet<CopyEntity>> fileSets = new ManifestBasedDataset(sourceFs, manifestReadFs, manifestPath, props).getFileSetIterator(destFs,
        CopyConfiguration.builder(destFs, props).build());
    Assert.assertTrue(fileSets.hasNext());
    FileSet<CopyEntity> fileSet = fileSets.next();
    Assert.assertEquals(fileSet.getFiles().size(), 2);  // 1 files to copy + 1 post publish step
    Assert.assertTrue(((PostPublishStep) fileSet.getFiles().get(1)).getStep() instanceof CreateAndSetDirectoryPermissionCommitStep);
  }

  private void setSourceAndDestFsMocks(FileSystem sourceFs, FileSystem destFs, Path manifestPath, FileSystem manifestReadFs) throws IOException, URISyntaxException {
    URI SRC_FS_URI = new URI("source", "the.source.org", "/", null);
    URI MANIFEST_READ_FS_URI = new URI("manifest-read", "the.manifest-source.org", "/", null);
    URI DEST_FS_URI = new URI("dest", "the.dest.org", "/", null);
    Mockito.when(sourceFs.getUri()).thenReturn(SRC_FS_URI);
    Mockito.when(manifestReadFs.getUri()).thenReturn(MANIFEST_READ_FS_URI);
    Mockito.when(destFs.getUri()).thenReturn(DEST_FS_URI);
    Mockito.when(destFs.exists(new Path("/tmp"))).thenReturn(true);
    Mockito.when(sourceFs.getFileStatus(any(Path.class))).thenReturn(localFs.getFileStatus(new Path(tmpDir.toString())));
    Mockito.when(sourceFs.exists(any(Path.class))).thenReturn(true);
    Mockito.when(manifestReadFs.exists(any(Path.class))).thenReturn(true);
    Mockito.when(manifestReadFs.getFileStatus(manifestPath)).thenReturn(localFs.getFileStatus(manifestPath));
    Mockito.when(manifestReadFs.open(manifestPath)).thenReturn(localFs.open(manifestPath));

    Mockito.doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      Path path = (Path)args[0];
      return localFs.makeQualified(path);
    }).when(sourceFs).makeQualified(any(Path.class));
  }
}
