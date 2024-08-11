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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;

import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.ManifestBasedDataset;
import org.apache.gobblin.data.management.copy.ManifestBasedDatasetFinder;
import org.apache.gobblin.data.management.copy.entities.PostPublishStep;
import org.apache.gobblin.data.management.copy.entities.PrePublishStep;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.util.commit.CreateDirectoryWithPermissionsCommitStep;
import org.apache.gobblin.util.commit.SetPermissionCommitStep;
import org.apache.gobblin.util.filesystem.OwnerAndPermission;

import static org.mockito.Mockito.any;


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
      setSourceAndDestFsMocks(sourceFs, destFs, manifestPath, manifestReadFs, true);

      Iterator<FileSet<CopyEntity>> fileSets =
          new ManifestBasedDataset(sourceFs, manifestReadFs, manifestPath, props).getFileSetIterator(destFs,
              CopyConfiguration.builder(destFs, props).build());
      Assert.assertTrue(fileSets.hasNext());
      FileSet<CopyEntity> fileSet = fileSets.next();
      Assert.assertEquals(fileSet.getFiles().size(), 4);  // 2 files to copy + 1 pre publish step + 1 post publish step
      Assert.assertTrue(((PrePublishStep) fileSet.getFiles().get(2)).getStep() instanceof CreateDirectoryWithPermissionsCommitStep);
      Assert.assertTrue(((PostPublishStep) fileSet.getFiles().get(3)).getStep() instanceof SetPermissionCommitStep);

      CreateDirectoryWithPermissionsCommitStep
          step = (CreateDirectoryWithPermissionsCommitStep) ((PrePublishStep) fileSet.getFiles().get(2)).getStep();

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
      setSourceAndDestFsMocks(sourceFs, destFs, manifestPath, manifestReadFs, true);
      Mockito.when(destFs.exists(new Path("/tmp/dataset/test1.txt"))).thenReturn(true);
      Mockito.when(destFs.exists(new Path("/tmp/dataset/test2.txt"))).thenReturn(false);
      Mockito.when(destFs.exists(new Path("/tmp"))).thenReturn(true);
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
      Assert.assertEquals(fileSet.getFiles().size(), 4);  // 2 files to copy + 1 pre publish step + 1 post publish step
      CommitStep createDirectoryStep = ((PrePublishStep) fileSet.getFiles().get(2)).getStep();
      Assert.assertTrue(createDirectoryStep instanceof CreateDirectoryWithPermissionsCommitStep);
      Map<String, List<OwnerAndPermission>> pathAndPermissions = ((CreateDirectoryWithPermissionsCommitStep) createDirectoryStep).getPathAndPermissions();
      Assert.assertEquals(pathAndPermissions.size(), 1);
      Assert.assertTrue(pathAndPermissions.containsKey("/tmp/dataset"));

      CommitStep setPermissionStep = ((PostPublishStep) fileSet.getFiles().get(3)).getStep();
      Assert.assertTrue(setPermissionStep instanceof SetPermissionCommitStep);
      Map<String, OwnerAndPermission> ownerAndPermissionMap = ((SetPermissionCommitStep) setPermissionStep).getPathAndPermissions();
      // Ignore /tmp as it already exists on destination
      Assert.assertEquals(ownerAndPermissionMap.size(), 1);
      Assert.assertTrue(ownerAndPermissionMap.containsKey("/tmp/dataset"));
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
      setSourceAndDestFsMocks(sourceFs, destFs, manifestPath, manifestReadFs, true);
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
      Assert.assertEquals(fileSet.getFiles().size(), 2); // Pre and Post publish step
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
    setSourceAndDestFsMocks(sourceFs, destFs, manifestPath, manifestReadFs, true);
    Iterator<FileSet<CopyEntity>> fileSets = new ManifestBasedDataset(sourceFs, manifestReadFs, manifestPath, props).getFileSetIterator(destFs,
        CopyConfiguration.builder(destFs, props).build());
    Assert.assertTrue(fileSets.hasNext());
    FileSet<CopyEntity> fileSet = fileSets.next();
    Assert.assertEquals(fileSet.getFiles().size(), 3);  // 1 files to copy + 1 pre publish step + 1 post publish step
    Assert.assertTrue(((PrePublishStep) fileSet.getFiles().get(1)).getStep() instanceof CreateDirectoryWithPermissionsCommitStep);
    Assert.assertTrue(((PostPublishStep) fileSet.getFiles().get(2)).getStep() instanceof SetPermissionCommitStep);

  }

  @Test
  public void testSetPermissionNestedTreePermissions() throws IOException, URISyntaxException {

    //Get manifest Path
    Path manifestPath = new Path(getClass().getClassLoader().getResource("manifestBasedDistcpTest/longNestedDirectoryTreeManifest.json").getPath());
    // Test manifestDatasetFinder
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/");
    props.setProperty("gobblin.copy.preserved.attributes", "rbugpvta");
    try (FileSystem sourceFs = Mockito.mock(FileSystem.class);
        FileSystem manifestReadFs = Mockito.mock(FileSystem.class);
        FileSystem destFs = Mockito.mock(FileSystem.class)) {
      setSourceAndDestFsMocks(sourceFs, destFs, manifestPath, manifestReadFs, false);
      // Mock that these files exist but still recopy due to different permissions
      Mockito.when(destFs.exists(new Path("/tmp/dataset/hourly/metadata/test1.txt"))).thenReturn(true);
      Mockito.when(destFs.exists(new Path("/tmp/dataset/hourly/metadata/test2.txt"))).thenReturn(true);
      Mockito.when(destFs.exists(new Path("/tmp"))).thenReturn(false);

      Mockito.when(destFs.getFileStatus(any(Path.class))).thenReturn(localFs.getFileStatus(new Path(tmpDir.toString())));

      setFsMockPathWithPermissions(sourceFs, "/tmp/dataset/hourly/metadata/test1.txt", "-rwxrwxrwx", "owner1", "group1", false);
      setFsMockPathWithPermissions(sourceFs, "/tmp/dataset/hourly/metadata/test2.txt", "-rwxrwxrwx", "owner1", "group1", false);
      setFsMockPathWithPermissions(sourceFs, "/tmp/dataset2/hourly/metadata/test1.txt", "-rwxrwxrwx", "owner2", "group2", false);
      setFsMockPathWithPermissions(sourceFs, "/tmp/dataset2/hourly/metadata/test2.txt", "-rwxrwxrwx", "owner2", "group2", false);
      setFsMockPathWithPermissions(sourceFs, "/tmp/dataset/hourly/metadata", "drwxrw-rw-", "owner1", "group1", true);
      setFsMockPathWithPermissions(sourceFs, "/tmp/dataset2/hourly/metadata", "dr-xr-xr-x", "owner2", "group2", true);
      setFsMockPathWithPermissions(sourceFs, "/tmp/dataset/hourly", "drwxrw-rw-", "owner1", "group1", true);
      setFsMockPathWithPermissions(sourceFs, "/tmp/dataset2/hourly", "dr-xr-xr-x", "owner2", "group2", true);
      setFsMockPathWithPermissions(sourceFs, "/tmp/dataset", "drwxr-x---", "owner1", "group1", true);
      setFsMockPathWithPermissions(sourceFs, "/tmp/dataset2", "dr-xr-xr-x", "owner2", "group2", true);
      setFsMockPathWithPermissions(sourceFs, "/tmp", "dr--r--r--", "owner3", "group3", true);

      // Specify a different acl for the destination file so that it is recopied even though the modification time is the same
      AclStatus aclStatusDest = buildAclStatusWithPermissions("user::r--,group::---,other::---", "group3", "owner3");
      Mockito.when(destFs.getAclStatus(any(Path.class))).thenReturn(aclStatusDest);

      Iterator<FileSet<CopyEntity>> fileSets =
          new ManifestBasedDataset(sourceFs, manifestReadFs, manifestPath, props).getFileSetIterator(destFs,
              CopyConfiguration.builder(destFs, props).build());
      Assert.assertTrue(fileSets.hasNext());
      FileSet<CopyEntity> fileSet = fileSets.next();
      System.out.println(fileSet.getFiles().get(6).toString());
      // 4 files to copy + 1 pre publish step + 1 post publish step + 1 deleteFileCommitStep for a temporary directory
      Assert.assertEquals(fileSet.getFiles().size(), 7);
      CommitStep createDirectoryStep = ((PrePublishStep) fileSet.getFiles().get(4)).getStep();
      Assert.assertTrue(createDirectoryStep instanceof CreateDirectoryWithPermissionsCommitStep);
      Map<String, List<OwnerAndPermission>> pathAndPermissions = ((CreateDirectoryWithPermissionsCommitStep) createDirectoryStep).getPathAndPermissions();
      Assert.assertEquals(pathAndPermissions.size(), 2);
      Assert.assertTrue(pathAndPermissions.containsKey("/tmp/dataset/hourly/metadata"));
      Assert.assertTrue(pathAndPermissions.containsKey("/tmp/dataset2/hourly/metadata"));

      CommitStep setPermissionStep = ((PostPublishStep) fileSet.getFiles().get(5)).getStep();
      Assert.assertTrue(setPermissionStep instanceof SetPermissionCommitStep);
      Map<String, OwnerAndPermission> ownerAndPermissionMap = ((SetPermissionCommitStep) setPermissionStep).getPathAndPermissions();
      // Ignore /tmp as it already exists on destination
      Assert.assertEquals(ownerAndPermissionMap.size(), 7);
      System.out.println(ownerAndPermissionMap);
      List<String> sortedMapKeys = new ArrayList<>(ownerAndPermissionMap.keySet());
      Assert.assertEquals(sortedMapKeys.get(0), "/tmp");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp").getFsPermission(), FsPermission.valueOf("dr--r--r--"));
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp").getOwner(), "owner3");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp").getGroup(), "group3");

      Assert.assertEquals(sortedMapKeys.get(1), "/tmp/dataset");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset").getFsPermission(), FsPermission.valueOf("drwxr-x---"));
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset").getOwner(), "owner1");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset").getGroup(), "group1");

      Assert.assertEquals(sortedMapKeys.get(2), "/tmp/dataset2");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset2").getFsPermission(), FsPermission.valueOf("dr-xr-xr-x"));
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset2").getOwner(), "owner2");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset2").getGroup(), "group2");

      Assert.assertEquals(sortedMapKeys.get(3), "/tmp/dataset/hourly");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset/hourly").getFsPermission(), FsPermission.valueOf("drwxrw-rw-"));
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset/hourly").getOwner(), "owner1");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset/hourly").getGroup(), "group1");

      Assert.assertEquals(sortedMapKeys.get(4), "/tmp/dataset2/hourly");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset2/hourly").getFsPermission(), FsPermission.valueOf("dr-xr-xr-x"));
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset2/hourly").getOwner(), "owner2");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset2/hourly").getGroup(), "group2");

      Assert.assertEquals(sortedMapKeys.get(5), "/tmp/dataset/hourly/metadata");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset/hourly/metadata").getFsPermission(), FsPermission.valueOf("drwxrw-rw-"));
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset/hourly/metadata").getOwner(), "owner1");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset/hourly/metadata").getGroup(), "group1");

      Assert.assertEquals(sortedMapKeys.get(6), "/tmp/dataset2/hourly/metadata");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset2/hourly/metadata").getFsPermission(), FsPermission.valueOf("dr-xr-xr-x"));
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset2/hourly/metadata").getOwner(), "owner2");
      Assert.assertEquals(ownerAndPermissionMap.get("/tmp/dataset2/hourly/metadata").getGroup(), "group2");
    }
  }

  @Test
  public void testDisableSetPermissionStep() throws Exception {
    //Get manifest Path
    String manifestLocation = getClass().getClassLoader().getResource("manifestBasedDistcpTest/manifestRootDirEmpty.json").getPath();
    // Test manifestDatasetFinder
    Properties props = new Properties();
    props.setProperty("gobblin.copy.manifestBased.manifest.location", manifestLocation);
    props.setProperty("gobblin.copy.manifestBased.enableSetPermissionPostPublish", "false");
    props.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/");
    ManifestBasedDatasetFinder finder = new ManifestBasedDatasetFinder(localFs, props);
    List<ManifestBasedDataset> datasets = finder.findDatasets();
    Assert.assertEquals(datasets.size(), 1);
    FileSystem sourceFs = Mockito.mock(FileSystem.class);
    FileSystem manifestReadFs = Mockito.mock(FileSystem.class);
    FileSystem destFs = Mockito.mock(FileSystem.class);
    Path manifestPath = new Path(manifestLocation);
    setSourceAndDestFsMocks(sourceFs, destFs, manifestPath, manifestReadFs, true);
    Iterator<FileSet<CopyEntity>> fileSets = new ManifestBasedDataset(sourceFs, manifestReadFs, manifestPath, props).getFileSetIterator(destFs,
        CopyConfiguration.builder(destFs, props).build());
    Assert.assertTrue(fileSets.hasNext());
    FileSet<CopyEntity> fileSet = fileSets.next();
    Assert.assertEquals(fileSet.getFiles().size(), 2);  // 1 files to copy + 1 pre publish step
  }

  private void setSourceAndDestFsMocks(FileSystem sourceFs, FileSystem destFs, Path manifestPath, FileSystem manifestReadFs, boolean setFileStatusMock) throws IOException, URISyntaxException {
    URI SRC_FS_URI = new URI("source", "the.source.org", "/", null);
    URI MANIFEST_READ_FS_URI = new URI("manifest-read", "the.manifest-source.org", "/", null);
    URI DEST_FS_URI = new URI("dest", "the.dest.org", "/", null);
    Mockito.when(sourceFs.getUri()).thenReturn(SRC_FS_URI);
    Mockito.when(manifestReadFs.getUri()).thenReturn(MANIFEST_READ_FS_URI);
    Mockito.when(destFs.getUri()).thenReturn(DEST_FS_URI);
    Mockito.when(destFs.exists(new Path("/tmp"))).thenReturn(true);
    if (setFileStatusMock) {
      Mockito.when(sourceFs.getFileStatus(any(Path.class))).thenReturn(localFs.getFileStatus(new Path(tmpDir.toString())));
    }
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

  private AclStatus buildAclStatusWithPermissions(String aclSpec, String group, String owner) {
    List<AclEntry> aclEntries = AclEntry.parseAclSpec(aclSpec, true);
    return new AclStatus.Builder().group(group).owner(owner).addEntries(aclEntries).build();
  }

  private FileStatus createFileStatus(String path, boolean isDir, String owner, String group, FsPermission permission) throws IOException {
    return new FileStatus(1028, isDir, 0, 0, 0, 0, permission, owner, group, null, new Path(path));
  }

  private void setFsMockPathWithPermissions(FileSystem fs, String path, String permissionStr, String owner, String group, boolean isDir) throws IOException {
    AclStatus aclStatus = new AclStatus.Builder().owner(owner).group(group).build();
    Mockito.when(fs.getFileStatus(new Path(path))).thenReturn(createFileStatus(path, isDir, owner, group, FsPermission.valueOf(permissionStr)));
    Mockito.when(fs.getAclStatus(new Path(path))).thenReturn(aclStatus);
  }
}
