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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.dataset.PartitionDescriptor;
import org.apache.gobblin.util.PathUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class CopyableFileTest {

  @Test
  public void testSerializeDeserialze() throws Exception {

    CopyableFile copyableFile =
        new CopyableFile(new FileStatus(10, false, 12, 100, 12345, new Path("/path")), new Path("/destination"),
            new OwnerAndPermission("owner", "group", FsPermission.getDefault()),
            Lists.newArrayList(new OwnerAndPermission("owner2", "group2", FsPermission.getDefault())),
            "checksum".getBytes(), PreserveAttributes.fromMnemonicString(""), "", 0, 0, Maps
            .<String, String>newHashMap(), "", null);

    DatasetDescriptor dataset = new DatasetDescriptor("hive", "db.table");
    PartitionDescriptor descriptor = new PartitionDescriptor("datepartition=2018/09/05", dataset);
    copyableFile.setDestinationData(descriptor);

    String s = CopyEntity.serialize(copyableFile);
    CopyEntity de = CopyEntity.deserialize(s);

    Assert.assertEquals(de, copyableFile);
  }

  @Test
  public void testSerializeDeserialzeNulls() throws Exception {

    CopyableFile copyableFile =
        new CopyableFile(null, null, new OwnerAndPermission("owner", "group",
            FsPermission.getDefault()), Lists.newArrayList(new OwnerAndPermission(null, "group2", FsPermission
            .getDefault())), "checksum".getBytes(), PreserveAttributes.fromMnemonicString(""), "", 0, 0,
            Maps.<String, String>newHashMap(), "", null);

    String serialized = CopyEntity.serialize(copyableFile);
    CopyEntity deserialized = CopyEntity.deserialize(serialized);

    Assert.assertEquals(deserialized, copyableFile);

  }

  @Test
  public void testSerializeDeserialzeList() throws Exception {

    List<CopyEntity> copyEntities =
        ImmutableList.<CopyEntity>of(CopyableFileUtils.getTestCopyableFile(), CopyableFileUtils.getTestCopyableFile(),
            CopyableFileUtils.getTestCopyableFile());

    String serialized = CopyEntity.serializeList(copyEntities);
    List<CopyEntity> deserialized = CopyEntity.deserializeList(serialized);

    Assert.assertEquals(deserialized, copyEntities);

  }

  @Test
  public void testSetFsDatasets() throws URISyntaxException {
    FileSystem originFs = mock(FileSystem.class);
    String originFsUri = "hdfs://source.company.biz:2000";
    String originPath = "/data/databases/source/profile";
    when(originFs.getUri()).thenReturn(new URI(originFsUri));
    when(originFs.getScheme()).thenReturn("hdfs");

    FileSystem targetFs = mock(FileSystem.class);
    String targetFsUri = "file:///";
    String destinationPath = "/data/databases/destination/profile";
    when(targetFs.getUri()).thenReturn(new URI(targetFsUri));
    when(targetFs.getScheme()).thenReturn("file");

    // Test when source file is not a directory
    FileStatus origin = new FileStatus(0l, false, 0, 0l, 0l, new Path(originPath));
    CopyableFile copyableFile = new CopyableFile(origin, new Path(destinationPath), null, null, null,
        PreserveAttributes.fromMnemonicString(""), "", 0, 0, Maps.<String, String>newHashMap(), "", null);
    copyableFile.setFsDatasets(originFs, targetFs);
    DatasetDescriptor source = (DatasetDescriptor) copyableFile.getSourceData();
    Assert.assertEquals(source.getName(), "/data/databases/source");
    Assert.assertEquals(source.getPlatform(), "hdfs");
    Assert.assertEquals(source.getMetadata().get("fsUri"), originFsUri);
    DatasetDescriptor destination = (DatasetDescriptor) copyableFile.getDestinationData();
    Assert.assertEquals(destination.getName(), "/data/databases/destination");
    Assert.assertEquals(destination.getPlatform(), "file");
    Assert.assertEquals(destination.getMetadata().get("fsUri"), targetFsUri);

    // Test when source file is a directory
    originPath = originFsUri + originPath;
    destinationPath = targetFsUri + destinationPath;
    origin = new FileStatus(0l, true, 0, 0l, 0l, new Path(originPath));
    copyableFile = new CopyableFile(origin, new Path(destinationPath), null, null, null,
        PreserveAttributes.fromMnemonicString(""), "", 0, 0, Maps.<String, String>newHashMap(), "", null);
    copyableFile.setFsDatasets(originFs, targetFs);
    source = (DatasetDescriptor) copyableFile.getSourceData();
    Assert.assertEquals(source.getName(), "/data/databases/source/profile");
    Assert.assertEquals(source.getPlatform(), "hdfs");
    Assert.assertEquals(source.getMetadata().get("fsUri"), originFsUri);
    destination = (DatasetDescriptor) copyableFile.getDestinationData();
    Assert.assertEquals(destination.getName(), "/data/databases/destination/profile");
    Assert.assertEquals(destination.getPlatform(), "file");
    Assert.assertEquals(destination.getMetadata().get("fsUri"), targetFsUri);
  }


  @Test
  public void testCopyableFileBuilderMinimumConfiguration()
      throws IOException {
    // Source
    String datasetRootDir = "/data/databases/source";
    Path datasetRoot = new Path(datasetRootDir);
    FileSystem originFS = FileSystem.getLocal(new Configuration());
    Path originFile = new Path(datasetRootDir, "copyableFile");
    FileStatus origin = new FileStatus(0l, false, 0, 0l, System.currentTimeMillis(), originFile);
    PreserveAttributes preserveAttributes = PreserveAttributes.fromMnemonicString("ugp");

    // Target
    String targetRoot = "/data/databases/destination";
    Path relativePath = PathUtils.relativizePath(originFile, datasetRoot);
    Path targetPath = new Path(targetRoot, relativePath);

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/publisher");
    CopyConfiguration copyConfiguration =
        CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).preserve(preserveAttributes).build();

    CopyableFile copyableFile = CopyableFile.builder(originFS, origin, datasetRoot, copyConfiguration)
        .destination(targetPath)
        .ancestorsOwnerAndPermission(Lists.<OwnerAndPermission>newArrayList()) // not testing ancestors
        .build();

    // Making sure all fields are populated correctly via CopyableFile builder

    // Verify preserve attribute options
    Assert.assertEquals(copyableFile.getPreserve().toMnemonicString(), preserveAttributes.toMnemonicString());

    // Verify origin
    Assert.assertEquals(copyableFile.getFileSet(), "");
    Assert.assertEquals(copyableFile.getOrigin(), origin);

    // Verify destination target, permissions and other attributes
    Assert.assertEquals(copyableFile.getChecksum().length, 0);
    Assert.assertEquals(copyableFile.getDestination().toString(), targetPath.toString());
    Assert.assertEquals(copyableFile.getDestinationOwnerAndPermission().getGroup(), origin.getGroup());
    Assert.assertEquals(copyableFile.getDestinationOwnerAndPermission().getOwner(), origin.getOwner());
    Assert.assertEquals(copyableFile.getDestinationOwnerAndPermission().getFsPermission(),
        origin.getPermission());

    // Verify auto determined timestamp
    Assert.assertEquals(copyableFile.getOriginTimestamp(), origin.getModificationTime());
    Assert.assertEquals(copyableFile.getUpstreamTimestamp(), origin.getModificationTime());
  }

  @Test
  public void testCopyableFileBuilderMaximumConfiguration()
      throws IOException {
    // Source
    String datasetRootDir = "/data/databases/source";
    Path datasetRoot = new Path(datasetRootDir);
    FileSystem originFS = FileSystem.getLocal(new Configuration());
    Path originFile = new Path(datasetRootDir, "copyableFile");
    FileStatus origin = new FileStatus(0l, false, 0, 0l, System.currentTimeMillis(), originFile);
    PreserveAttributes preserveAttributes = PreserveAttributes.fromMnemonicString("ugp");

    // Target
    String targetRoot = "/data/databases/destination";
    Path relativePath = PathUtils.relativizePath(originFile, datasetRoot);
    Path targetPath = new Path(targetRoot, relativePath);

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/publisher");
    CopyConfiguration copyConfiguration =
        CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).preserve(preserveAttributes).build();

    // Other attributes
    String fileSet = "fileset";
    byte[] checksum = new byte[1];
    long originTimestamp = 23091986l;
    long upstreamTimestamp = 23091986l;
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission("gobblin", "gobblin-dev", origin.getPermission());

    CopyableFile copyableFile = CopyableFile.builder(originFS, origin, datasetRoot, copyConfiguration)
        .fileSet(fileSet).checksum(checksum)
        .originTimestamp(originTimestamp).upstreamTimestamp(upstreamTimestamp)
        .destinationOwnerAndPermission(ownerAndPermission)
        .origin(origin)
        .preserve(preserveAttributes)
        .destination(targetPath)
        .ancestorsOwnerAndPermission(Lists.<OwnerAndPermission>newArrayList())
        .build();

    // Verify preserve attribute options
    Assert.assertEquals(copyableFile.getPreserve().toMnemonicString(), preserveAttributes.toMnemonicString());

    // Verify origin
    Assert.assertEquals(copyableFile.getFileSet(), fileSet);
    Assert.assertEquals(copyableFile.getOrigin(), origin);

    // Verify destination target, permissions and other attributes
    Assert.assertEquals(copyableFile.getChecksum().length, 1);
    Assert.assertEquals(copyableFile.getDestination().toString(), targetPath.toString());
    Assert.assertEquals(copyableFile.getDestinationOwnerAndPermission().getGroup(), ownerAndPermission.getGroup());
    Assert.assertEquals(copyableFile.getDestinationOwnerAndPermission().getOwner(), ownerAndPermission.getOwner());
    Assert.assertEquals(copyableFile.getDestinationOwnerAndPermission().getFsPermission(),
        ownerAndPermission.getFsPermission());

    // Verify auto determined timestamp
    Assert.assertEquals(copyableFile.getOriginTimestamp(), originTimestamp);
    Assert.assertEquals(copyableFile.getUpstreamTimestamp(), upstreamTimestamp);
  }

  @Test
  public void testResolveOwnerAndPermission() throws Exception {

    Path path = new Path("/test/path");

    FileStatus fileStatus = new FileStatus(1, false, 0, 0, 0, 0, FsPermission.getDefault(), "owner", "group", path);

    FileSystem fs = mock(FileSystem.class);
    Mockito.doReturn(fileStatus).when(fs).getFileStatus(path);
    Mockito.doReturn(path).when(fs).makeQualified(path);
    Mockito.doReturn(new URI("hdfs://uri")).when(fs).getUri();

    Properties properties = new Properties();
    properties.put(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/final/dir");

    OwnerAndPermission ownerAndPermission = CopyableFile.resolveReplicatedOwnerAndPermission(fs, path,
        new CopyConfiguration.CopyConfigurationBuilder(fs, properties).build());
    Assert.assertEquals(ownerAndPermission.getOwner(), null);
    Assert.assertEquals(ownerAndPermission.getGroup(), null);
    Assert.assertEquals(ownerAndPermission.getFsPermission(), null);

    ownerAndPermission = CopyableFile.resolveReplicatedOwnerAndPermission(fs, path,
        new CopyConfiguration.CopyConfigurationBuilder(fs, properties).targetGroup(Optional.of("target")).build());
    Assert.assertEquals(ownerAndPermission.getOwner(), null);
    Assert.assertEquals(ownerAndPermission.getGroup(), "target");
    Assert.assertEquals(ownerAndPermission.getFsPermission(), null);

    ownerAndPermission = CopyableFile.resolveReplicatedOwnerAndPermission(fs, path,
        new CopyConfiguration.CopyConfigurationBuilder(fs, properties).targetGroup(Optional.of("target")).
            preserve(PreserveAttributes.fromMnemonicString("ug")).build());
    Assert.assertEquals(ownerAndPermission.getOwner(), "owner");
    Assert.assertEquals(ownerAndPermission.getGroup(), "target");
    Assert.assertEquals(ownerAndPermission.getFsPermission(), null);

    ownerAndPermission = CopyableFile.resolveReplicatedOwnerAndPermission(fs, path,
        new CopyConfiguration.CopyConfigurationBuilder(fs, properties).preserve(PreserveAttributes.fromMnemonicString("ug")).build());
    Assert.assertEquals(ownerAndPermission.getOwner(), "owner");
    Assert.assertEquals(ownerAndPermission.getGroup(), "group");
    Assert.assertEquals(ownerAndPermission.getFsPermission(), null);

    ownerAndPermission = CopyableFile.resolveReplicatedOwnerAndPermission(fs, path,
        new CopyConfiguration.CopyConfigurationBuilder(fs, properties).preserve(PreserveAttributes.fromMnemonicString("ugp")).build());
    Assert.assertEquals(ownerAndPermission.getOwner(), "owner");
    Assert.assertEquals(ownerAndPermission.getGroup(), "group");
    Assert.assertEquals(ownerAndPermission.getFsPermission(), FsPermission.getDefault());

  }
}
