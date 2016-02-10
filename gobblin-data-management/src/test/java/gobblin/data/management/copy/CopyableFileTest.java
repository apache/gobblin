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
package gobblin.data.management.copy;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import gobblin.util.PathUtils;

public class CopyableFileTest {

  @Test
  public void testSerializeDeserialze() throws Exception {

    CopyableFile copyableFile =
        new CopyableFile(new FileStatus(10, false, 12, 100, 12345, new Path("/path")), new Path("/destination"),
            new Path("/relative"), new OwnerAndPermission("owner", "group", FsPermission.getDefault()),
            Lists.newArrayList(new OwnerAndPermission("owner2", "group2", FsPermission.getDefault())),
            "checksum".getBytes(), PreserveAttributes.fromMnemonicString(""), "", 0, 0);

    String s = CopyableFile.serialize(copyableFile);
    CopyableFile de = CopyableFile.deserialize(s);

    Assert.assertEquals(de, copyableFile);
  }

  @Test
  public void testSerializeDeserialzeNulls() throws Exception {

    CopyableFile copyableFile =
        new CopyableFile(null, null, new Path("/relative"), new OwnerAndPermission("owner", "group",
            FsPermission.getDefault()), Lists.newArrayList(new OwnerAndPermission(null, "group2", FsPermission
            .getDefault())), "checksum".getBytes(), PreserveAttributes.fromMnemonicString(""), "", 0, 0);

    String serialized = CopyableFile.serialize(copyableFile);
    CopyableFile deserialized = CopyableFile.deserialize(serialized);

    Assert.assertEquals(deserialized, copyableFile);

  }

  @Test
  public void testSerializeDeserialzeList() throws Exception {

    List<CopyableFile> copyableFiles =
        ImmutableList.of(CopyableFileUtils.getTestCopyableFile(), CopyableFileUtils.getTestCopyableFile(),
            CopyableFileUtils.getTestCopyableFile());

    String serialized = CopyableFile.serializeList(copyableFiles);
    List<CopyableFile> deserialized = CopyableFile.deserializeList(serialized);

    Assert.assertEquals(deserialized, copyableFiles);

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

    CopyConfiguration copyConfiguration =
        CopyConfiguration.builder().targetRoot(new Path(targetRoot)).preserve(preserveAttributes).build();

    CopyableFile copyableFile = CopyableFile.builder(originFS, origin, datasetRoot, copyConfiguration)
        .build();

    // Making sure all fields are populated correctly via CopyableFile builder

    // Verify preserve attribute options
    Assert.assertEquals(copyableFile.getPreserve().toMnemonicString(), preserveAttributes.toMnemonicString());

    // Verify origin
    Assert.assertEquals(copyableFile.getFileSet(), datasetRootDir);
    Assert.assertEquals(copyableFile.getOrigin(), origin);

    // Verify destination target, permissions and other attributes
    Assert.assertEquals(copyableFile.getChecksum().length, 0);
    Assert.assertEquals(copyableFile.getDestination().toString(), targetPath.toString());
    Assert.assertEquals(copyableFile.getRelativeDestination().toString(), relativePath.toString());
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

    CopyConfiguration copyConfiguration =
        CopyConfiguration.builder().targetRoot(new Path(targetRoot)).preserve(preserveAttributes).build();

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
        .relativeDestination(relativePath)
        .destination(targetPath).build();

    // Verify preserve attribute options
    Assert.assertEquals(copyableFile.getPreserve().toMnemonicString(), preserveAttributes.toMnemonicString());

    // Verify origin
    Assert.assertEquals(copyableFile.getFileSet(), fileSet);
    Assert.assertEquals(copyableFile.getOrigin(), origin);

    // Verify destination target, permissions and other attributes
    Assert.assertEquals(copyableFile.getChecksum().length, 1);
    Assert.assertEquals(copyableFile.getDestination().toString(), targetPath.toString());
    Assert.assertEquals(copyableFile.getRelativeDestination().toString(), relativePath.toString());
    Assert.assertEquals(copyableFile.getDestinationOwnerAndPermission().getGroup(), ownerAndPermission.getGroup());
    Assert.assertEquals(copyableFile.getDestinationOwnerAndPermission().getOwner(), ownerAndPermission.getOwner());
    Assert.assertEquals(copyableFile.getDestinationOwnerAndPermission().getFsPermission(),
        ownerAndPermission.getFsPermission());

    // Verify auto determined timestamp
    Assert.assertEquals(copyableFile.getOriginTimestamp(), originTimestamp);
    Assert.assertEquals(copyableFile.getUpstreamTimestamp(), upstreamTimestamp);
  }
}
