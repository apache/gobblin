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

import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


public class CopyableFileTest {

  @Test
  public void testSerializeDeserialze() throws Exception {

    CopyableFile copyableFile =
        new CopyableFile(new FileStatus(10, false, 12, 100, 12345, new Path("/path")), new Path("/destination"),
            new Path("/relative"), new OwnerAndPermission("owner", "group", FsPermission.getDefault()),
            Lists.newArrayList(new OwnerAndPermission("owner2", "group2", FsPermission.getDefault())),
            "checksum".getBytes());

    String s = CopyableFile.serialize(copyableFile);
    CopyableFile de = CopyableFile.deserialize(s);

    Assert.assertEquals(de, copyableFile);
  }

  @Test
  public void testSerializeDeserialzeNulls() throws Exception {

    CopyableFile copyableFile =
        new CopyableFile(null, null, new Path("/relative"), new OwnerAndPermission("owner", "group",
            FsPermission.getDefault()), Lists.newArrayList(new OwnerAndPermission(null, "group2", FsPermission
            .getDefault())), "checksum".getBytes());

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
}
