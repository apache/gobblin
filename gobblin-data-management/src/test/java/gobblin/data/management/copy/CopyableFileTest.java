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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Lists;


public class CopyableFileTest {

  @Test public void testSerDe() throws Exception {

    CopyableFile copyableFile = new CopyableFile(new FileStatus(10, false, 12, 100, 12345, new Path("/path")),
        new Path("/destination"), new OwnerAndPermission("owner", "group", FsPermission.getDefault()),
        Lists.newArrayList(new OwnerAndPermission("owner2", "group2", FsPermission.getDefault())),
        "checksum".getBytes());

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    copyableFile.write(new DataOutputStream(os));
    byte[] bytes = os.toByteArray();

    ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    CopyableFile deserialized = CopyableFile.read(new DataInputStream(is));

    Assert.assertEquals(copyableFile, deserialized);

  }
}
