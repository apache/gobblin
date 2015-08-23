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

package gobblin.data.management.trash;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestTrashTest {

  @Test
  public void test() throws IOException {
    FileSystem fs = mock(FileSystem.class);

    TestTrash trash = new TestTrash(fs, new Properties(), "user");

    Path path1 = new Path("/some/path");
    Path path2 = new Path("/some/other/path");

    Assert.assertTrue(trash.moveToTrash(path1));
    Assert.assertTrue(trash.moveToTrashAsOwner(path2));

    System.out.println(Arrays.toString(trash.getDeleteOperations().toArray()));

    Assert.assertEquals(trash.getDeleteOperations().size(), 2);
    Assert.assertTrue(trash.getDeleteOperations().get(0).getPath().equals(path1));
    Assert.assertNull(trash.getDeleteOperations().get(0).getUser());
    Assert.assertTrue(trash.getDeleteOperations().get(1).getPath().equals(path2));
    Assert.assertTrue(trash.getDeleteOperations().get(1).getUser().equals("user"));

  }

}
