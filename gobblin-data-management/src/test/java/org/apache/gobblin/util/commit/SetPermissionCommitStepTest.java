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

package org.apache.gobblin.util.commit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.data.management.copy.OwnerAndPermission;


/**
 * Test for {@link SetPermissionCommitStep}.
 */
@Test(groups = { "gobblin.commit" })
public class SetPermissionCommitStepTest {
  private static final String ROOT_DIR = "set-permission-commit-step-test";

  private FileSystem fs;
  private SetPermissionCommitStep step;
  Path dir1;
  FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

  @BeforeClass
  public void setUp() throws IOException {
    this.fs = FileSystem.getLocal(new Configuration());
    this.fs.delete(new Path(ROOT_DIR), true);

    dir1 = new Path(ROOT_DIR, "dir1");
    this.fs.mkdirs(dir1);

    OwnerAndPermission ownerAndPermission = new OwnerAndPermission("owner", "group", permission);
    Map<String, OwnerAndPermission> pathAndPermissions = new HashMap<>();
    pathAndPermissions.put(dir1.toString(), ownerAndPermission);

    this.step = new SetPermissionCommitStep(this.fs, pathAndPermissions, new Properties());
  }

  @AfterClass
  public void tearDown() throws IOException {
    this.fs.delete(new Path(ROOT_DIR), true);
  }

  @Test
  public void testExecute() throws IOException {
    Assert.assertNotEquals(this.fs.getFileStatus(dir1).getPermission(), permission);
    this.step.execute();
    Assert.assertEquals(this.fs.getFileStatus(dir1).getPermission(), permission);
  }
}
