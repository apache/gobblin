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
import java.util.List;
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

import com.google.common.collect.Lists;
import com.google.common.io.Files;

import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.util.filesystem.OwnerAndPermission;


/**
 * Test for {@link CreateDirectoryWithPermissionsCommitStep}.
 */
@Test(groups = { "gobblin.commit" })
public class CreateDirectoryWithPermissionsCommitStepTest {
  private static final Path ROOT_DIR = new Path(Files.createTempDir().getPath(),"create-dir-permissions-test");

  private FileSystem fs;
  @BeforeClass
  public void setUp() throws IOException {
    this.fs = FileSystem.getLocal(new Configuration());
    this.fs.delete(ROOT_DIR.getParent(), true);
    this.fs.mkdirs(ROOT_DIR);
  }

  @AfterClass
  public void tearDown() throws IOException {
    this.fs.delete(ROOT_DIR, true);
  }

  @Test
  public void testExecuteSingleLevel() throws IOException {
    Path dir1 = new Path(ROOT_DIR, "dir1");
    String owner = this.fs.getFileStatus(ROOT_DIR).getOwner();
    String group = this.fs.getFileStatus(ROOT_DIR).getGroup();
    FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(owner, group, permission);
    Map<String, List<OwnerAndPermission>> pathAndPermissions = new HashMap<>();
    pathAndPermissions.put(dir1.toString(), Lists.newArrayList(ownerAndPermission));

    CommitStep step = new CreateDirectoryWithPermissionsCommitStep(this.fs, pathAndPermissions, new Properties());
    Assert.assertFalse(this.fs.exists(dir1));
    step.execute();
    Assert.assertTrue(this.fs.exists(dir1));
    Assert.assertEquals(this.fs.getFileStatus(dir1).getPermission(), permission);
    Assert.assertEquals(this.fs.getFileStatus(dir1).getOwner(), this.fs.getFileStatus(ROOT_DIR).getOwner());
    Assert.assertEquals(this.fs.getFileStatus(dir1).getGroup(), this.fs.getFileStatus(ROOT_DIR).getGroup());
  }

  @Test
  public void testExecuteNested() throws IOException {
    Path dirNestedParent = new Path(ROOT_DIR, "dirParent");
    Path dirNestedChild = new Path(ROOT_DIR, "dirParent/dirChild");
    String owner = this.fs.getFileStatus(ROOT_DIR).getOwner();
    String group = this.fs.getFileStatus(ROOT_DIR).getGroup();
    FsPermission permissionParent = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
    OwnerAndPermission ownerAndPermissionParent = new OwnerAndPermission(owner, group, permissionParent);

    FsPermission permissionChild = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
    OwnerAndPermission ownerAndPermissionChild = new OwnerAndPermission(owner, group, permissionChild);

    Map<String, List<OwnerAndPermission>> pathAndPermissions = new HashMap<>();
    pathAndPermissions.put(dirNestedChild.toString(), Lists.newArrayList(ownerAndPermissionChild, ownerAndPermissionParent));

    CommitStep step = new CreateDirectoryWithPermissionsCommitStep(this.fs, pathAndPermissions, new Properties());
    Assert.assertFalse(this.fs.exists(dirNestedParent));
    Assert.assertFalse(this.fs.exists(dirNestedChild));

    step.execute();
    Assert.assertEquals(this.fs.exists(dirNestedChild), true);
    Assert.assertEquals(this.fs.exists(dirNestedParent), true);
    Assert.assertEquals(this.fs.getFileStatus(dirNestedChild).getPermission(), permissionChild);
    Assert.assertEquals(this.fs.getFileStatus(dirNestedChild).getOwner(), this.fs.getFileStatus(ROOT_DIR).getOwner());
    Assert.assertEquals(this.fs.getFileStatus(dirNestedChild).getGroup(), this.fs.getFileStatus(ROOT_DIR).getGroup());
    Assert.assertEquals(this.fs.getFileStatus(dirNestedParent).getPermission(), permissionParent);
  }

  @Test
  void testFailsLoudlyWhenConfigured() throws IOException{
    Path dir = new Path(ROOT_DIR, "dirFail");
    FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission("nonExistentOwner", "nonExistentGroup", fsPermission);

    Map<String, List<OwnerAndPermission>> pathAndPermissions = new HashMap<>();
    pathAndPermissions.put(dir.toString(), Lists.newArrayList(ownerAndPermission));

    CommitStep failingStep = new CreateDirectoryWithPermissionsCommitStep(this.fs, pathAndPermissions, new Properties());
    Assert.assertFalse(this.fs.exists(dir));

    Assert.assertThrows(IOException.class, failingStep::execute);

    Properties props = new Properties();
    props.setProperty(CreateDirectoryWithPermissionsCommitStep.STOP_ON_ERROR_KEY, "false");
    CommitStep passingStep = new CreateDirectoryWithPermissionsCommitStep(this.fs, pathAndPermissions, props);
    passingStep.execute();
    Assert.assertTrue(this.fs.exists(dir));
    // Set owner will fail silently due to not being configured
    Assert.assertNotEquals(this.fs.getFileStatus(dir).getOwner(), "nonExistentOwner");
  }

}
