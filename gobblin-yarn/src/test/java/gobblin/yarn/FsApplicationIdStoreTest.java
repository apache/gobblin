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

package gobblin.yarn;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link FsApplicationIdStore}.
 *
 * @author ynli
 */
@Test(groups = { "gobblin.yarn" })
public class FsApplicationIdStoreTest {

  private static final String TEST_APPLICATION_ID = "application_1445506804193_1113182";

  private FileSystem localFs;
  private Path testWorkDir;
  private ApplicationIdStore applicationIdStore;

  @BeforeClass
  public void setUp() throws IOException {
    this.localFs = FileSystem.getLocal(new Configuration());
    this.testWorkDir = new Path(FsApplicationIdStoreTest.class.getName());
    Path applicationIdFilePath = new Path(this.testWorkDir, "_application.id");
    this.applicationIdStore = new FsApplicationIdStore(this.localFs, applicationIdFilePath);
  }

  @Test
  public void testPut() throws IOException {
    this.applicationIdStore.put(TEST_APPLICATION_ID);
  }

  @Test(dependsOnMethods = "testPut")
  public void testGet() throws IOException {
    Assert.assertEquals(this.applicationIdStore.get().get(), TEST_APPLICATION_ID);
  }

  @AfterClass
  public void tearDown() throws IOException {
    try {
      this.applicationIdStore.close();
    } finally {
      if (this.localFs.exists(this.testWorkDir)) {
        this.localFs.delete(this.testWorkDir, true);
      }
    }
  }
}
