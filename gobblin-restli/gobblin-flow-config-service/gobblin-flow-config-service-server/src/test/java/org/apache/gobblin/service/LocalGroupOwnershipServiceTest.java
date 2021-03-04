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

package org.apache.gobblin.service;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.typesafe.config.Config;

import org.apache.gobblin.config.ConfigBuilder;


@Test(groups = { "gobblin.service" })
public class LocalGroupOwnershipServiceTest {
  private File _testDirectory;
  private GroupOwnershipService groupOwnershipService;
  private File groupConfigFile;

  @BeforeClass
  public void setUp() throws Exception {
    _testDirectory = Files.createTempDir();

    this.groupConfigFile = new File(_testDirectory + "/TestGroups.json");
    String groups ="{\"testGroup\": \"testName,testName2\"}";
    Files.write(groups.getBytes(), this.groupConfigFile);
    Config groupServiceConfig = ConfigBuilder.create()
        .addPrimitive(LocalGroupOwnershipService.GROUP_MEMBER_LIST, this.groupConfigFile.getAbsolutePath())
        .build();

    groupOwnershipService = new LocalGroupOwnershipService(groupServiceConfig);
  }

  @Test
  public void testLocalGroupOwnershipUpdates() throws Exception {
    List<ServiceRequester> testRequester = new ArrayList<>();
    testRequester.add(new ServiceRequester("testName", "USER_PRINCIPAL", "testFrom"));

    Assert.assertFalse(this.groupOwnershipService.isMemberOfGroup(testRequester, "testGroup2"));

    String filePath = this.groupConfigFile.getAbsolutePath();
    this.groupConfigFile.delete();
    this.groupConfigFile = new File(filePath);
    String groups ="{\"testGroup2\": \"testName,testName3\"}";
    Files.write(groups.getBytes(), this.groupConfigFile);

    // this should return true now as the localGroupOwnership service should have updated as the file changed
    Assert.assertTrue(this.groupOwnershipService.isMemberOfGroup(testRequester, "testGroup2"));
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    _testDirectory.delete();
  }
}
