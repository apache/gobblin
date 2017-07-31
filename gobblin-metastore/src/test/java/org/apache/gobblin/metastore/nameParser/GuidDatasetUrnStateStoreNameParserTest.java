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

package org.apache.gobblin.metastore.nameParser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.apache.gobblin.util.guid.Guid;


/**
 * Test for {@link GuidDatasetUrnStateStoreNameParser}.
 */
public class GuidDatasetUrnStateStoreNameParserTest {

  Path jobStateRootDir;
  FileSystem testFs;

  @BeforeTest
  public void setUp()
      throws IOException {
    this.jobStateRootDir = new Path("testStateStoreParser");
    this.testFs = FileSystem.getLocal(new Configuration());
  }

  @Test
  public void testPersistDatasetUrns()
      throws IOException {
    GuidDatasetUrnStateStoreNameParser parser =
        new GuidDatasetUrnStateStoreNameParser(this.testFs, this.jobStateRootDir);
    parser.persistDatasetUrns(Lists.newArrayList("dataset1", "dataset2"));
    Assert.assertTrue(this.testFs.exists(new Path(jobStateRootDir,
        GuidDatasetUrnStateStoreNameParser.StateStoreNameVersion.V1.getDatasetUrnNameMapFile())));
  }

  @Test(dependsOnMethods = {"testPersistDatasetUrns"})
  public void testGetDatasetUrnFromStateStoreName()
      throws IOException {
    GuidDatasetUrnStateStoreNameParser parser =
        new GuidDatasetUrnStateStoreNameParser(this.testFs, this.jobStateRootDir);
    Assert.assertEquals(parser.sanitizedNameToDatasetURNMap.size(), 2);
    Assert.assertTrue(parser.sanitizedNameToDatasetURNMap.inverse().containsKey("dataset1"));
    Assert.assertTrue(parser.sanitizedNameToDatasetURNMap.inverse().containsKey("dataset2"));
    Assert.assertEquals(parser.getStateStoreNameFromDatasetUrn("dataset1"), Guid.fromStrings("dataset1").toString());
    Assert.assertEquals(parser.getStateStoreNameFromDatasetUrn("dataset2"), Guid.fromStrings("dataset2").toString());
  }

  @AfterTest
  public void cleanUp()
      throws IOException {
    if (this.testFs.exists(this.jobStateRootDir)) {
      this.testFs.delete(this.jobStateRootDir, true);
    }
  }
}
