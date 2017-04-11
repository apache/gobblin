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
package gobblin.config.store.hdfs;

import gobblin.configuration.ConfigurationKeys;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class SimpleHDFSStoreMetadataTest {

  private static final Path TEST_PATH = new Path("gobblin-config-management/testOutput");
  private final FileSystem localFs;

  public SimpleHDFSStoreMetadataTest() throws Exception {
    this.localFs = FileSystem.get(URI.create(ConfigurationKeys.LOCAL_FS_URI), new Configuration());
  }

  /**
   * Check if the current version of the store gets updated
   */
  @Test
  public void testVersionUpdate() throws Exception {

    Path testVersionUpdatePath = new Path(TEST_PATH, "testVersionUpdate");

    this.localFs.mkdirs(testVersionUpdatePath);

    SimpleHDFSStoreMetadata simpleHDFSStoreMetadata = new SimpleHDFSStoreMetadata(this.localFs, testVersionUpdatePath);

    simpleHDFSStoreMetadata.setCurrentVersion("1.1");

    Assert.assertEquals(simpleHDFSStoreMetadata.getCurrentVersion(), "1.1");

    simpleHDFSStoreMetadata.setCurrentVersion("1.2");

    Assert.assertEquals(simpleHDFSStoreMetadata.getCurrentVersion(), "1.2");

  }

  /**
   * Make sure update version does not affect other metadata
   */
  @Test
  public void testVersionUpdateWithOtherMetadata() throws Exception {

    Path testVersionUpdateWithOtherMetadataPath = new Path(TEST_PATH, "testVersionUpdateWithOtherMetadata");

    this.localFs.mkdirs(testVersionUpdateWithOtherMetadataPath);

    SimpleHDFSStoreMetadata simpleHDFSStoreMetadata = new SimpleHDFSStoreMetadata(this.localFs, testVersionUpdateWithOtherMetadataPath);

    Config conf = ConfigFactory.parseMap(ImmutableMap.of("test.name", "test1", "test.type", "unittest"));

    simpleHDFSStoreMetadata.writeMetadata(conf);

    simpleHDFSStoreMetadata.setCurrentVersion("1.2");

    Assert.assertEquals(simpleHDFSStoreMetadata.getCurrentVersion(), "1.2");

    Assert.assertEquals(simpleHDFSStoreMetadata.readMetadata().getString("test.name"), "test1");

    Assert.assertEquals(simpleHDFSStoreMetadata.readMetadata().getString("test.type"), "unittest");

  }

  @AfterClass
  @BeforeClass
  public void cleanup() throws Exception {
    this.localFs.delete(TEST_PATH, true);
  }
}
