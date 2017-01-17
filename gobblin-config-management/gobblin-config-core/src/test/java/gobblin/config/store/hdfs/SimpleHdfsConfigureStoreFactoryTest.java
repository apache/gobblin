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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import gobblin.config.store.api.ConfigStoreCreationException;


/**
 * Unit tests for {@link SimpleHDFSConfigStoreFactory}.
 */
@Test(groups = "gobblin.config.store.hdfs", singleThreaded=true)
public class SimpleHdfsConfigureStoreFactoryTest {

  @Test
  public void testGetDefaults() throws URISyntaxException, ConfigStoreCreationException, IOException {
    Path configStoreDir = new Path(SimpleHDFSConfigStore.CONFIG_STORE_NAME);
    FileSystem localFS = FileSystem.getLocal(new Configuration());

    try {
      Assert.assertTrue(localFS.mkdirs(configStoreDir));

      SimpleLocalHDFSConfigStoreFactory simpleLocalHDFSConfigStoreFactory =
          new SimpleLocalHDFSConfigStoreFactory();

      URI configKey = new URI(simpleLocalHDFSConfigStoreFactory.getScheme(), "", "", "", "");
      SimpleHDFSConfigStore simpleHDFSConfigStore = simpleLocalHDFSConfigStoreFactory.createConfigStore(configKey);

      Assert
          .assertEquals(simpleHDFSConfigStore.getStoreURI().getScheme(), simpleLocalHDFSConfigStoreFactory.getScheme());
      Assert.assertNull(simpleHDFSConfigStore.getStoreURI().getAuthority());
      Assert.assertEquals(simpleHDFSConfigStore.getStoreURI().getPath(), System.getProperty("user.dir"));
    } finally {
      localFS.delete(configStoreDir, true);
    }
  }


  @Test
  public void testConfiguration() throws Exception {
    FileSystem localFS = FileSystem.getLocal(new Configuration());
    Path testRoot = localFS.makeQualified(new Path("testConfiguration"));
    Path configRoot = localFS.makeQualified(new Path(testRoot, "dir2"));
    Path configStoreRoot = new Path(configRoot,
                                    SimpleHDFSConfigStore.CONFIG_STORE_NAME);
    Assert.assertTrue(localFS.mkdirs(configStoreRoot));
    try {
      Config confConf1 =
          ConfigFactory.empty().withValue(SimpleHDFSConfigStoreFactory.DEFAULT_STORE_URI_KEY,
                                          ConfigValueFactory.fromAnyRef(configRoot.toString()));
      SimpleHDFSConfigStoreFactory confFactory = new SimpleHDFSConfigStoreFactory(confConf1);
      Assert.assertTrue(confFactory.hasDefaultStoreURI());
      Assert.assertEquals(confFactory.getDefaultStoreURI(), configRoot.toUri());
      Assert.assertEquals(confFactory.getPhysicalScheme(), "file");
      Assert.assertEquals(confFactory.getDefaultRootDir().toString(),
                          "file:" + System.getProperty("user.home"));

      // Valid path
      SimpleHDFSConfigStore store1 = confFactory.createConfigStore(new URI("simple-file:/d"));
      Assert.assertEquals(store1.getStoreURI().getScheme(), confFactory.getScheme());
      Assert.assertEquals(store1.getStoreURI().getAuthority(),
                          confFactory.getDefaultStoreURI().getAuthority());
      Assert.assertEquals(store1.getStoreURI().getPath(),
                          confFactory.getDefaultStoreURI().getPath());

      // Invalid path
      Config confConf2 =
          ConfigFactory.empty().withValue(SimpleHDFSConfigStoreFactory.DEFAULT_STORE_URI_KEY,
                                          ConfigValueFactory.fromAnyRef(testRoot.toString()));
      try {
        new SimpleHDFSConfigStoreFactory(confConf2);
        Assert.fail("Exception expected");
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("Path does not appear to be a config store root"));
      }

      // Empty path
      Config confConf3 =
          ConfigFactory.empty().withValue(SimpleHDFSConfigStoreFactory.DEFAULT_STORE_URI_KEY,
                                          ConfigValueFactory.fromAnyRef(""));
      try {
        new SimpleHDFSConfigStoreFactory(confConf3);
        Assert.fail("Exception expected");
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("Default store URI should be non-empty"));
      }
    }
    finally {
      localFS.delete(testRoot, true);
    }
  }
}
