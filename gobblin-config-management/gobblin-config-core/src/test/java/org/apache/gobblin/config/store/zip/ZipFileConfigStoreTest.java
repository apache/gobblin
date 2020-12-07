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
package org.apache.gobblin.config.store.zip;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.sun.nio.zipfs.ZipFileSystem;
import com.typesafe.config.Config;

import org.apache.gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import org.apache.gobblin.config.store.api.ConfigKeyPath;
import org.apache.gobblin.config.store.api.ConfigStoreCreationException;


/**
 * Unit tests for {@link ZipFileConfigStore}
 */
@Test
public class ZipFileConfigStoreTest {

  private ZipFileConfigStore store;
  private String version = "testVersion";
  private ConfigKeyPath rootPath = SingleLinkedListConfigKeyPath.ROOT;
  private ConfigKeyPath testPath = rootPath.createChild("test");
  private ConfigKeyPath child1Path = testPath.createChild("child1");
  private ConfigKeyPath child2Path = testPath.createChild("child2");

  /**
   * Layout of testing config store:
   * /_CONFIG_STORE
   *        /test
   *            /child1
   *                main.conf (gobblin.test.property = "string2")
   *                includes.conf (test/child1)
   *            /child2
   *                main.conf (gobblin.test.property = "string3")
   *        main.conf
   */
  @BeforeClass
  public void setUp() throws URISyntaxException, ConfigStoreCreationException, IOException {
    Path path = Paths.get(this.getClass().getClassLoader().getResource("zipStoreTest.zip").getPath());
    FileSystem fs = FileSystems.newFileSystem(path, null);

    this.store = new ZipFileConfigStore((ZipFileSystem) fs, path.toUri(), this.version, "_CONFIG_STORE");
  }

  @Test
  public void testGetOwnConfig() {
    Config config1 = this.store.getOwnConfig(this.rootPath, this.version);
    Assert.assertEquals(config1.getString("gobblin.property.test1"), "prop1");
    Assert.assertEquals(config1.getString("gobblin.property.test2"), "prop2");

    Config config2 = this.store.getOwnConfig(this.testPath, this.version);
    Assert.assertEquals(config2.getString("gobblin.test.property"), "string1");

    Config config3 = this.store.getOwnConfig(this.child1Path, this.version);
    Assert.assertEquals(config3.getString("gobblin.test.property"), "string2");

    Config config4 = this.store.getOwnConfig(this.child2Path, this.version);
    Assert.assertEquals(config4.getString("gobblin.test.property"), "string3");

  }

  @Test
  public void testGetOwnImports() {
    Collection<ConfigKeyPath> imports1 = this.store.getOwnImports(this.child1Path, this.version);
    Assert.assertEquals(imports1.size(), 1);
    Assert.assertTrue(imports1.contains(this.child1Path));

    Collection<ConfigKeyPath> imports2 = this.store.getOwnImports(this.child2Path, this.version);
    Assert.assertEquals(imports2.size(), 0);
  }

  @Test
  public void testGetChildren() {
    Collection<ConfigKeyPath> children1 = this.store.getChildren(this.rootPath, this.version);
    Assert.assertEquals(children1.size(), 1);
    Assert.assertTrue(children1.contains(this.testPath));

    Collection<ConfigKeyPath> children2 = this.store.getChildren(this.testPath, this.version);
    Assert.assertEquals(children2.size(), 2);
    Assert.assertTrue(children2.contains(this.child1Path));
    Assert.assertTrue(children2.contains(this.child2Path));

    Collection<ConfigKeyPath> children3 = this.store.getChildren(this.child1Path, this.version);
    Assert.assertEquals(children3.size(), 0);
  }
}
