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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;

import gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.config.store.deploy.ClasspathConfigSource;
import gobblin.config.store.deploy.FsDeploymentConfig;
import gobblin.util.ConfigUtils;
import gobblin.util.PathUtils;


/**
 * Unit tests for {@link SimpleHadoopFilesystemConfigStore} and {@link SimpleHDFSConfigStoreFactory}.
 */
@Test(groups = "gobblin.config.store.hdfs")
public class SimpleHdfsConfigStoreTest {

  private static final String CONFIG_DIR_NAME = "configDir";
  private static final String VERSION = "v1.0";
  private static final Path CONFIG_DIR_PATH =
      PathUtils.combinePaths(CONFIG_DIR_NAME, SimpleHadoopFilesystemConfigStore.CONFIG_STORE_NAME, VERSION);

  /**Set by {@link TestEnvironment#setup()}**/
  public static final String TAG_NAME_SYS_PROP_KEY = "sysProp.tagName1";
  public static final String TAG_NAME_SYS_PROP_VALUE = "tag1";

  public static final String TAG_NAME_RUNTIME_PROP_KEY = "runtimeProp.tagName2";
  public static final String TAG_NAME_RUNTIME_PROP_VALUE = "tag2";

  private FileSystem fs;
  private SimpleHadoopFilesystemConfigStore _simpleHadoopFilesystemConfigStore;

  @BeforeClass
  public void setUp() throws URISyntaxException, ConfigStoreCreationException, IOException {
    this.fs = FileSystem.getLocal(new Configuration());
    this.fs.mkdirs(CONFIG_DIR_PATH);

    SimpleLocalHDFSConfigStoreFactory simpleHDFSConfigStoreConfigFactory = new SimpleLocalHDFSConfigStoreFactory();

    URI storeURI = getStoreURI(System.getProperty("user.dir") + File.separator + CONFIG_DIR_NAME);
    this._simpleHadoopFilesystemConfigStore = simpleHDFSConfigStoreConfigFactory.createConfigStore(storeURI);
    this._simpleHadoopFilesystemConfigStore.deploy(new FsDeploymentConfig(new ClasspathConfigSource(new Properties()), VERSION));

  }

  @Test
  public void testGetCurrentVersion() throws IOException {

    Assert.assertEquals(this._simpleHadoopFilesystemConfigStore.getCurrentVersion(), VERSION);

    String newVersion = "v1.1";

    this._simpleHadoopFilesystemConfigStore.deploy(new FsDeploymentConfig(new ClasspathConfigSource(new Properties()), newVersion));

    Assert.assertEquals(this._simpleHadoopFilesystemConfigStore.getCurrentVersion(), newVersion);

  }

  @Test
  public void getStoreURI() {
    URI storeURI = this._simpleHadoopFilesystemConfigStore.getStoreURI();

    Assert.assertEquals(storeURI.getScheme(), SimpleHDFSConfigStoreFactory.SIMPLE_HDFS_SCHEME_PREFIX + "file");
    Assert.assertNull(storeURI.getAuthority());
    Assert.assertEquals(storeURI.getPath(), System.getProperty("user.dir") + File.separator + CONFIG_DIR_NAME);
  }

  @Test
  public void testGetChildren() throws IOException, URISyntaxException, ConfigStoreCreationException {
    String datasetName = "dataset-test-get-children";
    String childDatasetName = "childDataset";
    Path datasetPath = new Path(CONFIG_DIR_PATH, datasetName);

    try {
      this.fs.mkdirs(new Path(datasetPath, childDatasetName));

      ConfigKeyPath datasetConfigKey = SingleLinkedListConfigKeyPath.ROOT.createChild(datasetName);
      Collection<ConfigKeyPath> children = this._simpleHadoopFilesystemConfigStore.getChildren(datasetConfigKey, VERSION);

      Assert.assertEquals(children.size(), 1);
      Assert.assertEquals(children.iterator().next().getOwnPathName(), childDatasetName);
    } finally {
      if (this.fs.exists(datasetPath)) {
        this.fs.delete(datasetPath, true);
      }
    }
  }

  @Test
  public void testGetOwnImports() throws IOException, URISyntaxException, ConfigStoreCreationException {

    String datasetName = "dataset-test-get-own-imports";
    String tagKey1 = "/path/to/tag1";
    String tagKey2 = "/path/to/tag2";
    Path datasetPath = new Path(CONFIG_DIR_PATH, datasetName);

    try {
      this.fs.mkdirs(datasetPath);

      BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(this.fs.create(new Path(datasetPath, "includes.conf")), Charsets.UTF_8));

      writer.write(tagKey1);
      writer.newLine();
      writer.write(tagKey2);
      writer.close();

      ConfigKeyPath datasetConfigKey = SingleLinkedListConfigKeyPath.ROOT.createChild(datasetName);
      List<ConfigKeyPath> imports = this._simpleHadoopFilesystemConfigStore.getOwnImports(datasetConfigKey, VERSION);

      Assert.assertEquals(imports.size(), 2);
      Assert.assertEquals(imports.get(0).getAbsolutePathString(), tagKey2);
      Assert.assertEquals(imports.get(1).getAbsolutePathString(), tagKey1);
    } finally {
      if (this.fs.exists(datasetPath)) {
        this.fs.delete(datasetPath, true);
      }
    }
  }

  @Test
 public void testGetOwnImportsWithRuntimeConfigResolution() throws IOException, URISyntaxException, ConfigStoreCreationException {
    String datasetName = "dataset-test-get-own-imports-resolution";
    Path datasetPath = new Path(CONFIG_DIR_PATH, datasetName);
    Properties prop = new Properties();
    prop.put(TAG_NAME_RUNTIME_PROP_KEY, TAG_NAME_RUNTIME_PROP_VALUE);
    Optional<Config> runtimeConfig = Optional.fromNullable(ConfigUtils.propertiesToConfig(prop));
    try {
      this.fs.mkdirs(datasetPath);
      BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(this.fs.create(new Path(datasetPath, "includes.conf")), Charsets.UTF_8));
      writer.write("/path/to/${?" + TAG_NAME_RUNTIME_PROP_KEY + "}");
      writer.close();
      ConfigKeyPath datasetConfigKey = SingleLinkedListConfigKeyPath.ROOT.createChild(datasetName);
      List<ConfigKeyPath> imports = this._simpleHadoopFilesystemConfigStore.getOwnImports(datasetConfigKey, VERSION, runtimeConfig);
      Assert.assertEquals(imports.size(), 1);
      Assert.assertEquals(imports.get(0).getAbsolutePathString(), "/path/to/" + TAG_NAME_RUNTIME_PROP_VALUE);
    } finally {
      if (this.fs.exists(datasetPath)) {
        this.fs.delete(datasetPath, true);
      }
    }
  }

  @Test
  public void testGetOwnImportsWithResolution() throws IOException, URISyntaxException, ConfigStoreCreationException {

    String datasetName = "dataset-test-get-own-imports-resolution";

    Path datasetPath = new Path(CONFIG_DIR_PATH, datasetName);

    try {
      this.fs.mkdirs(datasetPath);

      BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(this.fs.create(new Path(datasetPath, "includes.conf")), Charsets.UTF_8));

      writer.write("/path/to/${?" + TAG_NAME_SYS_PROP_KEY + "}");
      writer.close();

      ConfigKeyPath datasetConfigKey = SingleLinkedListConfigKeyPath.ROOT.createChild(datasetName);
      List<ConfigKeyPath> imports = this._simpleHadoopFilesystemConfigStore.getOwnImports(datasetConfigKey, VERSION);

      Assert.assertEquals(imports.size(), 1);
      Assert.assertEquals(imports.get(0).getAbsolutePathString(), "/path/to/" + TAG_NAME_SYS_PROP_VALUE);
    } finally {
      if (this.fs.exists(datasetPath)) {
        this.fs.delete(datasetPath, true);
      }
    }
  }

  @Test
  public void testGetOwnConfig() throws ConfigStoreCreationException, URISyntaxException, IOException {
    String datasetName = "dataset-test-get-own-config";
    Path datasetPath = new Path(CONFIG_DIR_PATH, datasetName);

    try {
      this.fs.mkdirs(datasetPath);
      this.fs.create(new Path(datasetPath, "main.conf")).close();

      ConfigKeyPath datasetConfigKey = SingleLinkedListConfigKeyPath.ROOT.createChild(datasetName);
      Config config = this._simpleHadoopFilesystemConfigStore.getOwnConfig(datasetConfigKey, VERSION);

      Assert.assertTrue(config.isEmpty());
    } finally {
      if (this.fs.exists(datasetPath)) {
        this.fs.delete(datasetPath, true);
      }
    }
  }

  @Test(dependsOnMethods = { "testGetCurrentVersion" })
  public void testDeploy() throws Exception {

    Properties props = new Properties();

    props.setProperty(ClasspathConfigSource.CONFIG_STORE_CLASSPATH_RESOURCE_NAME_KEY, "_testDeploy");

    this._simpleHadoopFilesystemConfigStore.deploy(new FsDeploymentConfig(new ClasspathConfigSource(props), "2.0"));

    Path versionPath = PathUtils.combinePaths(CONFIG_DIR_NAME, SimpleHadoopFilesystemConfigStore.CONFIG_STORE_NAME, "2.0");

    Assert.assertTrue(fs.exists(new Path(versionPath, "dir1")));
    Assert.assertTrue(fs.exists(new Path(versionPath, "dir1/f1.conf")));

  }

  @Test
  public void testResolveImports() throws Exception {
    List<String> unresolved =
        ImmutableList.of("/path/to/tag0", "/path/to/${?" + TAG_NAME_SYS_PROP_KEY + "}", "${?" + TAG_NAME_SYS_PROP_KEY
            + "}/${?" + TAG_NAME_SYS_PROP_KEY + "}");

    List<String> resolved = SimpleHadoopFilesystemConfigStore.resolveIncludesList(unresolved);

    List<String> expected = ImmutableList.of("/path/to/tag0", "/path/to/tag1", "tag1/tag1");
    Assert.assertEquals(resolved, expected);
  }

  @AfterClass
  public void tearDown() throws IOException {
    if (this.fs.exists(new Path(CONFIG_DIR_NAME))) {
      this.fs.delete(new Path(CONFIG_DIR_NAME), true);
    }
  }

  private URI getStoreURI(String configDir) throws URISyntaxException {
    return new URI(SimpleHDFSConfigStoreFactory.SIMPLE_HDFS_SCHEME_PREFIX + "file", "localhost:8080", configDir, "", "");
  }
}
