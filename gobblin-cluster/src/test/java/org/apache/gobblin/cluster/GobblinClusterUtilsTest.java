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

package org.apache.gobblin.cluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.util.PathUtils;

import static org.apache.gobblin.cluster.GobblinClusterUtils.JAVA_TMP_DIR_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class GobblinClusterUtilsTest {
  private static final String TEST_APP_NAME = "appName";
  private static final String TEST_APP_ID = "appId";
  private static final String TEST_WORK_DIR = "file:///foo/bar";
  private static final String DEFAULT_HOME_DIR = "file:///home";

  @Test
  public void testGetAppWorkDirPathFromConfig() throws IOException {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    FileSystem mockFs = mock(FileSystem.class);

    when(mockFs.getHomeDirectory()).thenReturn(new Path(DEFAULT_HOME_DIR));
    when(mockFs.getUri()).thenReturn(localFs.getUri());

    //Set gobblin.cluster.workDir config
    Config config = ConfigFactory.empty().withValue(GobblinClusterConfigurationKeys.CLUSTER_WORK_DIR,
        ConfigValueFactory.fromAnyRef(TEST_WORK_DIR));
    Path workDirPath = GobblinClusterUtils.getAppWorkDirPathFromConfig(config, localFs, TEST_APP_NAME, TEST_APP_ID);

    assertEquals(PathUtils.combinePaths(TEST_WORK_DIR, TEST_APP_NAME, TEST_APP_ID), workDirPath);

    //Get workdir when gobblin.cluster.workDir is not specified
    workDirPath = GobblinClusterUtils
        .getAppWorkDirPathFromConfig(ConfigFactory.empty(), mockFs, TEST_APP_NAME, TEST_APP_ID);
    assertEquals(PathUtils.combinePaths(DEFAULT_HOME_DIR, TEST_APP_NAME, TEST_APP_ID), workDirPath);
  }

  @Test
  public void testSetSystemProperties() {
    //Set a dummy property before calling GobblinClusterUtils#setSystemProperties() and assert that this property and value
    //exists even after the call to the setSystemProperties() method.
    System.setProperty("prop1", "val1");

    Config config = ConfigFactory.empty().withValue(GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_SYSTEM_PROPERTY_PREFIX + ".prop2",
        ConfigValueFactory.fromAnyRef("val2"))
        .withValue(GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_SYSTEM_PROPERTY_PREFIX + ".prop3", ConfigValueFactory.fromAnyRef("val3"));

    GobblinClusterUtils.setSystemProperties(config);

    Assert.assertEquals(System.getProperty("prop1"), "val1");
    Assert.assertEquals(System.getProperty("prop2"), "val2");
    Assert.assertEquals(System.getProperty("prop3"), "val3");

    // Test specifically for key resolution using YARN_CACHE as the example.
    config = config.withValue(GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_SYSTEM_PROPERTY_PREFIX + "." +
        JAVA_TMP_DIR_KEY, ConfigValueFactory.fromAnyRef(GobblinClusterUtils.JVM_ARG_VALUE_RESOLVER.YARN_CACHE.name()))
        .withValue(GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_SYSTEM_PROPERTY_PREFIX + ".randomKey1",
            ConfigValueFactory.fromAnyRef(GobblinClusterUtils.JVM_ARG_VALUE_RESOLVER.YARN_CACHE.name()))
        .withValue(GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_SYSTEM_PROPERTY_PREFIX + ".randomKey2",
            ConfigValueFactory.fromAnyRef(GobblinClusterUtils.JVM_ARG_VALUE_RESOLVER.YARN_CACHE.name()))
        .withValue(GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_SYSTEM_PROPERTY_PREFIX + ".rejectedKey",
            ConfigValueFactory.fromAnyRef(GobblinClusterUtils.JVM_ARG_VALUE_RESOLVER.YARN_CACHE.name()))
        .withValue("gobblin.cluster.systemPropertiesList.YARN_CACHE", ConfigValueFactory.fromAnyRef("randomKey1,randomKey2"));
    GobblinClusterUtils.setSystemProperties(config);
    Assert.assertEquals(System.getProperty(JAVA_TMP_DIR_KEY), GobblinClusterUtils.JVM_ARG_VALUE_RESOLVER.YARN_CACHE.getResolution());
    Assert.assertEquals(System.getProperty("randomKey1"), GobblinClusterUtils.JVM_ARG_VALUE_RESOLVER.YARN_CACHE.getResolution());
    Assert.assertEquals(System.getProperty("randomKey2"), GobblinClusterUtils.JVM_ARG_VALUE_RESOLVER.YARN_CACHE.getResolution());
    // For keys not being added in the list of `gobblin.cluster.systemPropertiesList.YARN_CACHE`, the value wont'
    // be resolved.
    Assert.assertEquals(System.getProperty("rejectedKey"), GobblinClusterUtils.JVM_ARG_VALUE_RESOLVER.YARN_CACHE.name());

  }

}
