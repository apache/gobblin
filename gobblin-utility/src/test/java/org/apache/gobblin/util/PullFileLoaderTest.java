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

package gobblin.util;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;


public class PullFileLoaderTest {

  private final Path basePath;
  private final PullFileLoader loader;

  public PullFileLoaderTest() throws Exception {
    this.basePath = new Path(this.getClass().getClassLoader().getResource("pullFileLoaderTest").getFile());
    this.loader = new PullFileLoader(this.basePath, FileSystem.getLocal(new Configuration()),
        PullFileLoader.DEFAULT_JAVA_PROPS_PULL_FILE_EXTENSIONS, PullFileLoader.DEFAULT_HOCON_PULL_FILE_EXTENSIONS);
  }

  @Test
  public void testSimpleJobLoading() throws Exception {
    Path path;
    Config pullFile;

    path = new Path(this.basePath, "ajob.pull");
    pullFile = loader.loadPullFile(path, ConfigFactory.empty(), false);
    Assert.assertEquals(pullFile.getString("key2"), "aValue");
    Assert.assertEquals(pullFile.getString("key10"), "aValue");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 3);

    path = new Path(this.basePath, "dir1/job.pull");
    pullFile = loader.loadPullFile(path, ConfigFactory.empty(), false);
    Assert.assertEquals(pullFile.getString("key1"), "jobValue1,jobValue2,jobValue3");
    Assert.assertEquals(pullFile.getString("key2"), "jobValue2");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 3);

    path = new Path(this.basePath, "dir1/job.conf");
    pullFile = loader.loadPullFile(path, ConfigFactory.empty(), false);
    Assert.assertEquals(pullFile.getString("key1"), "jobValue1,jobValue2,jobValue3");
    Assert.assertEquals(pullFile.getString("key2"), "jobValue2");
    Assert.assertEquals(pullFile.getString("key10"), "jobValue2");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 4);
  }

  @Test
  public void testJobLoadingWithSysProps() throws Exception {
    Path path;
    Config pullFile;

    Properties sysProps = new Properties();
    sysProps.put("key1", "sysProps1");

    path = new Path(this.basePath, "ajob.pull");
    pullFile = loader.loadPullFile(path, ConfigUtils.propertiesToConfig(sysProps), false);
    Assert.assertEquals(pullFile.getString("key1"), "sysProps1");
    Assert.assertEquals(pullFile.getString("key2"), "aValue");
    Assert.assertEquals(pullFile.getString("key10"), "aValue");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 4);

    path = new Path(this.basePath, "dir1/job.pull");
    pullFile = loader.loadPullFile(path, ConfigUtils.propertiesToConfig(sysProps), false);
    Assert.assertEquals(pullFile.getString("key1"), "jobValue1,jobValue2,jobValue3");
    Assert.assertEquals(pullFile.getString("key2"), "jobValue2");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 3);

    path = new Path(this.basePath, "dir1/job.conf");
    pullFile = loader.loadPullFile(path, ConfigFactory.empty(), false);
    Assert.assertEquals(pullFile.getString("key1"), "jobValue1,jobValue2,jobValue3");
    Assert.assertEquals(pullFile.getString("key2"), "jobValue2");
    Assert.assertEquals(pullFile.getString("key10"), "jobValue2");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 4);
  }

  @Test
  public void testRecursiveJobLoading() throws Exception {
    Path path;
    Config pullFile;

    Properties sysProps = new Properties();
    sysProps.put("key1", "sysProps1");
    Collection<Config> configs =
        loader.loadPullFilesRecursively(this.basePath, ConfigUtils.propertiesToConfig(sysProps), false);

    path = new Path(this.basePath, "ajob.pull");
    pullFile = pullFileFromPath(configs, path);
    Assert.assertEquals(pullFile.getString("key1"), "sysProps1");
    Assert.assertEquals(pullFile.getString("key2"), "aValue");
    Assert.assertEquals(pullFile.getString("key10"), "aValue");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 4);

    path = new Path(this.basePath, "dir1/job.pull");
    pullFile = pullFileFromPath(configs, path);
    Assert.assertEquals(pullFile.getString("key1"), "jobValue1,jobValue2,jobValue3");
    Assert.assertEquals(pullFile.getString("key2"), "jobValue2");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 3);

    path = new Path(this.basePath, "dir1/job.conf");
    pullFile = pullFileFromPath(configs, path);
    Assert.assertEquals(pullFile.getString("key1"), "jobValue1,jobValue2,jobValue3");
    Assert.assertEquals(pullFile.getString("key2"), "jobValue2");
    Assert.assertEquals(pullFile.getString("key10"), "jobValue2");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 4);
  }

  @Test
  public void testJobLoadingWithSysPropsAndGlobalProps() throws Exception {
    Path path;
    Config pullFile;

    Properties sysProps = new Properties();
    sysProps.put("key1", "sysProps1");

    path = new Path(this.basePath, "ajob.pull");
    pullFile = loader.loadPullFile(path, ConfigUtils.propertiesToConfig(sysProps), true);
    Assert.assertEquals(pullFile.getString("key1"), "rootValue1");
    Assert.assertEquals(pullFile.getString("key2"), "aValue");
    Assert.assertEquals(pullFile.getString("key10"), "aValue");
    Assert.assertEquals(pullFile.getString("key3"), "rootValue3");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 5);

    path = new Path(this.basePath, "dir1/job.pull");
    pullFile = loader.loadPullFile(path, ConfigUtils.propertiesToConfig(sysProps), true);
    Assert.assertEquals(pullFile.getString("key1"), "jobValue1,jobValue2,jobValue3");
    Assert.assertEquals(pullFile.getString("key2"), "jobValue2");
    Assert.assertEquals(pullFile.getString("key3"), "rootValue3");
    Assert.assertEquals(pullFile.getString("key4"), "dir1Value4");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 5);

    path = new Path(this.basePath, "dir1/job.conf");
    pullFile = loader.loadPullFile(path, ConfigFactory.empty(), true);
    Assert.assertEquals(pullFile.getString("key1"), "jobValue1,jobValue2,jobValue3");
    Assert.assertEquals(pullFile.getString("key2"), "dir1Value4");
    Assert.assertEquals(pullFile.getString("key3"), "rootValue3");
    Assert.assertEquals(pullFile.getString("key4"), "dir1Value4");
    Assert.assertEquals(pullFile.getString("key10"), "jobValue2");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 6);
  }

  @Test
  public void testRecursiveJobLoadingWithAndGlobalProps() throws Exception {
    Path path;
    Config pullFile;

    Properties sysProps = new Properties();
    sysProps.put("key1", "sysProps1");
    Collection<Config> configs =
        loader.loadPullFilesRecursively(this.basePath, ConfigUtils.propertiesToConfig(sysProps), true);

    path = new Path(this.basePath, "ajob.pull");
    pullFile = pullFileFromPath(configs, path);
    Assert.assertEquals(pullFile.getString("key1"), "rootValue1");
    Assert.assertEquals(pullFile.getString("key2"), "aValue");
    Assert.assertEquals(pullFile.getString("key10"), "aValue");
    Assert.assertEquals(pullFile.getString("key3"), "rootValue3");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 5);

    path = new Path(this.basePath, "dir1/job.pull");
    pullFile = pullFileFromPath(configs, path);
    Assert.assertEquals(pullFile.getString("key1"), "jobValue1,jobValue2,jobValue3");
    Assert.assertEquals(pullFile.getString("key2"), "jobValue2");
    Assert.assertEquals(pullFile.getString("key3"), "rootValue3");
    Assert.assertEquals(pullFile.getString("key4"), "dir1Value4");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 5);

    path = new Path(this.basePath, "dir1/job.conf");
    pullFile = pullFileFromPath(configs, path);
    Assert.assertEquals(pullFile.getString("key1"), "jobValue1,jobValue2,jobValue3");
    Assert.assertEquals(pullFile.getString("key2"), "dir1Value4");
    Assert.assertEquals(pullFile.getString("key3"), "rootValue3");
    Assert.assertEquals(pullFile.getString("key4"), "dir1Value4");
    Assert.assertEquals(pullFile.getString("key10"), "jobValue2");
    Assert.assertEquals(pullFile.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY), path.toString());
    Assert.assertEquals(pullFile.entrySet().size(), 6);
  }


  private Config pullFileFromPath(Collection<Config> configs, Path path) throws IOException {
    for (Config config : configs) {
      if (config.getString(ConfigurationKeys.JOB_CONFIG_FILE_PATH_KEY).equals(path.toString())) {
        return config;
      }
    }
    throw new IOException("Not found.");
  }

}
