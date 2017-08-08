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
package org.apache.gobblin.runtime.job_catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;

/**
 * Unit tests for {@link ImmutableFSJobCatalog}
 */
public class TestImmutableFSJobCatalog {

  @Test
  public void testConfigAccessor() throws Exception {
    Config sysConfig1 = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
        .put(ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY, "/tmp")
        .build());

    ImmutableFSJobCatalog.ConfigAccessor cfgAccessor1 =
        new ImmutableFSJobCatalog.ConfigAccessor(sysConfig1);

    Assert.assertEquals(cfgAccessor1.getJobConfDir(), "/tmp");
    Assert.assertEquals(cfgAccessor1.getJobConfDirPath(), new Path("/tmp"));
    Assert.assertEquals(cfgAccessor1.getJobConfDirFileSystem().getClass(),
        FileSystem.get(new Configuration()).getClass());
    Assert.assertEquals(cfgAccessor1.getPollingInterval(),
        ConfigurationKeys.DEFAULT_JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL);

    Config sysConfig2 = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
        .put(ConfigurationKeys.JOB_CONFIG_FILE_DIR_KEY, "/tmp2")
        .put(ConfigurationKeys.JOB_CONFIG_FILE_MONITOR_POLLING_INTERVAL_KEY, 100)
        .build());

    ImmutableFSJobCatalog.ConfigAccessor cfgAccessor2 =
        new ImmutableFSJobCatalog.ConfigAccessor(sysConfig2);

    Assert.assertEquals(cfgAccessor2.getJobConfDir(), "file:///tmp2");
    Assert.assertEquals(cfgAccessor2.getJobConfDirPath(), new Path("file:///tmp2"));
    Assert.assertTrue(cfgAccessor2.getJobConfDirFileSystem() instanceof LocalFileSystem);
    Assert.assertEquals(cfgAccessor2.getPollingInterval(), 100);

    Assert.assertThrows(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        new ImmutableFSJobCatalog.ConfigAccessor(ConfigFactory.empty());
      }
    });
  }

}
