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

package gobblin.data.management.retention;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.policy.EmbeddedRetentionSelectionPolicy;
import gobblin.data.management.policy.NewestKSelectionPolicy;
import gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;
import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.finder.WatermarkDatasetVersionFinder;

public class ConfigurableCleanableDatasetTest {

  @Test
  public void testConfigureWithRetentionPolicy() throws Exception {

    Config conf =
        ConfigFactory.parseMap(ImmutableMap.<String, String> of("gobblin.retention.version.finder.class",
            "gobblin.data.management.version.finder.WatermarkDatasetVersionFinder",
            "gobblin.retention.retention.policy.class",
            "gobblin.data.management.retention.policy.NewestKRetentionPolicy",
            "gobblin.retention.newestK.versions.retained", "2"));

    ConfigurableCleanableDataset<FileSystemDatasetVersion> dataset =
        new ConfigurableCleanableDataset<FileSystemDatasetVersion>(FileSystem.get(new URI(ConfigurationKeys.LOCAL_FS_URI),
            new Configuration()), new Properties(), new Path("/someroot"), conf, LoggerFactory.getLogger(ConfigurableCleanableDatasetTest.class));

    Assert.assertEquals(dataset.getVersionFindersAndPolicies().get(0).getVersionSelectionPolicy().getClass(), EmbeddedRetentionSelectionPolicy.class);
    Assert.assertEquals(dataset.getVersionFindersAndPolicies().get(0).getVersionFinder().getClass(), WatermarkDatasetVersionFinder.class);
    Assert.assertEquals(dataset.isDatasetBlacklisted(), false);
  }

  @Test
  public void testConfigureWithSelectionPolicy() throws Exception {

    Config conf =
        ConfigFactory.parseMap(ImmutableMap.<String, String> of("gobblin.retention.version.finder.class",
            "gobblin.data.management.version.finder.WatermarkDatasetVersionFinder",
            "gobblin.retention.selection.policy.class",
            "gobblin.data.management.policy.NewestKSelectionPolicy",
            "gobblin.retention.selection.newestK.versionsSelected", "2"));

    ConfigurableCleanableDataset<FileSystemDatasetVersion> dataset =
        new ConfigurableCleanableDataset<FileSystemDatasetVersion>(FileSystem.get(new URI(ConfigurationKeys.LOCAL_FS_URI),
            new Configuration()), new Properties(), new Path("/someroot"), conf, LoggerFactory.getLogger(ConfigurableCleanableDatasetTest.class));

    Assert.assertEquals(dataset.getVersionFindersAndPolicies().get(0).getVersionSelectionPolicy().getClass(), NewestKSelectionPolicy.class);
    Assert.assertEquals(dataset.getVersionFindersAndPolicies().get(0).getVersionFinder().getClass(), WatermarkDatasetVersionFinder.class);
    Assert.assertEquals(dataset.isDatasetBlacklisted(), false);
  }

  @Test
  public void testConfigureWithMulitplePolicies() throws Exception {

    Map<String, String> partitionConf =
        ImmutableMap.<String, String> of("version.finder.class",
            "gobblin.data.management.version.finder.WatermarkDatasetVersionFinder", "selection.policy.class",
            "gobblin.data.management.policy.NewestKSelectionPolicy", "selection.newestK.versionsSelected", "2");
    Config conf = ConfigFactory.parseMap(ImmutableMap.<String, List<Map<String, String>>> of("gobblin.retention.dataset.partitions",
            ImmutableList.of(partitionConf, partitionConf)));

    ConfigurableCleanableDataset<FileSystemDatasetVersion> dataset =
        new ConfigurableCleanableDataset<FileSystemDatasetVersion>(FileSystem.get(new URI(ConfigurationKeys.LOCAL_FS_URI),
            new Configuration()), new Properties(), new Path("/someroot"), conf, LoggerFactory.getLogger(ConfigurableCleanableDatasetTest.class));

    Assert.assertEquals(dataset.getVersionFindersAndPolicies().get(0).getVersionSelectionPolicy().getClass(), NewestKSelectionPolicy.class);
    Assert.assertEquals(dataset.getVersionFindersAndPolicies().get(0).getVersionFinder().getClass(), WatermarkDatasetVersionFinder.class);

    Assert.assertEquals(dataset.getVersionFindersAndPolicies().get(1).getVersionSelectionPolicy().getClass(), NewestKSelectionPolicy.class);
    Assert.assertEquals(dataset.getVersionFindersAndPolicies().get(1).getVersionFinder().getClass(), WatermarkDatasetVersionFinder.class);
    Assert.assertEquals(dataset.isDatasetBlacklisted(), false);
  }

  @Test
  public void testDatasetIsBlacklisted() throws Exception {

    Config conf =
        ConfigFactory.parseMap(ImmutableMap.<String, String> of("gobblin.retention.version.finder.class",
            "gobblin.data.management.version.finder.WatermarkDatasetVersionFinder",
            "gobblin.retention.selection.policy.class",
            "gobblin.data.management.policy.NewestKSelectionPolicy",
            "gobblin.retention.selection.newestK.versionsSelected", "2",
            "gobblin.retention.dataset.is.blacklisted", "true"));

    ConfigurableCleanableDataset<FileSystemDatasetVersion> dataset =
        new ConfigurableCleanableDataset<FileSystemDatasetVersion>(FileSystem.get(new URI(ConfigurationKeys.LOCAL_FS_URI),
            new Configuration()), new Properties(), new Path("/someroot"), conf, LoggerFactory.getLogger(ConfigurableCleanableDatasetTest.class));

    Assert.assertEquals(dataset.isDatasetBlacklisted(), true);
  }
}
