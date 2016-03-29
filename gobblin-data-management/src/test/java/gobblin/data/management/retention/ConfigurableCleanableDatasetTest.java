/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.retention;

import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;
import gobblin.data.management.retention.policy.NewestKRetentionPolicy;
import gobblin.data.management.retention.version.DatasetVersion;
import gobblin.data.management.retention.version.finder.WatermarkDatasetVersionFinder;

import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ConfigurableCleanableDatasetTest {

  @Test
  public void testConstructor() throws Exception {

    Config conf =
        ConfigFactory.parseMap(ImmutableMap.<String, String> of("gobblin.retention.version.finder.class",
            "gobblin.data.management.retention.version.finder.WatermarkDatasetVersionFinder",
            "gobblin.retention.retention.policy.class",
            "gobblin.data.management.retention.policy.NewestKRetentionPolicy",
            "gobblin.retention.newestK.versions.retained", "2"));

    ConfigurableCleanableDataset<DatasetVersion> dataset =
        new ConfigurableCleanableDataset<DatasetVersion>(FileSystem.get(new URI(ConfigurationKeys.LOCAL_FS_URI),
            new Configuration()), new Properties(), new Path("/someroot"), conf, LoggerFactory.getLogger(ConfigurableCleanableDatasetTest.class));

    Assert.assertEquals(dataset.getRetentionPolicy().getClass(), NewestKRetentionPolicy.class);
    Assert.assertEquals(dataset.getVersionFinder().getClass(), WatermarkDatasetVersionFinder.class);
  }

}
