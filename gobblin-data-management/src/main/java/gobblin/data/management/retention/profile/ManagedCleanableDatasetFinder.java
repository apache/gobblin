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

package gobblin.data.management.retention.profile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import gobblin.config.client.ConfigClient;
import gobblin.config.client.ConfigClientCache;
import gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.config.store.api.VersionDoesNotExistException;
import gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;
import gobblin.data.management.retention.version.DatasetVersion;


/**
 * A {@link ConfigurableGlobDatasetFinder} backed by gobblin-config-management. It uses {@link ConfigClient} to get dataset configs
 */
public class ManagedCleanableDatasetFinder
    extends ConfigurableGlobDatasetFinder<ConfigurableCleanableDataset<DatasetVersion>> {

  public static final String CONFIG_MANAGEMENT_STORE_URI = "gobblin.config.management.store.uri";

  private final ConfigClient client;

  public ManagedCleanableDatasetFinder(FileSystem fs, Properties jobProps, Config config) throws IOException {
    this(fs, jobProps, config, ConfigClientCache.getClient(VersionStabilityPolicy.STRONG_LOCAL_STABILITY));
  }

  public ManagedCleanableDatasetFinder(FileSystem fs, Properties jobProps, Config config, ConfigClient client)
      throws IOException {
    super(fs, jobProps, config);
    this.client = client;
  }

  @Override
  public ConfigurableCleanableDataset<DatasetVersion> datasetAtPath(Path path) throws IOException {
    try {
      return new ConfigurableCleanableDataset<>(this.fs, this.props, path,
          this.client.getConfig(this.props.getProperty(CONFIG_MANAGEMENT_STORE_URI) + path.toString()),
          LoggerFactory.getLogger(ConfigurableCleanableDataset.class));
    } catch (VersionDoesNotExistException | ConfigStoreFactoryDoesNotExistsException | ConfigStoreCreationException
        | URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
