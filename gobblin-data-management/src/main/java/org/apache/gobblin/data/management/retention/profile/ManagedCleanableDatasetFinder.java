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

package org.apache.gobblin.data.management.retention.profile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import org.apache.gobblin.config.client.ConfigClient;
import org.apache.gobblin.config.client.ConfigClientCache;
import org.apache.gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import org.apache.gobblin.config.client.api.VersionStabilityPolicy;
import org.apache.gobblin.config.store.api.ConfigStoreCreationException;
import org.apache.gobblin.config.store.api.VersionDoesNotExistException;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;
import org.apache.gobblin.data.management.version.FileSystemDatasetVersion;


/**
 * A {@link ConfigurableGlobDatasetFinder} backed by gobblin-config-management. It uses {@link ConfigClient} to get dataset configs
 */
public class ManagedCleanableDatasetFinder
    extends ConfigurableGlobDatasetFinder<ConfigurableCleanableDataset<FileSystemDatasetVersion>> {

  private final ConfigClient client;

  public ManagedCleanableDatasetFinder(FileSystem fs, Properties jobProps, Config config) {
    this(fs, jobProps, config, ConfigClientCache.getClient(VersionStabilityPolicy.STRONG_LOCAL_STABILITY));
  }

  public ManagedCleanableDatasetFinder(FileSystem fs, Properties jobProps, Config config, ConfigClient client) {
    super(fs, jobProps, config);
    this.client = client;
  }

  @Override
  public ConfigurableCleanableDataset<FileSystemDatasetVersion> datasetAtPath(Path path) throws IOException {
    try {
      return new ConfigurableCleanableDataset<>(this.fs, this.props, path,
          this.client
              .getConfig(this.props.getProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI) + path.toString()),
          LoggerFactory.getLogger(ConfigurableCleanableDataset.class));
    } catch (VersionDoesNotExistException | ConfigStoreFactoryDoesNotExistsException | ConfigStoreCreationException
        | URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
