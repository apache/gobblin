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
package gobblin.data.management.retention.profile;

import gobblin.configuration.ConfigurationKeys;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import gobblin.dataset.Dataset;
import gobblin.data.management.copy.replication.ConfigBasedDatasetsFinder;
import gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;
import gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.store.api.ConfigStoreCreationException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConfigBasedCleanabledDatasetFinder extends ConfigBasedDatasetsFinder{

  private FileSystem fileSystem;
  public static final String DATASET_PATH = ConfigurationKeys.CONFIG_BASED_PREFIX + ".fullDatasetPath";

  public ConfigBasedCleanabledDatasetFinder(FileSystem fs, Properties jobProps) throws IOException{
    super(fs, jobProps);
    fileSystem = fs;
  }

  /**
   * For all the leaf-level file found, load their configuration, create CleanableDataset and
   * add them into Collection datasets.
   *
   * Different from {@link gobblin.data.management.copy.replication.ConfigBasedCopyableDatasetFinder}, here we can only
   * serially create {@link Dataset} as race condition can be triggered with respect to trash folder access when creating
   * {@link ConfigurableCleanableDataset} object.
   *
   * @return The list of cleanable datasets
   */
  @Override
  public List<Dataset> findDatasets() throws IOException {
    Set<URI> leafDatasets = getValidDatasetURIs(this.commonRoot);
    if (leafDatasets.isEmpty()) {
      return ImmutableList.of();
    }

    // Serial execution, or trigger race condition for the trash folder.
    final List<Dataset> result = new ArrayList<>();
    for (URI validURI : leafDatasets) {
      try {
        Config c = configClient.getConfig(validURI);
        Preconditions.checkArgument(c.hasPath(DATASET_PATH), "Missing required configuration in ConfigStore obejct: fullDatasetPath");
        Path relativizedPath = new Path(c.getString(DATASET_PATH));
        result.add(
            new ConfigurableCleanableDataset<>(FileSystem.newInstance(new Configuration()), props, relativizedPath, c, log));
      } catch (ConfigStoreFactoryDoesNotExistsException | ConfigStoreCreationException e) {
        log.error("Caught error while loading ConfigStore object from given URI");
        throw new RuntimeException(e);
      }
    }

    return result;
  }
}
