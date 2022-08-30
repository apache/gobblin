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

package org.apache.gobblin.service.monitoring;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.jgit.diff.DiffEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_store.FSSpecStore;
import org.apache.gobblin.util.PullFileLoader;


/**
 * Listener for {@link GitConfigMonitor} to apply changes from Git to a {@link FlowCatalog} for adding and removing jobs
 */
@Slf4j
public class GitConfigListener implements GitDiffListener {
  private static final int CONFIG_FILE_DEPTH = 3;
  private static final String SPEC_DESCRIPTION = "Git-based flow config";
  private static final String SPEC_VERSION = FlowSpec.Builder.DEFAULT_VERSION;
  private final FlowCatalog flowCatalog;

  final String repositoryDir;
  final String configBaseFolderName;
  final PullFileLoader pullFileLoader;
  final Set<String> javaPropsExtensions;
  final Set<String> hoconFileExtensions;
  private final Config emptyConfig = ConfigFactory.empty();


  public GitConfigListener(FlowCatalog flowCatalog, String repositoryDir, String configBaseFolderName, String javaPropsExtentions, String hoconFileExtentions) {
    this.flowCatalog = flowCatalog;
    this.configBaseFolderName = configBaseFolderName;
    this.repositoryDir = repositoryDir;

    Path folderPath = new Path(repositoryDir, configBaseFolderName);
    this.javaPropsExtensions = Sets.newHashSet(javaPropsExtentions.split(","));
    this.hoconFileExtensions = Sets.newHashSet(hoconFileExtentions.split(","));
    try {
      this.pullFileLoader = new PullFileLoader(folderPath,
          FileSystem.get(URI.create(ConfigurationKeys.LOCAL_FS_URI), new Configuration()),
          this.javaPropsExtensions, this.hoconFileExtensions);
    } catch (IOException e) {
      throw new RuntimeException("Could not create pull file loader", e);
    }
  }
  /**
   * Add a {@link FlowSpec} for an added, updated, or modified flow config
   * @param change
   */
  @Override
  public void addChange(DiffEntry change) {
    if (checkConfigFilePath(change.getNewPath())) {
      Path configFilePath = new Path(this.repositoryDir, change.getNewPath());

      try {
        Config flowConfig = loadConfigFileWithFlowNameOverrides(configFilePath);

        this.flowCatalog.put(FlowSpec.builder()
            .withConfig(flowConfig)
            .withVersion(SPEC_VERSION)
            .withDescription(SPEC_DESCRIPTION)
            .build());
      } catch (Throwable e) {
        log.warn("Could not load config file: " + configFilePath);
      }
    }
  }

  /**
   * remove a {@link FlowSpec} for a deleted or renamed flow config
   * @param change
   */
  @Override
  public void removeChange(DiffEntry change) {
    if (checkConfigFilePath(change.getOldPath())) {
      Path configFilePath = new Path(this.repositoryDir, change.getOldPath());
      String flowName = FSSpecStore.getSpecName(configFilePath);
      String flowGroup = FSSpecStore.getSpecGroup(configFilePath);

      // build a dummy config to get the proper URI for delete
      Config dummyConfig = ConfigBuilder.create()
          .addPrimitive(ConfigurationKeys.FLOW_GROUP_KEY, flowGroup)
          .addPrimitive(ConfigurationKeys.FLOW_NAME_KEY, flowName)
          .build();

      FlowSpec spec = FlowSpec.builder()
          .withConfig(dummyConfig)
          .withVersion(SPEC_VERSION)
          .withDescription(SPEC_DESCRIPTION)
          .build();

      this.flowCatalog.remove(spec.getUri());
    }
  }


  /**
   * check whether the file has the proper naming and hierarchy
   * @param configFilePath the relative path from the repo root
   * @return false if the file does not conform
   */
  private boolean checkConfigFilePath(String configFilePath) {
    // The config needs to stored at configDir/flowGroup/flowName.(pull|job|json|conf)
    Path configFile = new Path(configFilePath);
    String fileExtension = Files.getFileExtension(configFile.getName());

    if (configFile.depth() != CONFIG_FILE_DEPTH
        || !configFile.getParent().getParent().getName().equals(configBaseFolderName)
        || !(PullFileLoader.DEFAULT_JAVA_PROPS_PULL_FILE_EXTENSIONS.contains(fileExtension)
        || PullFileLoader.DEFAULT_JAVA_PROPS_PULL_FILE_EXTENSIONS.contains(fileExtension))) {
      log.warn("Changed file does not conform to directory structure and file name format, skipping: "
          + configFilePath);

      return false;
    }

    return true;
  }

  /**
   * Load the config file and override the flow name and flow path properties with the names from the file path
   * @param configFilePath path of the config file relative to the repository root
   * @return the configuration object
   * @throws IOException
   */
  private Config loadConfigFileWithFlowNameOverrides(Path configFilePath) throws IOException {
    Config flowConfig = this.pullFileLoader.loadPullFile(configFilePath, emptyConfig, false);
    String flowName = FSSpecStore.getSpecName(configFilePath);
    String flowGroup = FSSpecStore.getSpecGroup(configFilePath);

    return flowConfig.withValue(ConfigurationKeys.FLOW_NAME_KEY, ConfigValueFactory.fromAnyRef(flowName))
        .withValue(ConfigurationKeys.FLOW_GROUP_KEY, ConfigValueFactory.fromAnyRef(flowGroup));
  }
}
