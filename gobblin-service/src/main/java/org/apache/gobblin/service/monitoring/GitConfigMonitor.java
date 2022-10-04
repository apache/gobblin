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

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;

/**
 * Service that monitors for jobs from a git repository.
 * The git repository must have an initial commit that has no config files since that is used as a base for getting
 * the change list.
 * The config needs to be organized with the following structure:
 * <root_config_dir>/<flowGroup>/<flowName>.(pull|job|json|conf)
 * The <flowGroup> and <flowName> is used to generate the URI used to store the config in the {@link FlowCatalog}
 */
@Slf4j
@Singleton
public class GitConfigMonitor extends GitMonitoringService {
  public static final String GIT_CONFIG_MONITOR_PREFIX = "gobblin.service.gitConfigMonitor";

  private static final String PROPERTIES_EXTENSIONS = "pull,job";
  private static final String CONF_EXTENSIONS = "json,conf";
  private static final String DEFAULT_GIT_CONFIG_MONITOR_REPO_DIR = "git-flow-config";
  private static final String DEFAULT_GIT_CONFIG_MONITOR_CONFIG_DIR = "gobblin-config";
  private static final String DEFAULT_GIT_CONFIG_MONITOR_BRANCH_NAME = "master";

  private static final int DEFAULT_GIT_CONFIG_MONITOR_POLLING_INTERVAL = 60;

  private static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(ConfigurationKeys.GIT_MONITOR_REPO_DIR, DEFAULT_GIT_CONFIG_MONITOR_REPO_DIR)
          .put(ConfigurationKeys.GIT_MONITOR_CONFIG_BASE_DIR, DEFAULT_GIT_CONFIG_MONITOR_CONFIG_DIR)
          .put(ConfigurationKeys.GIT_MONITOR_BRANCH_NAME, DEFAULT_GIT_CONFIG_MONITOR_BRANCH_NAME)
          .put(ConfigurationKeys.GIT_MONITOR_POLLING_INTERVAL, DEFAULT_GIT_CONFIG_MONITOR_POLLING_INTERVAL)
          .put(ConfigurationKeys.FLOWGRAPH_JAVA_PROPS_EXTENSIONS, PROPERTIES_EXTENSIONS)
          .put(ConfigurationKeys.FLOWGRAPH_HOCON_FILE_EXTENSIONS, CONF_EXTENSIONS)
          .build());

  private final FlowCatalog flowCatalog;

  @Inject
  GitConfigMonitor(Config config, FlowCatalog flowCatalog) {
    super(config.getConfig(GIT_CONFIG_MONITOR_PREFIX).withFallback(DEFAULT_FALLBACK));
    this.flowCatalog = flowCatalog;
    Config configWithFallbacks = config.getConfig(GIT_CONFIG_MONITOR_PREFIX).withFallback(DEFAULT_FALLBACK);
    this.listeners.add(new GitConfigListener(flowCatalog, configWithFallbacks.getString(ConfigurationKeys.GIT_MONITOR_REPO_DIR),
        configWithFallbacks.getString(ConfigurationKeys.GIT_MONITOR_CONFIG_BASE_DIR), configWithFallbacks.getString(ConfigurationKeys.FLOWGRAPH_JAVA_PROPS_EXTENSIONS),
        configWithFallbacks.getString(ConfigurationKeys.FLOWGRAPH_HOCON_FILE_EXTENSIONS)));
  }

  @Override
  public boolean shouldPollGit() {
    // if not active or if the flow catalog is not up yet then can't process config changes
    if (!isActive || !this.flowCatalog.isRunning()) {
      log.warn("GitConfigMonitor: skip poll since the JobCatalog is not yet running. isActive = {}", this.isActive);
      return false;
    }
    return true;
  }

}
