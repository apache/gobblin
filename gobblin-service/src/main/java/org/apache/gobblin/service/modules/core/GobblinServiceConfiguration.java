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

package org.apache.gobblin.service.modules.core;

import java.util.Objects;

import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;

import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


@ToString
public class GobblinServiceConfiguration {

  @Getter
  private final String serviceName;

  @Getter
  private final String serviceId;

  @Getter
  private final boolean isWarmStandbyEnabled;

  @Getter
  private final boolean isTopologyCatalogEnabled;

  @Getter
  private final boolean isFlowCatalogEnabled;

  @Getter
  private final boolean isSchedulerEnabled;

  @Getter
  private final boolean isRestLIServerEnabled;

  @Getter
  private final boolean isTopologySpecFactoryEnabled;

  @Getter
  private final boolean isGitConfigMonitorEnabled;

  @Getter
  private final boolean isDagManagerEnabled;

  @Getter
  private final boolean isJobStatusMonitorEnabled;

  @Getter
  private final boolean isHelixManagerEnabled;

  @Getter
  private final boolean flowCatalogLocalCommit;

  @Getter
  private final boolean onlyAnnounceLeader;

  @Getter
  private final Config innerConfig;

  @Getter
  @Nullable
  private final Path serviceWorkDir;

  public GobblinServiceConfiguration(String serviceName, String serviceId, Config config,
      @Nullable Path serviceWorkDir) {
    this.serviceName = Objects.requireNonNull(serviceName,"Service name cannot be null");
    this.serviceId = Objects.requireNonNull(serviceId,"Service id cannot be null");
    this.innerConfig = Objects.requireNonNull(config, "Config cannot be null");
    this.serviceWorkDir = serviceWorkDir;

    isTopologyCatalogEnabled =
        ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_TOPOLOGY_CATALOG_ENABLED_KEY, true);
    isFlowCatalogEnabled =
        ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_FLOW_CATALOG_ENABLED_KEY, true);

    if (isFlowCatalogEnabled) {
      flowCatalogLocalCommit =
          ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_FLOW_CATALOG_LOCAL_COMMIT,
              ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_FLOW_CATALOG_LOCAL_COMMIT);
      isGitConfigMonitorEnabled =
          ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_GIT_CONFIG_MONITOR_ENABLED_KEY, false);
    } else {
      flowCatalogLocalCommit = false;
      isGitConfigMonitorEnabled = false;
    }

    this.isWarmStandbyEnabled = ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_WARM_STANDBY_ENABLED_KEY, false);

    this.isHelixManagerEnabled = config.hasPath(ServiceConfigKeys.ZK_CONNECTION_STRING_KEY);
    this.isDagManagerEnabled =
        ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_DAG_MANAGER_ENABLED_KEY, ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_DAG_MANAGER_ENABLED);
    this.isJobStatusMonitorEnabled =
        ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_JOB_STATUS_MONITOR_ENABLED_KEY, true);
    this.isSchedulerEnabled =
        ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_SCHEDULER_ENABLED_KEY, true);
    this.isRestLIServerEnabled =
        ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_RESTLI_SERVER_ENABLED_KEY, true);
    this.isTopologySpecFactoryEnabled =
        ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_TOPOLOGY_SPEC_FACTORY_ENABLED_KEY, true);
    this.onlyAnnounceLeader = ConfigUtils.getBoolean(config, ServiceConfigKeys.GOBBLIN_SERVICE_D2_ONLY_ANNOUNCE_LEADER, false);
  }
}
