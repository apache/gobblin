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

package org.apache.gobblin.service;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ServiceConfigKeys {

  public static final String GOBBLIN_SERVICE_PREFIX = "gobblin.service.";
  public static final String GOBBLIN_ORCHESTRATOR_LISTENER_CLASS = "org.apache.gobblin.service.modules.orchestration.Orchestrator";

  // Gobblin Service Manager Keys
  public static final String GOBBLIN_SERVICE_FLOW_CATALOG_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "flowCatalog.enabled";
  public static final String GOBBLIN_SERVICE_SCHEDULER_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "scheduler.enabled";
  public static final String GOBBLIN_SERVICE_INSTANCE_NAME = GOBBLIN_SERVICE_PREFIX + "instance.name";

  public static final String GOBBLIN_SERVICE_RESTLI_SERVER_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "restliServer.enabled";
  public static final String GOBBLIN_SERVICE_TOPOLOGY_SPEC_FACTORY_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "topologySpecFactory.enabled";
  public static final String GOBBLIN_SERVICE_GIT_CONFIG_MONITOR_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "gitConfigMonitor.enabled";
  public static final String GOBBLIN_SERVICE_JOB_STATUS_MONITOR_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "jobStatusMonitor.enabled";
  public static final String GOBBLIN_SERVICE_FLOWSPEC = GOBBLIN_SERVICE_PREFIX + "flowSpec";
  public static final String GOBBLIN_SERVICE_FLOWGRAPH_CLASS_KEY = GOBBLIN_SERVICE_PREFIX + "flowGraph.class";
  public static final String GOBBLIN_SERVICE_FLOWGRAPH_HELPER_KEY = GOBBLIN_SERVICE_PREFIX + "flowGraphHelper.class";

  // Flow Compiler Keys
  public static final String GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY = GOBBLIN_SERVICE_PREFIX + "flowCompiler.class";
  public static final String COMPILATION_SUCCESSFUL = "compilation.successful";
  public static final String COMPILATION_RESPONSE = "compilation.response";

  // Job Level Keys
  public static final String WORK_UNIT_SIZE = GOBBLIN_SERVICE_PREFIX + "work.unit.size";
  public static final String TOTAL_WORK_UNIT_SIZE = GOBBLIN_SERVICE_PREFIX + "total.work.unit.size";
  /**
   * Directly use canonical class name here to avoid introducing additional dependency here.
   */
  public static final String DEFAULT_GOBBLIN_SERVICE_FLOWCOMPILER_CLASS =
      "org.apache.gobblin.service.modules.flow.IdentityFlowToJobSpecCompiler";

  // Flow specific Keys
  public static final String FLOW_SOURCE_IDENTIFIER_KEY = "gobblin.flow.sourceIdentifier";
  public static final String FLOW_DESTINATION_IDENTIFIER_KEY = "gobblin.flow.destinationIdentifier";

  // Topology Factory Keys (for overall factory)
  public static final String TOPOLOGY_FACTORY_PREFIX = "topologySpecFactory.";
  public static final String DEFAULT_TOPOLOGY_SPEC_FACTORY =
      "org.apache.gobblin.service.modules.topology.ConfigBasedTopologySpecFactory";
  public static final String TOPOLOGYSPEC_FACTORY_KEY = TOPOLOGY_FACTORY_PREFIX + "class";
  public static final String TOPOLOGY_FACTORY_TOPOLOGY_NAMES_KEY = TOPOLOGY_FACTORY_PREFIX + "topologyNames";

  // Topology Factory Keys (for individual topologies)
  public static final String TOPOLOGYSPEC_DESCRIPTION_KEY = "description";
  public static final String TOPOLOGYSPEC_VERSION_KEY = "version";
  public static final String TOPOLOGYSPEC_URI_KEY = "uri";

  public static final String DEFAULT_SPEC_EXECUTOR =
      "org.apache.gobblin.runtime.spec_executorInstance.InMemorySpecExecutor";
  public static final String SPEC_EXECUTOR_KEY = "specExecutorInstance.class";

  public static final String DATA_MOVEMENT_AUTHORIZER_CLASS = "dataMovementAuthorizer.class";

  // Template Catalog Keys
  public static final String TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY = GOBBLIN_SERVICE_PREFIX + "templateCatalogs.fullyQualifiedPath";
  public static final String TEMPLATE_CATALOGS_CLASS_KEY = GOBBLIN_SERVICE_PREFIX + "templateCatalogs.class";

  public static final String ATTRS_PATH_IN_CONFIG = "executorAttrs";

  // Gobblin Service Graph Representation Topology related Keys
  public static final String NODE_SECURITY_KEY = "node.secured";
  // True means node is by default secure.
  public static final String DEFAULT_NODE_SECURITY = "true";

  public static final String SERVICE_PORT = GOBBLIN_SERVICE_PREFIX + "port";

  // Prefix for config to ServiceBasedAppLauncher that will only be used by GaaS and not orchestrated jobs
  public static final String GOBBLIN_SERVICE_APP_LAUNCHER_PREFIX = "gobblinServiceAppLauncher";

  //Flow concurrency config key to control default service behavior.
  public static final String FLOW_CONCURRENCY_ALLOWED = GOBBLIN_SERVICE_PREFIX + "flowConcurrencyAllowed";
  public static final Boolean DEFAULT_FLOW_CONCURRENCY_ALLOWED = true;

  public static final String LEADER_URL = "leaderUrl";

  public static final String QUOTA_MANAGER_CLASS = GOBBLIN_SERVICE_PREFIX + "quotaManager.class";
  public static final String DEFAULT_QUOTA_MANAGER = "org.apache.gobblin.service.modules.orchestration.InMemoryUserQuotaManager";

  public static final String QUOTA_STORE_DB_TABLE_KEY = "quota.store.db.table";
  public static final String DEFAULT_QUOTA_STORE_DB_TABLE = "quota_table";

  public static final String RUNNING_DAG_IDS_DB_TABLE_KEY = "running.dag.ids.store.db.table";
  public static final String DEFAULT_RUNNING_DAG_IDS_DB_TABLE = "running_dag_ids";


  // Group Membership authentication service
  public static final String GROUP_OWNERSHIP_SERVICE_CLASS = GOBBLIN_SERVICE_PREFIX + "groupOwnershipService.class";
  public static final String DEFAULT_GROUP_OWNERSHIP_SERVICE = "org.apache.gobblin.service.NoopGroupOwnershipService";

  public static final int MAX_FLOW_NAME_LENGTH = 128; // defined in FlowId.pdl
  public static final int MAX_FLOW_GROUP_LENGTH = 128; // defined in FlowId.pdl
  public static final int MAX_FLOW_EXECUTION_ID_LENGTH = 13; // length of flowExecutionId which is epoch timestamp
  public static final int MAX_JOB_NAME_LENGTH = 374;
  public static final int MAX_JOB_GROUP_LENGTH = 374;
  public static final int MAX_DAG_NODE_ID_LENGTH = MAX_FLOW_NAME_LENGTH + MAX_FLOW_GROUP_LENGTH + MAX_FLOW_EXECUTION_ID_LENGTH +
      MAX_JOB_NAME_LENGTH + MAX_JOB_GROUP_LENGTH + 4; // 4 to account for delimiters' length
  public static final int MAX_DAG_ID_LENGTH = MAX_FLOW_NAME_LENGTH + MAX_FLOW_GROUP_LENGTH + MAX_FLOW_EXECUTION_ID_LENGTH
      + 2; // 2 to account for delimiters' length
  public static final String STATE_STORE_TABLE_SUFFIX = "gst";
  public static final String STATE_STORE_KEY_SEPARATION_CHARACTER = ".";
  public static final String DAG_STORE_KEY_SEPARATION_CHARACTER = "_";

  // Service database connection
  public static final String SERVICE_DB_URL_KEY = GOBBLIN_SERVICE_PREFIX + "db.url";
  public static final String SERVICE_DB_USERNAME = GOBBLIN_SERVICE_PREFIX + "db.username";
  public static final String SERVICE_DB_PASSWORD = GOBBLIN_SERVICE_PREFIX + "db.password";
  public static final String SERVICE_DB_MAX_CONNECTIONS = GOBBLIN_SERVICE_PREFIX + "db.maxConnections";
  public static final String SERVICE_DB_MAX_CONNECTION_LIFETIME = GOBBLIN_SERVICE_PREFIX + "db.maxConnectionLifetime";

  // Mysql-based issues repository
  public static final String MYSQL_ISSUE_REPO_PREFIX = GOBBLIN_SERVICE_PREFIX + "issueRepo.mysql.";

  public static final String MYSQL_ISSUE_REPO_CLEANUP_INTERVAL = MYSQL_ISSUE_REPO_PREFIX + "cleanupInterval";
  public static final Duration DEFAULT_MYSQL_ISSUE_REPO_CLEANUP_INTERVAL = Duration.ofHours(1);

  public static final String MYSQL_ISSUE_REPO_MAX_ISSUES_TO_KEEP = MYSQL_ISSUE_REPO_PREFIX + "maxIssuesToKeep";
  public static final long DEFAULT_MYSQL_ISSUE_REPO_MAX_ISSUES_TO_KEEP = 10 * 1000 * 1000;

  public static final String MYSQL_ISSUE_REPO_DELETE_ISSUES_OLDER_THAN =
      MYSQL_ISSUE_REPO_PREFIX + "deleteIssuesOlderThan";
  public static final Duration DEFAULT_MYSQL_ISSUE_REPO_DELETE_ISSUES_OLDER_THAN = Duration.ofDays(30);

  // In-memory issue repository
  public static final String MEMORY_ISSUE_REPO_PREFIX = GOBBLIN_SERVICE_PREFIX + "issueRepo.memory.";

  public static final String MEMORY_ISSUE_REPO_MAX_CONTEXT_COUNT = MEMORY_ISSUE_REPO_PREFIX + "maxContextCount";
  public static final int DEFAULT_MEMORY_ISSUE_REPO_MAX_CONTEXT_COUNT = 100;

  public static final String MEMORY_ISSUE_REPO_MAX_ISSUE_PER_CONTEXT = MEMORY_ISSUE_REPO_PREFIX + "maxIssuesPerContext";
  public static final int DEFAULT_MEMORY_ISSUE_REPO_MAX_ISSUE_PER_CONTEXT= 20;

  public static final String ISSUE_REPO_CLASS = GOBBLIN_SERVICE_PREFIX + "issueRepo.class";
  public static final String QUOTA_MANAGER_PREFIX = "UserQuotaManagerPrefix.";

  public static final String GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_PREFIX = ServiceConfigKeys.GOBBLIN_SERVICE_PREFIX + "dagProcessingEngine.";
  public static final String NUM_DAG_PROC_THREADS_KEY = GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_PREFIX + "numThreads";
  public static final String DAG_PROC_ENGINE_NON_RETRYABLE_EXCEPTIONS_KEY = GOBBLIN_SERVICE_DAG_PROCESSING_ENGINE_PREFIX + "nonRetryableExceptions";
  public static final Integer DEFAULT_NUM_DAG_PROC_THREADS = 3;
  public static final long DEFAULT_FLOW_FINISH_DEADLINE_MILLIS = TimeUnit.HOURS.toMillis(24);
}
