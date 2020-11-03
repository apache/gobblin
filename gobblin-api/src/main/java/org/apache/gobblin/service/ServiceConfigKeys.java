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

import org.apache.gobblin.annotation.Alpha;

@Alpha
public class ServiceConfigKeys {

  public static final String GOBBLIN_SERVICE_PREFIX = "gobblin.service.";
  public static final String GOBBLIN_SERVICE_JOB_SCHEDULER_LISTENER_CLASS = "org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler";

  // Gobblin Service Manager Keys
  public static final String GOBBLIN_SERVICE_TOPOLOGY_CATALOG_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "topologyCatalog.enabled";
  public static final String GOBBLIN_SERVICE_FLOW_CATALOG_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "flowCatalog.enabled";
  public static final String GOBBLIN_SERVICE_SCHEDULER_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "scheduler.enabled";
  public static final String GOBBLIN_SERVICE_ORCHESTRATOR_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "orchestrator.enabled";
  public static final String GOBBLIN_SERVICE_RESTLI_SERVER_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "restliServer.enabled";
  public static final String GOBBLIN_SERVICE_TOPOLOGY_SPEC_FACTORY_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "topologySpecFactory.enabled";
  public static final String GOBBLIN_SERVICE_GIT_CONFIG_MONITOR_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "gitConfigMonitor.enabled";
  public static final String GOBBLIN_SERVICE_DAG_MANAGER_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "dagManager.enabled";
  public static final String GOBBLIN_SERVICE_JOB_STATUS_MONITOR_ENABLED_KEY = GOBBLIN_SERVICE_PREFIX + "jobStatusMonitor.enabled";

  // Helix / ServiceScheduler Keys
  public static final String HELIX_CLUSTER_NAME_KEY = GOBBLIN_SERVICE_PREFIX + "helix.cluster.name";
  public static final String ZK_CONNECTION_STRING_KEY = GOBBLIN_SERVICE_PREFIX + "zk.connection.string";
  public static final String HELIX_INSTANCE_NAME_OPTION_NAME = "helix_instance_name";
  public static final String HELIX_INSTANCE_NAME_KEY = GOBBLIN_SERVICE_PREFIX + "helixInstanceName";
  public static final String GOBBLIN_SERVICE_FLOWSPEC = GOBBLIN_SERVICE_PREFIX + "flowSpec";

  // Helix message sub types for FlowSpec
  public static final String HELIX_FLOWSPEC_ADD = "FLOWSPEC_ADD";
  public static final String HELIX_FLOWSPEC_REMOVE = "FLOWSPEC_REMOVE";
  public static final String HELIX_FLOWSPEC_UPDATE = "FLOWSPEC_UPDATE";

  // Flow Compiler Keys
  public static final String GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY = GOBBLIN_SERVICE_PREFIX + "flowCompiler.class";
  public static final String COMPILATION_SUCCESSFUL = "compilation.successful";

  // Flow Catalog Keys
  public static final String GOBBLIN_SERVICE_FLOW_CATALOG_LOCAL_COMMIT = GOBBLIN_SERVICE_PREFIX + "flowCatalog.localCommit";
  public static final boolean DEFAULT_GOBBLIN_SERVICE_FLOW_CATALOG_LOCAL_COMMIT = true;

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
  public static final String EDGE_SECURITY_KEY = "edge.secured";

  public static final String DATA_MOVEMENT_AUTHORIZER_CLASS = "dataMovementAuthorizer.class";

  // Template Catalog Keys
  public static final String TEMPLATE_CATALOGS_FULLY_QUALIFIED_PATH_KEY = GOBBLIN_SERVICE_PREFIX + "templateCatalogs.fullyQualifiedPath";

  // Keys related to user-specified policy on route selection.
  // Undesired connection to form an executable JobSpec.
  // Formatted as a String list, each entry contains a string in the format of "Source1:Sink1:URI",
  // which indicates that data movement from source1 to sink1 with specific URI of specExecutor should be avoided.
  public static final String POLICY_BASED_BLOCKED_CONNECTION = GOBBLIN_SERVICE_PREFIX + "blockedConnections";

  // Comma separated list of nodes that is blacklisted. Names put here will become the nodeName which is the ID of a serviceNode.
  public static final String POLICY_BASED_BLOCKED_NODES = GOBBLIN_SERVICE_PREFIX + "blockedNodes";
  // Complete path of how the data movement is executed from source to sink.
  // Formatted as a String, each hop separated by comma, from source to sink in order.
  public static final String POLICY_BASED_DATA_MOVEMENT_PATH = GOBBLIN_SERVICE_PREFIX + "fullDataPath";

  public static final String ATTRS_PATH_IN_CONFIG = "executorAttrs";

  // Gobblin Service Graph Representation Topology related Keys
  public static final String NODE_SECURITY_KEY = "node.secured";
  // True means node is by default secure.
  public static final String DEFAULT_NODE_SECURITY = "true";


  // Policy related configuration Keys
  public static final String DEFAULT_SERVICE_POLICY = "static";
  public static final String SERVICE_POLICY_NAME = GOBBLIN_SERVICE_PREFIX + "servicePolicy";
  // Logging
  public static final String GOBBLIN_SERVICE_LOG4J_CONFIGURATION_FILE = "log4j-service.properties";
  // GAAS Listerning Port
  public static final String SERVICE_PORT = GOBBLIN_SERVICE_PREFIX + "port";
  public static final String SERVICE_NAME = GOBBLIN_SERVICE_PREFIX + "serviceName";
  public static final String SERVICE_URL_PREFIX = GOBBLIN_SERVICE_PREFIX + "serviceUrlPrefix";

  // Prefix for config to ServiceBasedAppLauncher that will only be used by GaaS and not orchestrated jobs
  public static final String GOBBLIN_SERVICE_APP_LAUNCHER_PREFIX = "gobblinServiceAppLauncher";

  //Flow concurrency config key to control default service behavior.
  public static final String FLOW_CONCURRENCY_ALLOWED = GOBBLIN_SERVICE_PREFIX + "flowConcurrencyAllowed";
  public static final Boolean DEFAULT_FLOW_CONCURRENCY_ALLOWED = true;

  public static final String LEADER_URL = "leaderUrl";

  public static final String FORCE_LEADER = GOBBLIN_SERVICE_PREFIX + "forceLeader";
  public static final boolean DEFAULT_FORCE_LEADER = false;
}
