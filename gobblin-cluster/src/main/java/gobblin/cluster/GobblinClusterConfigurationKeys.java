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

package gobblin.cluster;

import gobblin.annotation.Alpha;


/**
 * A central place for configuration related constants of a Gobblin Cluster.
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinClusterConfigurationKeys {

  public static final String GOBBLIN_CLUSTER_PREFIX = "gobblin.cluster.";

  // General Gobblin Cluster application configuration properties.
  public static final String APPLICATION_NAME_OPTION_NAME = "app_name";
  public static final String STANDALONE_CLUSTER_MODE = "standalone_cluster";

  // Helix configuration properties.
  public static final String HELIX_CLUSTER_NAME_KEY = GOBBLIN_CLUSTER_PREFIX + "helix.cluster.name";
  public static final String ZK_CONNECTION_STRING_KEY = GOBBLIN_CLUSTER_PREFIX + "zk.connection.string";
  public static final String WORK_UNIT_FILE_PATH = GOBBLIN_CLUSTER_PREFIX + "work.unit.file.path";
  public static final String HELIX_INSTANCE_NAME_OPTION_NAME = "helix_instance_name";

  /**
   * A path pointing to a directory that contains job execution files to be executed by Gobblin. This directory can
   * have a nested structure.
   *
   * @see <a href="https://github.com/linkedin/gobblin/wiki/Working-with-Job-Configuration-Files">Job Config Files</a>
   */
  public static final String JOB_CONF_PATH_KEY = GOBBLIN_CLUSTER_PREFIX + "job.conf.path";
  public static final String INPUT_WORK_UNIT_DIR_NAME = "_workunits";
  public static final String OUTPUT_TASK_STATE_DIR_NAME = "_taskstates";
  public static final String TAR_GZ_FILE_SUFFIX = ".tar.gz";

  // Other misc configuration properties.
  public static final String TASK_SUCCESS_OPTIONAL_KEY = "TASK_SUCCESS_OPTIONAL";
  public static final String GOBBLIN_CLUSTER_LOG4J_CONFIGURATION_FILE = "log4j-cluster.properties";
  public static final String JOB_CONFIGURATION_MANAGER_KEY = GOBBLIN_CLUSTER_PREFIX + "job.configuration.manager";

  public static final String JOB_SPEC_REFRESH_INTERVAL = GOBBLIN_CLUSTER_PREFIX + "job.spec.refresh.interval";
  public static final String SPEC_EXECUTOR_INSTANCE_CONSUMER_CLASS_KEY = GOBBLIN_CLUSTER_PREFIX + "specConsumer.class";
  public static final String DEFAULT_SPEC_EXECUTOR_INSTANCE_CONSUMER_CLASS = "gobblin.service.SimpleKafkaSpecExecutorInstance";
}
