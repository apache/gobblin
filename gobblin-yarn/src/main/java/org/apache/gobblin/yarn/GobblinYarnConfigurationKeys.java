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

package org.apache.gobblin.yarn;

/**
 * A central place for configuration related constants of Gobblin on Yarn.
 *
 * @author Yinan Li
 */
public class GobblinYarnConfigurationKeys {

  public static final String GOBBLIN_YARN_PREFIX = "gobblin.yarn.";

  // General Gobblin Yarn application configuration properties.
  public static final String APPLICATION_NAME_KEY = GOBBLIN_YARN_PREFIX + "app.name";
  public static final String APP_QUEUE_KEY = GOBBLIN_YARN_PREFIX + "app.queue";
  public static final String APP_REPORT_INTERVAL_MINUTES_KEY = GOBBLIN_YARN_PREFIX + "app.report.interval.minutes";
  public static final String MAX_GET_APP_REPORT_FAILURES_KEY = GOBBLIN_YARN_PREFIX + "max.get.app.report.failures";
  public static final String EMAIL_NOTIFICATION_ON_SHUTDOWN_KEY =
      GOBBLIN_YARN_PREFIX + "email.notification.on.shutdown";
  public static final String RELEASED_CONTAINERS_CACHE_EXPIRY_SECS = GOBBLIN_YARN_PREFIX + "releasedContainersCacheExpirySecs";
  public static final int DEFAULT_RELEASED_CONTAINERS_CACHE_EXPIRY_SECS = 300;
  public static final String APP_VIEW_ACL = GOBBLIN_YARN_PREFIX + "appViewAcl";
  public static final String DEFAULT_APP_VIEW_ACL = "*";

  // Gobblin Yarn ApplicationMaster configuration properties.
  public static final String APP_MASTER_MEMORY_MBS_KEY = GOBBLIN_YARN_PREFIX + "app.master.memory.mbs";
  public static final String APP_MASTER_CORES_KEY = GOBBLIN_YARN_PREFIX + "app.master.cores";
  public static final String APP_MASTER_JARS_KEY = GOBBLIN_YARN_PREFIX + "app.master.jars";
  public static final String APP_MASTER_FILES_LOCAL_KEY = GOBBLIN_YARN_PREFIX + "app.master.files.local";
  public static final String APP_MASTER_FILES_REMOTE_KEY = GOBBLIN_YARN_PREFIX + "app.master.files.remote";
  public static final String APP_MASTER_WORK_DIR_NAME = "appmaster";
  public static final String APP_MASTER_JVM_ARGS_KEY = GOBBLIN_YARN_PREFIX + "app.master.jvm.args";
  public static final String APP_MASTER_SERVICE_CLASSES = GOBBLIN_YARN_PREFIX + "app.master.serviceClasses";
  // Amount of overhead to subtract when computing the Xmx value. This is to account for non-heap memory, like metaspace
  // and stack memory
  public static final String APP_MASTER_JVM_MEMORY_OVERHEAD_MBS_KEY = GOBBLIN_YARN_PREFIX + "app.master.jvmMemoryOverheadMbs";
  public static final int DEFAULT_APP_MASTER_JVM_MEMORY_OVERHEAD_MBS = 0;
  // The ratio of the amount of Xmx to carve out of the container memory before adjusting for jvm memory overhead
  public static final String APP_MASTER_JVM_MEMORY_XMX_RATIO_KEY = GOBBLIN_YARN_PREFIX + "app.master.jvmMemoryXmxRatio";
  public static final double DEFAULT_APP_MASTER_JVM_MEMORY_XMX_RATIO = 1.0;

  // Gobblin Yarn container configuration properties.
  public static final String INITIAL_CONTAINERS_KEY = GOBBLIN_YARN_PREFIX + "initial.containers";
  public static final String CONTAINER_MEMORY_MBS_KEY = GOBBLIN_YARN_PREFIX + "container.memory.mbs";
  public static final String CONTAINER_CORES_KEY = GOBBLIN_YARN_PREFIX + "container.cores";
  public static final String CONTAINER_JARS_KEY = GOBBLIN_YARN_PREFIX + "container.jars";
  public static final String CONTAINER_FILES_LOCAL_KEY = GOBBLIN_YARN_PREFIX + "container.files.local";
  public static final String CONTAINER_FILES_REMOTE_KEY = GOBBLIN_YARN_PREFIX + "container.files.remote";
  public static final String CONTAINER_WORK_DIR_NAME = "container";
  public static final String CONTAINER_JVM_ARGS_KEY = GOBBLIN_YARN_PREFIX + "container.jvm.args";
  public static final String CONTAINER_HOST_AFFINITY_ENABLED = GOBBLIN_YARN_PREFIX + "container.affinity.enabled";
  // Amount of overhead to subtract when computing the Xmx value. This is to account for non-heap memory, like metaspace
  // and stack memory
  public static final String CONTAINER_JVM_MEMORY_OVERHEAD_MBS_KEY = GOBBLIN_YARN_PREFIX + "container.jvmMemoryOverheadMbs";
  public static final int DEFAULT_CONTAINER_JVM_MEMORY_OVERHEAD_MBS = 0;
  // The ratio of the amount of Xmx to carve out of the container memory before adjusting for jvm memory overhead
  public static final String CONTAINER_JVM_MEMORY_XMX_RATIO_KEY = GOBBLIN_YARN_PREFIX + "container.jvmMemoryXmxRatio";
  public static final double DEFAULT_CONTAINER_JVM_MEMORY_XMX_RATIO = 1.0;

  // Helix configuration properties.
  public static final String HELIX_INSTANCE_MAX_RETRIES = GOBBLIN_YARN_PREFIX + "helix.instance.max.retries";

  // Security and authentication configuration properties.
  public static final String KEYTAB_FILE_PATH = GOBBLIN_YARN_PREFIX + "keytab.file.path";
  public static final String KEYTAB_PRINCIPAL_NAME = GOBBLIN_YARN_PREFIX + "keytab.principal.name";
  public static final String TOKEN_FILE_NAME = ".token";
  public static final String LOGIN_INTERVAL_IN_MINUTES = GOBBLIN_YARN_PREFIX + "login.interval.minutes";
  public static final String TOKEN_RENEW_INTERVAL_IN_MINUTES = GOBBLIN_YARN_PREFIX + "token.renew.interval.minutes";

  // Resource/dependencies configuration properties.
  public static final String LIB_JARS_DIR_KEY = GOBBLIN_YARN_PREFIX + "lib.jars.dir";

  public static final String LIB_JARS_DIR_NAME = "_libjars";
  public static final String APP_JARS_DIR_NAME = "_appjars";
  public static final String APP_FILES_DIR_NAME = "_appfiles";
  public static final String APP_LOGS_DIR_NAME = "_applogs";

  //Container Log location properties
  public static final String GOBBLIN_YARN_CONTAINER_LOG_DIR_NAME = GobblinYarnConfigurationKeys.GOBBLIN_YARN_PREFIX + "app.container.log.dir";
  public static final String GOBBLIN_YARN_CONTAINER_LOG_FILE_NAME = GobblinYarnConfigurationKeys.GOBBLIN_YARN_PREFIX + "app.container.log.file";

  // Other misc configuration properties.
  public static final String LOGS_SINK_ROOT_DIR_KEY = GOBBLIN_YARN_PREFIX + "logs.sink.root.dir";
  public static final String LOG_FILE_EXTENSIONS = GOBBLIN_YARN_PREFIX + "log.file.extensions" ;
  public static final String LOG_COPIER_DISABLE_DRIVER_COPY = GOBBLIN_YARN_PREFIX + "log.copier.disable.driver.copy";
  public static final String GOBBLIN_YARN_CONTAINER_TIMEZONE = GOBBLIN_YARN_PREFIX + "container.timezone" ;
  public static final String DEFAULT_GOBBLIN_YARN_CONTAINER_TIMEZONE = "America/Los_Angeles" ;

  //Constant definitions
  public static final String GOBBLIN_YARN_LOG4J_CONFIGURATION_FILE = "log4j-yarn.properties";
  public static final String JVM_USER_TIMEZONE_CONFIG = "user.timezone";

  //Configuration properties relating to container mode of execution e.g. Gobblin cluster runs on Yarn
  public static final String CONTAINER_NUM_KEY = "container.num";
}
