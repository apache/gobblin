/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

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

  // Gobblin Yarn ApplicationMaster configuration properties.
  public static final String APP_MASTER_MEMORY_MBS_KEY = GOBBLIN_YARN_PREFIX + "app.master.memory.mbs";
  public static final String APP_MASTER_CORES_KEY = GOBBLIN_YARN_PREFIX + "app.master.cores";
  public static final String APP_MASTER_JARS_KEY = GOBBLIN_YARN_PREFIX + "app.master.jars";
  public static final String APP_MASTER_FILES_LOCAL_KEY = GOBBLIN_YARN_PREFIX + "app.master.files.local";
  public static final String APP_MASTER_FILES_REMOTE_KEY = GOBBLIN_YARN_PREFIX + "app.master.files.remote";
  public static final String APP_MASTER_WORK_DIR_NAME = "appmaster";
  public static final String APP_MASTER_JVM_ARGS_KEY = GOBBLIN_YARN_PREFIX + "app.master.jvm.args";

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

  public static final String LOGS_SINK_ROOT_DIR_KEY = GOBBLIN_YARN_PREFIX + "logs.sink.root.dir";
  public static final String LIB_JARS_DIR_NAME = "_libjars";
  public static final String APP_JARS_DIR_NAME = "_appjars";
  public static final String APP_FILES_DIR_NAME = "_appfiles";
  public static final String APP_LOGS_DIR_NAME = "_applogs";

  // Other misc configuration properties.
  public static final String LOG_COPIER_SCHEDULER = GOBBLIN_YARN_PREFIX + "log.copier.scheduler";
  public static final String LOG_COPIER_MAX_FILE_SIZE = GOBBLIN_YARN_PREFIX + "log.copier.max.file.size";
  public static final String GOBBLIN_YARN_LOG4J_CONFIGURATION_FILE = "log4j-yarn.properties";
}
