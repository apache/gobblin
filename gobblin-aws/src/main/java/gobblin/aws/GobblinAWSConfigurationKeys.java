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

package gobblin.aws;

/**
 * A central place for configuration related constants of Gobblin on AWS.
 *
 * @author Abhishek Tiwari
 */
public class GobblinAWSConfigurationKeys {

  public static final String GOBBLIN_AWS_PREFIX = "gobblin.aws.";

  // General Gobblin AWS application configuration properties.
  public static final String CLUSTER_NAME_KEY = GOBBLIN_AWS_PREFIX + "cluster.name";
  public static final String EMAIL_NOTIFICATION_ON_SHUTDOWN_KEY =
      GOBBLIN_AWS_PREFIX + "email.notification.on.shutdown";

  // Gobblin AWS cluster configuration properties.
  public static final String AWS_REGION_KEY = GOBBLIN_AWS_PREFIX + "region";
  public static final String AWS_CONF_DIR = GOBBLIN_AWS_PREFIX + "conf.dir";

  // Gobblin AWS NFS configuration properties.
  public static final String NFS_PARENT_DIR_KEY = GOBBLIN_AWS_PREFIX + "nfs.parent.dir";

  // Gobblin AWS master configuration properties.
  public static final String MASTER_AMI_ID_KEY = GOBBLIN_AWS_PREFIX + "master.ami.id";
  public static final String MASTER_INSTANCE_TYPE_KEY = GOBBLIN_AWS_PREFIX + "master.instance.type";
  public static final String MASTER_JVM_MEMORY_KEY = GOBBLIN_AWS_PREFIX + "master.jvm.memory";
  public static final String MASTER_JVM_ARGS_KEY = GOBBLIN_AWS_PREFIX + "master.jvm.args";

  public static final String MASTER_JARS_KEY = GOBBLIN_AWS_PREFIX + "master.jars";
  public static final String MASTER_CONF_LOCAL_KEY = GOBBLIN_AWS_PREFIX + "master.conf.local";
  public static final String MASTER_CONF_S3_KEY = GOBBLIN_AWS_PREFIX + "master.conf.s3.uri";
  public static final String MASTER_JARS_S3_KEY = GOBBLIN_AWS_PREFIX + "master.jars.s3.uri";

  // Gobblin AWS worker configuration properties.
  public static final String WORKER_AMI_ID_KEY = GOBBLIN_AWS_PREFIX + "worker.ami.id";
  public static final String WORKER_INSTANCE_TYPE_KEY = GOBBLIN_AWS_PREFIX + "worker.instance.type";
  public static final String WORKER_JVM_MEMORY_KEY = GOBBLIN_AWS_PREFIX + "worker.jvm.memory";
  public static final String WORKER_JVM_ARGS_KEY = GOBBLIN_AWS_PREFIX + "worker.jvm.args";
  public static final String MIN_WORKERS_KEY = GOBBLIN_AWS_PREFIX + "min.workers";
  public static final String MAX_WORKERS_KEY = GOBBLIN_AWS_PREFIX + "max.workers";
  public static final String DESIRED_WORKERS_KEY = GOBBLIN_AWS_PREFIX + "desired.workers";

  public static final String WORKER_JARS_KEY = GOBBLIN_AWS_PREFIX + "worker.jars";
  public static final String WORKER_CONF_LOCAL_KEY = GOBBLIN_AWS_PREFIX + "worker.conf.local";
  public static final String WORKER_CONF_S3_KEY = GOBBLIN_AWS_PREFIX + "worker.conf.s3.uri";
  public static final String WORKER_JARS_S3_KEY = GOBBLIN_AWS_PREFIX + "worker.jars.s3.uri";

  // Security and authentication configuration properties.
  public static final String CREDENTIALS_REFRESH_INTERVAL_IN_MINUTES =
      GOBBLIN_AWS_PREFIX + "credentials.refresh.interval.minutes";
  public static final String SERVICE_ACCESS_KEY = GOBBLIN_AWS_PREFIX + "service.access";
  public static final String SERVICE_SECRET_KEY = GOBBLIN_AWS_PREFIX + "service.secret";
  public static final String CLIENT_ASSUME_ROLE_KEY = GOBBLIN_AWS_PREFIX + "client.assume.role";
  public static final String CLIENT_ROLE_ARN_KEY = GOBBLIN_AWS_PREFIX + "client.role.arn";
  public static final String CLIENT_EXTERNAL_ID_KEY = GOBBLIN_AWS_PREFIX + "client.external.id";
  public static final String CLIENT_SESSION_ID_KEY = GOBBLIN_AWS_PREFIX + "client.session.id";

  // Resource/dependencies configuration properties.
  public static final String LIB_JARS_DIR_KEY = GOBBLIN_AWS_PREFIX + "lib.jars.dir";
  public static final String LOGS_SINK_ROOT_DIR_KEY = GOBBLIN_AWS_PREFIX + "logs.sink.root.dir";

  // Other misc configuration properties.
  public static final String GOBBLIN_AWS_LOG4J_CONFIGURATION_FILE = "log4j-aws.properties";
}
