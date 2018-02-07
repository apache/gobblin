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
package org.apache.gobblin.aws;

import org.apache.gobblin.annotation.Alpha;


/**
 * A central place for configuration related constants of Gobblin on AWS.
 *
 * @author Abhishek Tiwari
 */
@Alpha
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

  public static final String MASTER_JARS_KEY = GOBBLIN_AWS_PREFIX + "master.jars.dir";
  public static final String MASTER_S3_CONF_URI_KEY = GOBBLIN_AWS_PREFIX + "master.s3.conf.uri";
  public static final String MASTER_S3_CONF_FILES_KEY = GOBBLIN_AWS_PREFIX + "master.s3.conf.files";
  public static final String MASTER_S3_JARS_URI_KEY = GOBBLIN_AWS_PREFIX + "master.s3.jars.uri";
  public static final String MASTER_S3_JARS_FILES_KEY = GOBBLIN_AWS_PREFIX + "master.s3.jars.files";

  // Gobblin AWS worker configuration properties.
  public static final String WORKER_AMI_ID_KEY = GOBBLIN_AWS_PREFIX + "worker.ami.id";
  public static final String WORKER_INSTANCE_TYPE_KEY = GOBBLIN_AWS_PREFIX + "worker.instance.type";
  public static final String WORKER_JVM_MEMORY_KEY = GOBBLIN_AWS_PREFIX + "worker.jvm.memory";
  public static final String WORKER_JVM_ARGS_KEY = GOBBLIN_AWS_PREFIX + "worker.jvm.args";
  public static final String MIN_WORKERS_KEY = GOBBLIN_AWS_PREFIX + "min.workers";
  public static final String MAX_WORKERS_KEY = GOBBLIN_AWS_PREFIX + "max.workers";
  public static final String DESIRED_WORKERS_KEY = GOBBLIN_AWS_PREFIX + "desired.workers";

  public static final String WORKER_JARS_KEY = GOBBLIN_AWS_PREFIX + "worker.jars.dir";
  public static final String WORKER_S3_CONF_URI_KEY = GOBBLIN_AWS_PREFIX + "worker.s3.conf.uri";
  public static final String WORKER_S3_CONF_FILES_KEY = GOBBLIN_AWS_PREFIX + "worker.s3.conf.files";
  public static final String WORKER_S3_JARS_URI_KEY = GOBBLIN_AWS_PREFIX + "worker.s3.jars.uri";
  public static final String WORKER_S3_JARS_FILES_KEY = GOBBLIN_AWS_PREFIX + "worker.s3.jars.files";

  // Security and authentication configuration properties.
  public static final String CREDENTIALS_REFRESH_INTERVAL = GOBBLIN_AWS_PREFIX + "credentials.refresh.interval";
  public static final String SERVICE_ACCESS_KEY = GOBBLIN_AWS_PREFIX + "service.access";
  public static final String SERVICE_SECRET_KEY = GOBBLIN_AWS_PREFIX + "service.secret";
  public static final String CLIENT_ASSUME_ROLE_KEY = GOBBLIN_AWS_PREFIX + "client.assume.role";
  public static final String CLIENT_ROLE_ARN_KEY = GOBBLIN_AWS_PREFIX + "client.role.arn";
  public static final String CLIENT_EXTERNAL_ID_KEY = GOBBLIN_AWS_PREFIX + "client.external.id";
  public static final String CLIENT_SESSION_ID_KEY = GOBBLIN_AWS_PREFIX + "client.session.id";

  // Resource/dependencies configuration properties.
  public static final String LOGS_SINK_ROOT_DIR_KEY = GOBBLIN_AWS_PREFIX + "logs.sink.root.dir";

  // Log4j properties.
  public static final String GOBBLIN_AWS_LOG4J_CONFIGURATION_FILE = "log4j-aws.properties";

  // Job conf properties.
  public static final String JOB_CONF_S3_URI_KEY = GOBBLIN_AWS_PREFIX + "job.conf.s3.uri";
  public static final String JOB_CONF_SOURCE_FILE_FS_URI_KEY = GOBBLIN_AWS_PREFIX + "job.conf.source.file.fs.uri";
  public static final String JOB_CONF_SOURCE_FILE_PATH_KEY = GOBBLIN_AWS_PREFIX + "job.conf.source.file.path";
  public static final String JOB_CONF_REFRESH_INTERVAL = GOBBLIN_AWS_PREFIX + "job.conf.refresh.interval";

  // Work environment properties.
  public static final String APP_WORK_DIR = GOBBLIN_AWS_PREFIX + "work.dir";
  public static final String GOBBLIN_VERSION = GOBBLIN_AWS_PREFIX + "version";

  // DEFAULT VALUES

  // General Gobblin AWS application configuration properties.
  public static final String DEFAULT_CLUSTER_NAME = "gobblinApplication";
  public static final String DEFAULT_GOBBLIN_VERSION = "0.6.2-701-g7c07fd5";
  public static final boolean DEFAULT_EMAIL_NOTIFICATION_ON_SHUTDOWN = false;

  // Gobblin AWS cluster configuration properties.
  public static final String DEFAULT_AWS_REGION = "us-west-2";
  public static final String DEFAULT_AWS_CONF_DIR_POSTFIX = "cluster-conf";

  // Gobblin AWS NFS configuration properties.
  public static final String DEFAULT_NFS_PARENT_DIR = "/home/ec2-user/gobblinApplication/";

  // Gobblin AWS master configuration properties.
  public static final String DEFAULT_MASTER_AMI_ID = "ami-f303fb93";
  public static final String DEFAULT_MASTER_INSTANCE_TYPE = "m3-medium";
  public static final String DEFAULT_MASTER_JVM_MEMORY = "3G";

  public static final String DEFAULT_MASTER_JARS_POSTFIX = "gobblin-lib";
  public static final String DEFAULT_MASTER_S3_CONF_URI = "https://s3-region.amazonaws.com/s3bucket/gobblin-confs/cluster-conf/";
  public static final String DEFAULT_MASTER_S3_CONF_FILES = "application.conf,log4j-aws.properties,quartz.properties";
  public static final String DEFAULT_MASTER_S3_JARS_URI = "https://s3-us-west-2.amazonaws.com/gobblin-libs/latest-jars/";

  // Do not use final on the public static strings, even though it is logical.
  // Refer: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6447475

  // Findbugs:
  // Huge string constants is duplicated across multiple class files
  // A large String constant is duplicated across multiple class files.
  // This is likely because a final field is initialized to a String constant,
  // and the Java language mandates that all references to a final field from other
  // classes be inlined into that classfile. See JDK bug 6447475 for a description
  // of an occurrence of this bug in the JDK and how resolving it reduced the size of the JDK by 1 megabyte.
  public static final String DEFAULT_MASTER_S3_JARS_FILES;
  static {
    DEFAULT_MASTER_S3_JARS_FILES = "ST4-4.0.4.jar,activation-1.1.1.jar,annotations-2.0.1.jar,ant-1.9.1.jar,ant-launcher-1.9.1.jar,antlr-runtime-3.5.2.jar,aopalliance-1.0.jar,apache-log4j-extras-1.2.17.jar,asm-3.1.jar,asm-commons-3.1.jar,asm-tree-3.1.jar,avro-1.7.7.jar,avro-ipc-1.7.7-tests.jar,avro-ipc-1.7.7.jar,avro-mapred-1.7.7-hadoop2.jar,aws-java-sdk-applicationautoscaling-1.11.8.jar,aws-java-sdk-autoscaling-1.11.8.jar,aws-java-sdk-core-1.11.8.jar,aws-java-sdk-ec2-1.11.8.jar,aws-java-sdk-iam-1.11.8.jar,aws-java-sdk-kms-1.11.8.jar,aws-java-sdk-s3-1.11.8.jar,aws-java-sdk-sts-1.11.8.jar,azkaban-2.5.0.jar,bcpg-jdk15on-1.52.jar,bcprov-jdk15on-1.52.jar,bonecp-0.8.0.RELEASE.jar,bsh-2.0b4.jar,c3p0-0.9.1.1.jar,calcite-avatica-1.2.0-incubating.jar,calcite-core-1.2.0-incubating.jar,calcite-linq4j-1.2.0-incubating.jar,cglib-nodep-2.2.jar,codemodel-2.2.jar,commons-cli-1.3.1.jar,commons-codec-1.10.jar,commons-collections-3.2.1.jar,commons-compiler-2.7.6.jar,commons-compress-1.10.jar,commons-configuration-1.10.jar,commons-daemon-1.0.13.jar,commons-dbcp-1.4.jar,commons-el-1.0.jar,commons-email-1.4.jar,commons-httpclient-3.1.jar,commons-io-2.5.jar,commons-lang-2.6.jar,commons-lang3-3.4.jar,commons-logging-1.2.jar,commons-math3-3.5.jar,commons-net-3.1.jar,commons-pool-1.5.4.jar,commons-pool2-2.4.2.jar,commons-vfs2-2.0.jar,config-1.2.1.jar,curator-client-2.10.0.jar,curator-framework-2.10.0.jar,curator-recipes-2.10.0.jar,d2-1.15.9.jar,data-1.15.9.jar,data-transform-1.15.9.jar,datanucleus-api-jdo-3.2.6.jar,datanucleus-core-3.2.10.jar,datanucleus-rdbms-3.2.9.jar,degrader-1.15.9.jar,derby-10.12.1.1.jar,eigenbase-properties-1.1.5.jar,flyway-core-3.2.1.jar,generator-1.15.9.jar,geronimo-annotation_1.0_spec-1.1.1.jar,geronimo-jaspic_1.0_spec-1.0.jar,geronimo-jpa_3.0_spec-1.0.jar,geronimo-jta_1.1_spec-1.1.1.jar,gobblin-admin-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-api-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-aws-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-azkaban-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-cluster-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-compaction-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-config-client-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-config-core-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-core-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-data-management-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-example-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-hive-registration-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-kafka-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-metastore-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-metrics-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-rest-api-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-rest-api-data-template-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-rest-api-rest-client-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-rest-client-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-rest-server-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-runtime-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-test-harness-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-tunnel-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-utility-" + DEFAULT_GOBBLIN_VERSION + ".jar,gobblin-yarn-" + DEFAULT_GOBBLIN_VERSION + ".jar,groovy-all-2.1.6.jar,gson-2.6.2.jar,guava-15.0.jar,guava-retrying-2.0.0.jar,guice-4.0.jar,guice-servlet-3.0.jar,hadoop-annotations-2.3.0.jar,hadoop-auth-2.3.0.jar,hadoop-common-2.3.0.jar,hadoop-hdfs-2.3.0.jar,hadoop-mapreduce-client-common-2.3.0.jar,hadoop-mapreduce-client-core-2.3.0.jar,hadoop-yarn-api-2.3.0.jar,hadoop-yarn-client-2.3.0.jar,hadoop-yarn-common-2.3.0.jar,hadoop-yarn-server-common-2.3.0.jar,hamcrest-core-1.1.jar,helix-core-0.6.6-SNAPSHOT.jar,hive-ant-1.0.1.jar,hive-common-1.0.1.jar,hive-exec-1.0.1-core.jar,hive-jdbc-1.0.1.jar,hive-metastore-1.0.1.jar,hive-serde-1.0.1.jar,hive-service-1.0.1.jar,hive-shims-0.20-1.0.1.jar,hive-shims-0.20S-1.0.1.jar,hive-shims-0.23-1.0.1.jar,hive-shims-1.0.1.jar,hive-shims-common-1.0.1.jar,hive-shims-common-secure-1.0.1.jar,httpclient-4.5.2.jar,httpcore-4.4.4.jar,influxdb-java-2.1.jar,jackson-annotations-2.6.0.jar,jackson-core-2.6.6.jar,jackson-core-asl-1.9.13.jar,jackson-databind-2.6.6.jar,jackson-dataformat-cbor-2.6.6.jar,jackson-jaxrs-1.8.3.jar,jackson-mapper-asl-1.9.13.jar,jackson-xc-1.8.3.jar,janino-2.7.6.jar,jansi-1.11.jar,jasper-compiler-5.5.23.jar,jasper-runtime-5.5.23.jar,jasypt-1.9.2.jar,java-xmlbuilder-0.4.jar,javassist-3.18.2-GA.jar,javax.inject-1.jar,javax.mail-1.5.2.jar,javax.servlet-api-3.1.0.jar,jaxb-api-2.2.2.jar,jaxb-impl-2.2.3-1.jar,jcommander-1.48.jar,jdo-api-3.0.1.jar,jdo2-api-2.1.jar,jersey-core-1.9.jar,jersey-guice-1.9.jar,jersey-json-1.9.jar,jersey-server-1.9.jar,jets3t-0.9.0.jar,jettison-1.1.jar,jetty-6.1.26.jar,jetty-all-7.6.0.v20120127.jar,jetty-http-9.2.14.v20151106.jar,jetty-io-9.2.14.v20151106.jar,jetty-security-9.2.14.v20151106.jar,jetty-server-9.2.14.v20151106.jar,jetty-servlet-9.2.14.v20151106.jar,jetty-util-6.1.26.jar,jetty-util-9.2.14.v20151106.jar,jline-0.9.94.jar,joda-time-2.9.3.jar,jopt-simple-3.2.jar,jpam-1.1.jar,jsch-0.1.53.jar,json-20070829.jar,jsp-api-2.1.jar,jsr305-3.0.0.jar,jta-1.1.jar,junit-3.8.1.jar,kafka-clients-0.8.2.2.jar,kafka_2.11-0.8.2.2.jar,li-jersey-uri-1.15.9.jar,libfb303-0.9.0.jar,libthrift-0.9.3.jar,log4j-1.2.17.jar,lombok-1.16.8.jar,lz4-1.2.0.jar,mail-1.4.1.jar,maven-scm-api-1.4.jar,maven-scm-provider-svn-commons-1.4.jar,maven-scm-provider-svnexe-1.4.jar,metrics-core-2.2.0.jar,metrics-core-3.1.0.jar,metrics-graphite-3.1.0.jar,metrics-jvm-3.1.0.jar,mina-core-1.1.7.jar,mockito-core-1.10.19.jar,mysql-connector-java-5.1.38.jar,netty-3.2.3.Final.jar,netty-3.7.0.Final.jar,objenesis-2.1.jar,okhttp-2.4.0.jar,okio-1.4.0.jar,opencsv-2.3.jar,paranamer-2.3.jar,parseq-1.3.6.jar,pegasus-common-1.15.9.jar,pentaho-aggdesigner-algorithm-5.1.5-jhyde.jar,plexus-utils-1.5.6.jar,protobuf-java-2.5.0.jar,quartz-2.2.3.jar,r2-1.15.9.jar,reflections-0.9.10.jar,regexp-1.3.jar,restli-client-1.15.9.jar,restli-common-1.15.9.jar,restli-docgen-1.15.9.jar,restli-netty-standalone-1.15.9.jar,restli-server-1.15.9.jar,restli-tools-1.15.9.jar,retrofit-1.9.0.jar,scala-library-2.11.8.jar,scala-parser-combinators_2.11-1.0.2.jar,scala-xml_2.11-1.0.2.jar,servlet-api-2.5-20081211.jar,servlet-api-2.5.jar,slf4j-api-1.7.21.jar,slf4j-log4j12-1.7.21.jar,snappy-0.3.jar,snappy-java-1.1.1.7.jar,stax-api-1.0-2.jar,stax-api-1.0.1.jar,testng-6.9.10.jar,transaction-api-1.1.jar,velocity-1.7.jar,xmlenc-0.52.jar,zkclient-0.5.jar,zookeeper-3.4.6.jar";
  }

  // Gobblin AWS worker configuration properties.
  public static final String DEFAULT_WORKER_AMI_ID = "ami-f303fb93";
  public static final String DEFAULT_WORKER_INSTANCE_TYPE = "m3-medium";
  public static final String DEFAULT_WORKER_JVM_MEMORY = "3G";
  public static final int DEFAULT_MIN_WORKERS = 2;
  public static final int DEFAULT_MAX_WORKERS = 4;
  public static final int DEFAULT_DESIRED_WORKERS = 2;

  public static final String DEFAULT_WORKER_JARS_POSTFIX = DEFAULT_MASTER_JARS_POSTFIX;
  public static final String DEFAULT_WORKER_S3_CONF_URI = DEFAULT_MASTER_S3_CONF_URI;
  public static final String DEFAULT_WORKER_S3_CONF_FILES = DEFAULT_MASTER_S3_CONF_FILES;
  public static final String DEFAULT_WORKER_S3_JARS_URI = DEFAULT_MASTER_S3_JARS_URI;
  public static final String DEFAULT_WORKER_S3_JARS_FILES = DEFAULT_MASTER_S3_JARS_FILES;

  // Resource/dependencies configuration properties.
  public static final String DEFAULT_LOGS_SINK_ROOT_DIR_POSTFIX = "logs";

  // Work environment properties.
  public static final String DEFAULT_APP_WORK_DIR_POSTFIX = "work.dir";
}
