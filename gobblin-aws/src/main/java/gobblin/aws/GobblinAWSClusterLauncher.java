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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.mail.EmailException;
import org.apache.helix.Criteria;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.model.Message;
import org.quartz.utils.FindbugsSuppressWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.autoscaling.model.BlockDeviceMapping;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;
import com.amazonaws.services.autoscaling.model.Tag;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.Instance;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.cluster.GobblinClusterConfigurationKeys;
import gobblin.cluster.GobblinClusterUtils;
import gobblin.cluster.HelixMessageSubTypes;
import gobblin.cluster.HelixUtils;
import gobblin.util.ConfigUtils;
import gobblin.util.EmailUtils;


/**
 * A client driver to launch Gobblin as an AWS Cluster.
 *
 * <p>
 *   This class upon starting, will check if there is an AWS Cluster that is already running and
 *   it is able to reconnect to. More specifically, it checks if an cluster with the same cluster name
 *   exists and can be reconnected to i.e. if the cluster has not completed yet. If so, it simply starts
 *   monitoring that cluster.
 * </p>
 *
 * <p>
 *   On the other hand, if there's no such a reconnectable AWS cluster, This class will launch a new AWS
 *   cluster and start the {@link GobblinAWSClusterMaster}. It also persists the new cluster details so it
 *   is able to reconnect to the AWS cluster if it is restarted for some reason.
 * </p>
 *
 * <p>
 *   If a shutdown signal is received, it sends a Helix
 *   {@link org.apache.helix.model.Message.MessageType#SCHEDULER_MSG} to the {@link GobblinAWSClusterMaster}
 *   asking it to shutdown. It also sends an email notification for the shutdown if
 *   {@link GobblinAWSConfigurationKeys#EMAIL_NOTIFICATION_ON_SHUTDOWN_KEY} is {@code true}.
 * </p>
 *
 * @author Abhishek Tiwari
 */
public class GobblinAWSClusterLauncher {
  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinAWSClusterLauncher.class);

  private static final Splitter SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();

  private static final String STDOUT = "stdout";
  private static final String STDERR = "stderr";
  private static final String NFS_SHARE_ALL_IPS = "*";
  private static final String NFS_SHARE_DEFAULT_OPTS = "rw,sync,no_subtree_check,fsid=1,no_root_squash";
  private static final String NFS_CONF_FILE = "/etc/exports";
  private static final String NFS_SERVER_INSTALL_CMD = "yum install nfs-utils nfs-utils-lib";
  private static final String NFS_SERVER_START_CMD = "/etc/init.d/nfs start";
  private static final String NFS_EXPORT_FS_CMD = "exportfs -a";
  private static final String NFS_TYPE_4 = "nfs4";

  private final Config config;

  private final HelixManager helixManager;
  private final EventBus eventBus = new EventBus(GobblinAWSClusterLauncher.class.getSimpleName());
  private volatile Optional<ServiceManager> serviceManager = Optional.absent();
  private AWSClusterSecurityManager awsClusterSecurityManager;

  private final Closer closer = Closer.create();

  // AWS cluster meta
  private final String clusterName;
  private volatile Optional<String> clusterId = Optional.absent();

  private volatile boolean stopped = false;
  private final boolean emailNotificationOnShutdown;

  // AWS Gobblin cluster common config
  private final String awsRegion;
  private final String awsConfDir;

  // AWS Gobblin Master Instance config
  private final String masterAmiId;
  private final String masterInstanceType;
  private final String masterJvmMemory;

  // AWS Gobblin Worker Instance config
  private final String workerAmiId;
  private final String workerInstanceType;
  private final String workerJvmMemory;
  private final Integer minWorkers;
  private final Integer maxWorkers;
  private final Integer desiredWorkers;

  private final Optional<String> masterJvmArgs;
  private final Optional<String> workerJvmArgs;

  private String masterPublicIp;

  private final String nfsParentDir;
  private final String masterJarsDir;
  private final String masterConfLocalDir;
  private final String masterS3ConfUri;
  private final String masterS3ConfFiles;
  private final String masterS3JarsUri;
  private final String masterS3JarsFiles;

  private final String workerJarsDir;
  private final String workerConfLocalDir;
  private final String workerS3ConfUri;
  private final String workerS3ConfFiles;
  private final String workerS3JarsUri;
  private final String workerS3JarsFiles;

  private final String libJarsDir;
  private final String sinkLogRootDir;
  private final String appWorkDir;

  private String masterLaunchConfigName;
  private String masterAutoScalingGroupName;

  private String workerLaunchConfigName;
  private String workerAutoScalingGroupName;


  public GobblinAWSClusterLauncher(Config config) throws IOException {
    this.config = config;

    this.clusterName = config.getString(GobblinAWSConfigurationKeys.CLUSTER_NAME_KEY);

    String zkConnectionString = config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    LOGGER.info("Using ZooKeeper connection string: " + zkConnectionString);

    this.helixManager = HelixManagerFactory
        .getZKHelixManager(config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY),
            GobblinClusterUtils.getHostname(), InstanceType.SPECTATOR, zkConnectionString);

    this.awsRegion = config.getString(GobblinAWSConfigurationKeys.AWS_REGION_KEY);
    this.awsConfDir = appendSlash(config.getString(GobblinAWSConfigurationKeys.AWS_CONF_DIR));

    this.masterAmiId = config.getString(GobblinAWSConfigurationKeys.MASTER_AMI_ID_KEY);
    this.masterInstanceType = config.getString(GobblinAWSConfigurationKeys.MASTER_INSTANCE_TYPE_KEY);
    this.masterJvmMemory = config.getString(GobblinAWSConfigurationKeys.MASTER_JVM_MEMORY_KEY);

    this.workerAmiId = config.getString(GobblinAWSConfigurationKeys.WORKER_AMI_ID_KEY);
    this.workerInstanceType = config.getString(GobblinAWSConfigurationKeys.WORKER_INSTANCE_TYPE_KEY);
    this.workerJvmMemory = config.getString(GobblinAWSConfigurationKeys.WORKER_JVM_MEMORY_KEY);
    this.minWorkers = config.getInt(GobblinAWSConfigurationKeys.MIN_WORKERS_KEY);
    this.maxWorkers = config.getInt(GobblinAWSConfigurationKeys.MAX_WORKERS_KEY);
    this.desiredWorkers = config.getInt(GobblinAWSConfigurationKeys.DESIRED_WORKERS_KEY);

    this.masterJvmArgs = config.hasPath(GobblinAWSConfigurationKeys.MASTER_JVM_ARGS_KEY) ?
        Optional.of(config.getString(GobblinAWSConfigurationKeys.MASTER_JVM_ARGS_KEY)) :
        Optional.<String>absent();
    this.workerJvmArgs = config.hasPath(GobblinAWSConfigurationKeys.WORKER_JVM_ARGS_KEY) ?
        Optional.of(config.getString(GobblinAWSConfigurationKeys.WORKER_JVM_ARGS_KEY)) :
        Optional.<String>absent();

    this.nfsParentDir = appendSlash(config.getString(GobblinAWSConfigurationKeys.NFS_PARENT_DIR_KEY));

    this.masterJarsDir = appendSlash(config.getString(GobblinAWSConfigurationKeys.MASTER_JARS_KEY));
    this.masterConfLocalDir = appendSlash(config.getString(GobblinAWSConfigurationKeys.MASTER_CONF_LOCAL_KEY));
    this.masterS3ConfUri = appendSlash(config.getString(GobblinAWSConfigurationKeys.MASTER_S3_CONF_URI_KEY));
    this.masterS3ConfFiles = config.getString(GobblinAWSConfigurationKeys.MASTER_S3_CONF_FILES_KEY);
    this.masterS3JarsUri = config.getString(GobblinAWSConfigurationKeys.MASTER_S3_JARS_URI_KEY);
    this.masterS3JarsFiles = config.getString(GobblinAWSConfigurationKeys.MASTER_S3_JARS_FILES_KEY);

    this.workerJarsDir = appendSlash(config.getString(GobblinAWSConfigurationKeys.WORKER_JARS_KEY));
    this.workerConfLocalDir = appendSlash(config.getString(GobblinAWSConfigurationKeys.WORKER_CONF_LOCAL_KEY));
    this.workerS3ConfUri = appendSlash(config.getString(GobblinAWSConfigurationKeys.WORKER_S3_CONF_URI_KEY));
    this.workerS3ConfFiles = config.getString(GobblinAWSConfigurationKeys.WORKER_S3_CONF_FILES_KEY);
    this.workerS3JarsUri = config.getString(GobblinAWSConfigurationKeys.WORKER_S3_JARS_URI_KEY);
    this.workerS3JarsFiles = config.getString(GobblinAWSConfigurationKeys.WORKER_S3_JARS_FILES_KEY);

    this.libJarsDir = appendSlash(config.getString(GobblinAWSConfigurationKeys.LIB_JARS_DIR_KEY));
    this.sinkLogRootDir = appendSlash(config.getString(GobblinAWSConfigurationKeys.LOGS_SINK_ROOT_DIR_KEY));
    this.appWorkDir = appendSlash(config.getString(GobblinAWSConfigurationKeys.APP_WORK_DIR));

    this.emailNotificationOnShutdown =
        config.getBoolean(GobblinAWSConfigurationKeys.EMAIL_NOTIFICATION_ON_SHUTDOWN_KEY);
  }

  private String appendSlash(String value) {
    if (value.endsWith("/")) {
      return value;
    }
    return value + "/";
  }

  /**
   * Launch a new Gobblin cluster on AWS.
   *
   * @throws IOException If there's something wrong launching the cluster
   */
  public void launch() throws IOException {
    this.eventBus.register(this);

    // Create Helix cluster and connect to it
    final String helixClusterName = this.config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
    HelixUtils
        .createGobblinHelixCluster(this.config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY),
            helixClusterName);
    LOGGER.info("Created Helix cluster " + helixClusterName);

    connectHelixManager();

    // Start all the services
    List<Service> services = Lists.newArrayList();
    this.awsClusterSecurityManager = new AWSClusterSecurityManager(this.config);
    services.add(this.awsClusterSecurityManager);
    this.serviceManager = Optional.of(new ServiceManager(services));
    this.serviceManager.get().startAsync();

    // Core logic to launch cluster
    this.clusterId = getClusterId();

    // TODO: Add cluster monitoring
  }

  /**
   * Stop this {@link GobblinAWSClusterLauncher} instance.
   *
   * @throws IOException If this {@link GobblinAWSClusterLauncher} instance fails to clean up its working directory.
   */
  public synchronized void stop() throws IOException, TimeoutException {
    if (this.stopped) {
      return;
    }

    LOGGER.info("Stopping the " + GobblinAWSClusterLauncher.class.getSimpleName());

    try {
      if (this.clusterId.isPresent()) {
        sendShutdownRequest();
      }

      if (this.serviceManager.isPresent()) {
        this.serviceManager.get().stopAsync().awaitStopped(5, TimeUnit.MINUTES);
      }

      disconnectHelixManager();
    } finally {
      try {
        if (this.clusterId.isPresent()) {
           cleanUpClusterWorkDirectory(this.clusterId.get());
        }
      } finally {
        this.closer.close();
      }
    }

    this.stopped = true;
  }

  @VisibleForTesting
  void connectHelixManager() {
    try {
      this.helixManager.connect();
    } catch (Exception e) {
      LOGGER.error("HelixManager failed to connect", e);
      throw Throwables.propagate(e);
    }
  }

  @VisibleForTesting
  void disconnectHelixManager() {
    if (this.helixManager.isConnected()) {
      this.helixManager.disconnect();
    }
  }

  private Optional<String> getClusterId() throws IOException {
    final Optional<String> reconnectableClusterId = getReconnectableClusterId();
    if (reconnectableClusterId.isPresent()) {
      LOGGER.info("Found reconnectable cluster with cluster ID: " + reconnectableClusterId.get());
      return reconnectableClusterId;
    }

    LOGGER.info("No reconnectable cluster found so creating a cluster");
    return Optional.of(setupGobblinCluster());
  }

  @VisibleForTesting
  Optional<String> getReconnectableClusterId() throws IOException {
    // TODO: Discover all available ASG's and reconnect if there is an ClusterMaster

    return Optional.absent();
  }

  /**
   * Setup the Gobblin AWS cluster.
   *
   * @throws IOException If there's anything wrong setting up the AWS cluster
   */
  @VisibleForTesting
  String setupGobblinCluster() throws IOException {

    final String uuid = UUID.randomUUID().toString();

    // Create security group
    // TODO: Make security group restrictive
    final String securityGroupName = "GobblinSecurityGroup_" + uuid;
    AWSSdkClient.createSecurityGroup(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        securityGroupName,
        "Gobblin cluster security group");
    AWSSdkClient.addPermissionsToSecurityGroup(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        securityGroupName,
        "0.0.0.0/0",
        "tcp",
        0,
        65535);

    // Create key value pair
    final String keyName = "GobblinKey_" + uuid;
    final String material = AWSSdkClient.createKeyValuePair(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        keyName);
    LOGGER.info("Material is: " + material);
    FileUtils.writeStringToFile(new File(keyName + ".pem"), material);

    // Get all availability zones in the region. Currently, we will only use first
    final List<AvailabilityZone> availabilityZones = AWSSdkClient.getAvailabilityZones(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)));

    // Launch Cluster Master
    final String clusterId = launchClusterMaster(uuid, keyName, securityGroupName, availabilityZones.get(0));

    // Launch WorkUnit runners
    launchWorkUnitRunners(uuid, keyName, securityGroupName, availabilityZones.get(0));

    return clusterId;
  }

  private String launchClusterMaster(String uuid, String keyName, String securityGroups,
      AvailabilityZone availabilityZone) {
    final String userData = buildClusterMasterCommand();

    // Create launch config for Cluster master
    this.masterLaunchConfigName = "GobblinMasterLaunchConfig_" + uuid;
    AWSSdkClient.createLaunchConfig(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        this.masterLaunchConfigName,
        this.masterAmiId,
        this.masterInstanceType,
        keyName,
        securityGroups,
        Optional.<String>absent(),
        Optional.<String>absent(),
        Optional.<BlockDeviceMapping>absent(),
        Optional.<String>absent(),
        Optional.<InstanceMonitoring>absent(),
        userData);

    // Create ASG for Cluster master
    // TODO: Make size configurable when we have support multi-master
    this.masterAutoScalingGroupName = "GobblinMasterASG_" + uuid;
    final int minNumMasters = 1;
    final int maxNumMasters = 1;
    final int desiredNumMasters = 1;
    final Tag tag = new Tag().withKey("GobblinMaster").withValue(uuid);
    AWSSdkClient.createAutoScalingGroup(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        this.masterAutoScalingGroupName,
        this.masterLaunchConfigName,
        minNumMasters, maxNumMasters,
        desiredNumMasters,
        Optional.of(availabilityZone.getZoneName()),
        Optional.<Integer>absent(),
        Optional.<Integer>absent(),
        Optional.<String>absent(),
        Optional.<String>absent(),
        Optional.<String>absent(),
        Lists.newArrayList(tag));

    LOGGER.info("Waiting for cluster master to launch");
    final long startTime = System.currentTimeMillis();
    final long launchTimeout = TimeUnit.MINUTES.toMillis(10);
    boolean isMasterLaunched = false;
    List<Instance> instanceIds = Collections.emptyList();
    while (!isMasterLaunched && (System.currentTimeMillis() - startTime) < launchTimeout) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting for cluster master to boot up", e);
      }
      instanceIds = AWSSdkClient.getInstancesForGroup(this.awsClusterSecurityManager,
          Region.getRegion(Regions.fromName(this.awsRegion)),
          this.masterAutoScalingGroupName,
          "running");
      isMasterLaunched = instanceIds.size() > 0;
    }

    if (!isMasterLaunched) {
      throw new RuntimeException("Timed out while waiting for cluster master. "
          + "Check for issue manually for ASG: " + this.masterAutoScalingGroupName);
    }

    // This will change if cluster master restarts, but that will be handled by Helix events
    // TODO: Add listener to Helix / Zookeeper for master restart and update master public ip
    // .. although tracking master public ip is not important
    this.masterPublicIp = instanceIds.get(0).getPublicIpAddress();

    return "GobblinClusterMaster_" + uuid;
  }

  private void launchWorkUnitRunners(String uuid, String keyName,
      String securityGroups,
      AvailabilityZone availabilityZone) {
    final String userData = buildClusterWorkerCommand();

    // Create launch config for Cluster worker
    this.workerLaunchConfigName = "GobblinWorkerLaunchConfig_" + uuid;
    AWSSdkClient.createLaunchConfig(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        this.workerLaunchConfigName,
        this.workerAmiId,
        this.workerInstanceType,
        keyName,
        securityGroups,
        Optional.<String>absent(),
        Optional.<String>absent(),
        Optional.<BlockDeviceMapping>absent(),
        Optional.<String>absent(),
        Optional.<InstanceMonitoring>absent(),
        userData);

    // Create ASG for Cluster workers
    this.workerAutoScalingGroupName = "GobblinWorkerASG_" + uuid;
    final Tag tag = new Tag().withKey("GobblinWorker").withValue(uuid);
    AWSSdkClient.createAutoScalingGroup(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        this.workerAutoScalingGroupName,
        this.workerLaunchConfigName,
        this.minWorkers,
        this.maxWorkers,
        this.desiredWorkers,
        Optional.of(availabilityZone.getZoneName()),
        Optional.<Integer>absent(),
        Optional.<Integer>absent(),
        Optional.<String>absent(),
        Optional.<String>absent(),
        Optional.<String>absent(),
        Lists.newArrayList(tag));
  }

  /***
   * This method generates the userdata that would be executed by cloud-init module in EC2 instance
   * upon boot up for {@link GobblinAWSClusterMaster}.
   *
   * The userdata is a shell script that does the following:
   * 1. Mount NFS Server (TODO: To be replaced with EFS soon)
   * 2. Create all prerequisite directories
   * 3. Download cluster configuration from S3
   * 4. Download gobblin application jars from S3 (TODO: To be replaced via baked in jars in custom Gobblin AMI)
   * 5. Download gobblin custom jars from S3
   * 6. Launch {@link GobblinAWSClusterMaster} java application
   * 7. TODO: Add cron that watches the {@link GobblinAWSClusterMaster} application and restarts it if it dies
   *
   * @return Userdata to launch {@link GobblinAWSClusterMaster}
   */
  private String buildClusterMasterCommand() {
    final StringBuilder userDataCmds = new StringBuilder().append("#!/bin/bash").append("\n");

    final String clusterMasterClassName = GobblinAWSClusterMaster.class.getSimpleName();

    // Create NFS server
    // TODO: Replace with EFS (it went into GA on 6/30/2016)
    // Note: Until EFS availability, ClusterMaster is SPOF because we loose NFS when it's relaunched / replaced
    //       .. this can be worked around, but would be an un-necessary work
    final String nfsDir = this.nfsParentDir + this.clusterName;

    final String nfsShareDirCmd = String.format("echo '%s %s(%s)' | tee --append %s",
        nfsDir, NFS_SHARE_ALL_IPS, NFS_SHARE_DEFAULT_OPTS, NFS_CONF_FILE);
    userDataCmds.append("mkdir -p ").append(nfsDir).append(File.separator).append("1").append("\n");
    userDataCmds.append(NFS_SERVER_INSTALL_CMD).append("\n");
    userDataCmds.append(nfsShareDirCmd).append("\n");
    userDataCmds.append(NFS_SERVER_START_CMD).append("\n");
    userDataCmds.append(NFS_EXPORT_FS_CMD).append("\n");

    // Create various directories
    userDataCmds.append("mkdir -p ").append(this.sinkLogRootDir).append("\n");
    userDataCmds.append("chown -R ec2-user:ec2-user /home/ec2-user/*").append("\n");

    // Setup short variables to save userdata space
    userDataCmds.append("cgS3=").append(this.masterS3ConfUri).append("\n");
    userDataCmds.append("cg=").append(this.awsConfDir).append("\n");
    userDataCmds.append("jrS3=").append(this.masterS3JarsUri).append("\n");
    userDataCmds.append("jr=").append(this.masterJarsDir).append("\n");

    // Download configurations from S3
    final StringBuilder classpath = new StringBuilder();
    final List<String> awsConfs = SPLITTER.splitToList(this.masterS3ConfFiles);
    for (String awsConf : awsConfs) {
      userDataCmds.append(String.format("wget -P \"${cg}\" \"${cgS3}\"%s", awsConf)).append("\n");
    }
    classpath.append(this.awsConfDir);

    // Download jars from S3
    // TODO: Eventually limit only custom user jars to pulled from S3, load rest from AMI
    final List<String> awsJars = SPLITTER.splitToList(this.masterS3JarsFiles);
    for (String awsJar : awsJars) {
      userDataCmds.append(String.format("wget -P \"${jr}\" \"${jrS3}\"%s", awsJar)).append("\n");
    }
    classpath.append(":").append(this.masterJarsDir).append("*");

    // TODO: Add cron that brings back master if it dies
    // Launch Gobblin Cluster Master
    final StringBuilder launchGobblinClusterMasterCmd = new StringBuilder()
        .append("java")
        .append(" -cp ").append(classpath)
        .append(" -Xmx").append(this.masterJvmMemory)
        .append(" ").append(this.masterJvmArgs.or(""))
        .append(" ").append(GobblinAWSClusterMaster.class.getName())
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
        .append(" ").append(this.clusterName)
        .append(" --").append(GobblinAWSConfigurationKeys.APP_WORK_DIR)
        .append(" ").append(this.appWorkDir)
        .append(" 1>").append(this.sinkLogRootDir)
            .append(clusterMasterClassName).append(".")
            .append("master").append(".")
            .append(GobblinAWSClusterLauncher.STDOUT)
        .append(" 2>").append(this.sinkLogRootDir)
            .append(clusterMasterClassName).append(".")
            .append("master").append(".")
            .append(GobblinAWSClusterLauncher.STDERR);
    userDataCmds.append(launchGobblinClusterMasterCmd).append("\n");

    final String userData = userDataCmds.toString();
    LOGGER.info("Userdata for master node: " + userData);

    return encodeBase64(userData);
  }

  /***
   * This method generates the userdata that would be executed by cloud-init module in EC2 instance
   * upon boot up for {@link GobblinAWSTaskRunner}.
   *
   * The userdata is a shell script that does the following:
   * 1. Mount NFS volume (TODO: To be replaced with EFS soon)
   * 2. Create all prerequisite directories
   * 3. Download cluster configuration from S3
   * 4. Download gobblin application jars from S3 (TODO: To be replaced via baked in jars in custom Gobblin AMI)
   * 5. Download gobblin custom jars from S3
   * 6. Launch {@link GobblinAWSTaskRunner} java application
   * 7. TODO: Add cron that watches the {@link GobblinAWSTaskRunner} application and restarts it if it dies
   *
   * @return Userdata to launch {@link GobblinAWSTaskRunner}
   */
  private String buildClusterWorkerCommand() {
    final StringBuilder userDataCmds = new StringBuilder().append("#!/bin/bash").append("\n");

    final String clusterWorkerClassName = GobblinAWSTaskRunner.class.getSimpleName();

    // Connect to NFS server
    // TODO: Replace with EFS (it went into GA on 6/30/2016)
    final String nfsDir = this.nfsParentDir + this.clusterName;
    final String nfsMountCmd = String.format("mount -t %s %s:%s %s", NFS_TYPE_4, this.masterPublicIp, nfsDir,
        nfsDir);
    userDataCmds.append("mkdir -p ").append(nfsDir).append("\n");
    userDataCmds.append(nfsMountCmd).append("\n");

    // Create various other directories
    userDataCmds.append("mkdir -p ").append(this.sinkLogRootDir).append("\n");
    userDataCmds.append("chown -R ec2-user:ec2-user /home/ec2-user/*").append("\n");

    // Setup short variables to save userdata space
    userDataCmds.append("cg0=").append(this.workerS3ConfUri).append("\n");
    userDataCmds.append("cg=").append(this.awsConfDir).append("\n");
    userDataCmds.append("jr0=").append(this.workerS3JarsUri).append("\n");
    userDataCmds.append("jr=").append(this.workerJarsDir).append("\n");

    // Download configurations from S3
    final StringBuilder classpath = new StringBuilder();
    final List<String> awsConfs = SPLITTER.splitToList(this.workerS3ConfFiles);
    for (String awsConf : awsConfs) {
      userDataCmds.append(String.format("wget -P \"${cg}\" \"${cg0}\"%s", awsConf)).append("\n");
    }
    classpath.append(this.awsConfDir);

    // Download jars from S3
    // TODO: Limit only custom user jars to pulled from S3, load rest from AMI
    final List<String> awsJars = SPLITTER.splitToList(this.workerS3JarsFiles);
    for (String awsJar : awsJars) {
      userDataCmds.append(String.format("wget -P \"${jr}\" \"${jr0}\"%s", awsJar)).append("\n");
    }
    classpath.append(":").append(this.workerJarsDir).append("*");

    // Get a random Helix instance name
    userDataCmds.append("pi=`curl http://169.254.169.254/latest/meta-data/local-ipv4`").append("\n");

    // TODO: Add cron that brings back worker if it dies
    // Launch Gobblin Worker
    final StringBuilder launchGobblinClusterWorkerCmd = new StringBuilder()
        .append("java")
        .append(" -cp ").append(classpath)
        .append(" -Xmx").append(this.workerJvmMemory)
        .append(" ").append(this.workerJvmArgs.or(""))
        .append(" ").append(GobblinAWSTaskRunner.class.getName())
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
        .append(" ").append(this.clusterName)
        .append(" --").append(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME)
        .append(" ").append("$pi")
        .append(" --").append(GobblinAWSConfigurationKeys.APP_WORK_DIR)
        .append(" ").append(this.appWorkDir)
        .append(" 1>").append(this.sinkLogRootDir)
            .append(clusterWorkerClassName).append(".")
            .append("$pi").append(".")
            .append(GobblinAWSClusterLauncher.STDOUT)
        .append(" 2>").append(this.sinkLogRootDir)
            .append(clusterWorkerClassName).append(".")
            .append("$pi").append(".")
            .append(GobblinAWSClusterLauncher.STDERR);
    userDataCmds.append(launchGobblinClusterWorkerCmd);

    final String userData = userDataCmds.toString();
    LOGGER.info("Userdata for worker node: " + userData);

    return encodeBase64(userData);
  }

  @FindbugsSuppressWarnings("DM_DEFAULT_ENCODING")
  private String encodeBase64(String data) {
    final byte[] encodedBytes = Base64.encodeBase64(data.getBytes());

    return new String(encodedBytes);
  }

  /***
   * List and generate classpath string from paths
   *
   * Note: This is currently unused, and will be brought in to use with custom Gobblin AMI
   *
   * @param paths Paths to list
   * @return Classpath string
   */
  private String getClasspathFromPaths(File... paths) {
    final StringBuilder classpath = new StringBuilder();
    boolean isFirst = true;
    for (File path : paths) {
      if (!isFirst) {
        classpath.append(":");
      }
      final String subClasspath = getClasspathFromPath(path);
      if (subClasspath.length() > 0) {
        classpath.append(subClasspath);
        isFirst = false;
      }
    }

    return classpath.toString();
  }

  private String getClasspathFromPath(File path) {
    if (null == path) {
      return StringUtils.EMPTY;
    }
    if (!path.isDirectory()) {
      return path.getAbsolutePath();
    }

    return Joiner.on(":").skipNulls().join(path.list(FileFileFilter.FILE));
  }

  @VisibleForTesting
  void sendShutdownRequest() {
    final Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setResource("%");
    criteria.setPartition("%");
    criteria.setPartitionState("%");
    criteria.setRecipientInstanceType(InstanceType.CONTROLLER);
    criteria.setSessionSpecific(true);

    final Message shutdownRequest = new Message(Message.MessageType.SHUTDOWN,
        HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString().toLowerCase() + UUID.randomUUID().toString());
    shutdownRequest.setMsgSubType(HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString());
    shutdownRequest.setMsgState(Message.MessageState.NEW);
    shutdownRequest.setTgtSessionId("*");

    // Wait for 5 minutes
    final int timeout = 300000;

    // Send shutdown request to Cluster master, which will send shutdown request to workers
    // .. master will callback shutdownASG() once it receives shutdown response from workers as well
    // .. shuts itself down
    final int messagesSent = this.helixManager.getMessagingService().sendAndWait(criteria, shutdownRequest,
        shutdownASG(),timeout);
    if (messagesSent == 0) {
      LOGGER.error(String.format("Failed to send the %s message to the controller", shutdownRequest.getMsgSubType()));
    }
  }

  /***
   * Callback method that deletes AutoScalingGroup
   * @return Callback method that deletes AutoScalingGroup
   */
  private AsyncCallback shutdownASG() {
    Optional<List<String>> optionalLaunchConfigurationNames = Optional
        .of(Arrays.asList(this.masterLaunchConfigName, this.workerLaunchConfigName));
    Optional<List<String>> optionalAutoScalingGroupNames = Optional
        .of(Arrays.asList(this.masterAutoScalingGroupName, this.workerAutoScalingGroupName));

    return new AWSShutdownHandler(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        optionalLaunchConfigurationNames,
        optionalAutoScalingGroupNames);
  }

  private void cleanUpClusterWorkDirectory(String clusterId) throws IOException {
    final File appWorkDir = new File(GobblinClusterUtils.getAppWorkDirPath(this.clusterName, clusterId));

    if (appWorkDir.exists() && appWorkDir.isDirectory()) {
      LOGGER.info("Deleting application working directory " + appWorkDir);
      FileUtils.deleteDirectory(appWorkDir);
    }
  }

  private void sendEmailOnShutdown(Optional<String> report) {
    final String subject = String.format("Gobblin AWS cluster %s completed", this.clusterName);

    final StringBuilder messageBuilder = new StringBuilder("Gobblin AWS cluster was shutdown at: " + new Date());
    if (report.isPresent()) {
      messageBuilder.append(' ').append(report.get());
    }

    try {
      EmailUtils.sendEmail(ConfigUtils.configToState(this.config), subject, messageBuilder.toString());
    } catch (EmailException ee) {
      LOGGER.error("Failed to send email notification on shutdown", ee);
    }
  }

  public static void main(String[] args) throws Exception {
    final GobblinAWSClusterLauncher gobblinAWSClusterLauncher =
        new GobblinAWSClusterLauncher(ConfigFactory.load());
    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          gobblinAWSClusterLauncher.stop();
        } catch (IOException ioe) {
          LOGGER.error("Failed to shutdown the " + GobblinAWSClusterLauncher.class.getSimpleName(), ioe);
        } catch (TimeoutException te) {
          LOGGER.error("Timeout in stopping the service manager", te);
        } finally {
          if (gobblinAWSClusterLauncher.emailNotificationOnShutdown) {
            gobblinAWSClusterLauncher.sendEmailOnShutdown(Optional.<String>absent());
          }
        }
      }
    });

    gobblinAWSClusterLauncher.launch();
  }
}
