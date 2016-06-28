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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.mail.EmailException;
import org.apache.helix.Criteria;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
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
import com.google.common.util.concurrent.ListeningExecutorService;
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
 *   This class, upon starting, will check if there's an AWS Cluster that it has previously running and
 *   it is able to reconnect to. More specifically, it checks if an cluster with the same cluster name
 *   exists and can be reconnected to, i.e., if the cluster has not completed yet. If so, it simply starts
 *   monitoring that cluster.
 * </p>
 *
 * <p>
 *   On the other hand, if there's no such a reconnectable AWS cluster, This class will launch a new AWS
 *   cluster and start the {@link GobblinAWSClusterMaster}. It also persists the new cluster details so it
 *   is able to reconnect to the AWS cluster if it is restarted for some reason. Once the cluster is
 *   launched, this class starts to monitor the cluster by periodically polling the status of the cluster
 *   through a {@link ListeningExecutorService}.
 * </p>
 *
 * <p>
 *   If a shutdown signal is received, it sends a Helix
 *   {@link org.apache.helix.model.Message.MessageType#SCHEDULER_MSG} to the {@link GobblinAWSClusterMaster}
 *   asking it to shutdown and release all the allocated containers. It also sends an email notification for
 *   the shutdown if {@link GobblinAWSConfigurationKeys#EMAIL_NOTIFICATION_ON_SHUTDOWN_KEY} is {@code true}.
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

  // A generator for an integer ID of a Helix instance (participant)
  private final AtomicInteger helixInstanceIdGenerator = new AtomicInteger(0);

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
   * @throws IOException if there's something wrong launching the cluster
   */
  public void launch() throws IOException {
    this.eventBus.register(this);

    // Create Helix cluster and connect to it
    String helixClusterName = this.config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
    HelixUtils
        .createGobblinHelixCluster(this.config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY),
            helixClusterName);
    LOGGER.info("Created Helix cluster " + helixClusterName);

    connectHelixManager();

    // Start all the services running
    // TODO: Add log copier service
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
   * @throws IOException if this {@link GobblinAWSClusterLauncher} instance fails to clean up its working directory.
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
    Optional<String> reconnectableClusterId = getReconnectableClusterId();
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
   * @throws IOException if there's anything wrong setting up the AWS cluster
   */
  @VisibleForTesting
  String setupGobblinCluster() throws IOException {

    String uuid = UUID.randomUUID().toString();

    // Create security group
    // TODO: Make security group restrictive and permission set configurable
    String securityGroupName = "GobblinSecurityGroup_" + uuid;
    AWSSdkClient.createSecurityGroup(this.awsClusterSecurityManager, Region.getRegion(Regions.fromName(this.awsRegion)),
        securityGroupName, "Gobblin cluster security group");
    AWSSdkClient.addPermissionsToSecurityGroup(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)), securityGroupName, "0.0.0.0/0", "tcp", 0, 65535);

    // Create key value pair
    String keyName = "GobblinKey_" + uuid;
    String material = AWSSdkClient.createKeyValuePair(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        keyName);
    LOGGER.info("Material is: " + material);
    FileUtils.writeStringToFile(new File(keyName + ".pem"), material);

    // Get all availability zones in the region. Currently, we will only use first
    List<AvailabilityZone> availabilityZones = AWSSdkClient.getAvailabilityZones(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)));

    // Launch Cluster Master
    String clusterId = launchClusterMaster(uuid, keyName, securityGroupName, availabilityZones.get(0));

    // Launch WorkUnit runners
    launchWorkUnitRunners(uuid, keyName, securityGroupName, availabilityZones.get(0));

    return clusterId;
  }

  private String launchClusterMaster(String uuid, String keyName, String securityGroups,
      AvailabilityZone availabilityZone) {
    String userData = buildClusterMasterCommand(this.masterJvmMemory);

    // Create launch config for Cluster master
    String launchConfigName = "GobblinMasterLaunchConfig_" + uuid;
    AWSSdkClient.createLaunchConfig(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        launchConfigName,
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
    // TODO: Make size configurable when we have a solid multi-master story
    int minNumMasters = 1;
    int maxNumMasters = 1;
    int desiredNumMasters = 1;
    String autoscalingGroupName = "GobblinMasterASG_" + uuid;
    Tag tag = new Tag().withKey("GobblinMaster").withValue(uuid);
    AWSSdkClient.createAutoScalingGroup(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        autoscalingGroupName,
        launchConfigName,
        minNumMasters,
        maxNumMasters,
        desiredNumMasters,
        Optional.of(availabilityZone.getZoneName()),
        Optional.<Integer>absent(),
        Optional.<Integer>absent(),
        Optional.<String>absent(),
        Optional.<String>absent(),
        Optional.<String>absent(),
        Lists.newArrayList(tag));

    LOGGER.info("Waiting for cluster master to launch");
    long startTime = System.currentTimeMillis();
    long launchTimeout = TimeUnit.MINUTES.toMillis(10);
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
          autoscalingGroupName,
          "running");
      isMasterLaunched = instanceIds.size() > 0;
    }

    if (!isMasterLaunched) {
      throw new RuntimeException("Timed out while waiting for cluster master. "
          + "Check for issue manually for ASG: " + autoscalingGroupName);
    }

    // This will change if cluster master restarts, but that will be handled by Helix events
    this.masterPublicIp = instanceIds.get(0).getPublicIpAddress();

    return "GobblinClusterMaster_" + uuid;
  }

  private void launchWorkUnitRunners(String uuid, String keyName,
      String securityGroups,
      AvailabilityZone availabilityZone) {
    String userData = buildClusterWorkerCommand(this.workerJvmMemory);

    // Create launch config for Cluster master
    String launchConfigName = "GobblinWorkerLaunchConfig_" + uuid;
    AWSSdkClient.createLaunchConfig(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        launchConfigName,
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
    String autoscalingGroupName = "GobblinWorkerASG_" + uuid;
    Tag tag = new Tag().withKey("GobblinWorker").withValue(uuid);
    AWSSdkClient.createAutoScalingGroup(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)),
        autoscalingGroupName,
        launchConfigName,
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

  private String buildClusterMasterCommand(String memory) {
    StringBuilder userDataCmds = new StringBuilder().append("#!/bin/bash").append("\n");

    String clusterMasterClassName = GobblinAWSClusterMaster.class.getSimpleName();

    // Create NFS server
    // TODO: Replace with EFS when available in GA
    // Note: Until EFS availability, ClusterMaster is SPOF because we loose NFS when it's relaunched / replaced
    //       .. this can be worked around, but would be an un-necessary work
    String nfsDir = this.nfsParentDir + this.clusterName;

    String nfsShareDirCmd = String.format("echo '%s %s(%s)' | tee --append %s",
        nfsDir, NFS_SHARE_ALL_IPS, NFS_SHARE_DEFAULT_OPTS, NFS_CONF_FILE);
    userDataCmds.append("mkdir -p ").append(nfsDir).append(File.separator).append("1").append("\n");
    userDataCmds.append(NFS_SERVER_INSTALL_CMD).append("\n");
    userDataCmds.append(nfsShareDirCmd).append("\n");
    userDataCmds.append(NFS_SERVER_START_CMD).append("\n");
    userDataCmds.append(NFS_EXPORT_FS_CMD).append("\n");

    // Create various directories
    userDataCmds.append("mkdir -p ").append(this.sinkLogRootDir).append("\n");
    userDataCmds.append("chown -R ec2-user:ec2-user /home/ec2-user/*").append("\n");

    // Setup variables to save userdata space
    userDataCmds.append("cgS3=").append(this.masterS3ConfUri).append("\n");
    userDataCmds.append("cg=").append(this.awsConfDir).append("\n");
    userDataCmds.append("jrS3=").append(this.masterS3JarsUri).append("\n");
    userDataCmds.append("jr=").append(this.masterJarsDir).append("\n");

    // Download configurations from S3
    StringBuilder classpath = new StringBuilder();
    List<String> awsConfs = SPLITTER.splitToList(this.masterS3ConfFiles);
    for (String awsConf : awsConfs) {
      userDataCmds.append(String.format("wget -P \"${cg}\" \"${cgS3}\"%s", awsConf)).append("\n");
    }
    classpath.append(this.awsConfDir);

    // Download jars from S3
    // TODO: Limit only custom user jars to pulled from S3, load rest from AMI
    List<String> awsJars = SPLITTER.splitToList(this.masterS3JarsFiles);
    for (String awsJar : awsJars) {
      userDataCmds.append(String.format("wget -P \"${jr}\" \"${jrS3}\"%s", awsJar)).append("\n");
    }
    classpath.append(":").append(this.masterJarsDir).append("*");

    // Launch Gobblin Cluster Master
    StringBuilder launchGobblinClusterMasterCmd = new StringBuilder()
        .append("java")
        .append(" -cp ").append(classpath)
        .append(" -Xmx").append(memory)
        .append(" ").append(this.masterJvmArgs.or(""))
        .append(" ").append(GobblinAWSClusterMaster.class.getName())
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
        .append(" ").append(this.clusterName)
        .append(" 1>").append(this.sinkLogRootDir)
            .append(clusterMasterClassName).append(".")
            .append(this.masterPublicIp).append(".")
            .append(GobblinAWSClusterLauncher.STDOUT)
        .append(" 2>").append(this.sinkLogRootDir)
            .append(clusterMasterClassName).append(".")
            .append(this.masterPublicIp).append(".")
            .append(GobblinAWSClusterLauncher.STDERR);
    userDataCmds.append(launchGobblinClusterMasterCmd).append("\n");

    String userData = userDataCmds.toString();
    LOGGER.info("Userdata for master node: " + userData);

    return encodeBase64(userData);
  }

  private String buildClusterWorkerCommand(String memory) {
    StringBuilder userDataCmds = new StringBuilder().append("#!/bin/bash").append("\n");

    String clusterWorkerClassName = GobblinAWSClusterMaster.class.getSimpleName();

    // Connect to NFS server
    // TODO: Replace with EFS when available in GA
    String nfsDir = this.nfsParentDir + this.clusterName;
    String nfsMountCmd = String.format("mount -t %s %s:%s %s", NFS_TYPE_4, this.masterPublicIp, nfsDir,
        nfsDir);
    userDataCmds.append("mkdir -p ").append(nfsDir).append("\n");
    userDataCmds.append(nfsMountCmd).append("\n");

    // Create various other directories
    userDataCmds.append("mkdir -p ").append(this.sinkLogRootDir).append("\n");
    userDataCmds.append("chown -R ec2-user:ec2-user /home/ec2-user/*").append("\n");

    // Setup variables to save userdata space
    userDataCmds.append("cgS3=").append(this.workerS3ConfUri).append("\n");
    userDataCmds.append("cg=").append(this.awsConfDir).append("\n");
    userDataCmds.append("jrS3=").append(this.workerS3JarsUri).append("\n");
    userDataCmds.append("jr=").append(this.workerJarsDir).append("\n");

    // Download configurations from S3
    StringBuilder classpath = new StringBuilder();
    List<String> awsConfs = SPLITTER.splitToList(this.workerS3ConfFiles);
    for (String awsConf : awsConfs) {
      userDataCmds.append(String.format("wget -P \"${cg}\" \"${cgS3}\"%s", awsConf)).append("\n");
    }
    classpath.append(this.awsConfDir);

    // Download jars from S3
    // TODO: Limit only custom user jars to pulled from S3, load rest from AMI
    List<String> awsJars = SPLITTER.splitToList(this.workerS3JarsFiles);
    for (String awsJar : awsJars) {
      userDataCmds.append(String.format("wget -P \"${jr}\" \"${jrS3}\"%s", awsJar)).append("\n");
    }
    classpath.append(":").append(this.workerJarsDir).append("*");

    String helixInstanceName = HelixUtils.getHelixInstanceName(GobblinAWSTaskRunner.class.getSimpleName(),
        helixInstanceIdGenerator.incrementAndGet());

    // Launch Gobblin Worker
    StringBuilder launchGobblinClusterWorkerCmd = new StringBuilder()
        .append("java")
        .append(" -cp ").append(classpath)
        .append(" -Xmx").append(memory)
        .append(" ").append(this.workerJvmArgs.or(""))
        .append(" ").append(GobblinAWSTaskRunner.class.getName())
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
        .append(" ").append(this.clusterName)
        .append(" --").append(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME)
        .append(" ").append(helixInstanceName)
        .append(" 1>").append(this.sinkLogRootDir)
            .append(clusterWorkerClassName).append(".")
            .append(helixInstanceName).append(".")
            .append(GobblinAWSClusterLauncher.STDOUT)
        .append(" 2>").append(this.sinkLogRootDir)
            .append(clusterWorkerClassName).append(".")
            .append(helixInstanceName).append(".")
            .append(GobblinAWSClusterLauncher.STDERR);
    userDataCmds.append(launchGobblinClusterWorkerCmd);

    String userData = userDataCmds.toString();
    LOGGER.info("Userdata for worker node: " + userData);

    return encodeBase64(userData);
  }

  @FindbugsSuppressWarnings("DM_DEFAULT_ENCODING")
  private String encodeBase64(String data) {
    byte[] encodedBytes = Base64.encodeBase64(data.getBytes());

    return new String(encodedBytes);
  }

  /***
   * List and generate classpath string from paths
   *
   * Note: This is currently unused, and will be brought in use with custom Gobblin AMI
   *
   * @param paths Paths to list
   * @return Classpath string
   */
  private String getClasspathFromPaths(File... paths) {
    StringBuilder classpath = new StringBuilder();
    boolean isFirst = true;
    for (File path : paths) {
      if (!isFirst) {
        classpath.append(":");
      }
      String subClasspath = getClasspathFromPath(path);
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
    Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setResource("%");
    criteria.setPartition("%");
    criteria.setPartitionState("%");
    criteria.setRecipientInstanceType(InstanceType.CONTROLLER);
    criteria.setSessionSpecific(true);

    Message shutdownRequest = new Message(Message.MessageType.SHUTDOWN,
        HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString().toLowerCase() + UUID.randomUUID().toString());
    shutdownRequest.setMsgSubType(HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString());
    shutdownRequest.setMsgState(Message.MessageState.NEW);
    shutdownRequest.setTgtSessionId("*");

    int messagesSent = this.helixManager.getMessagingService().send(criteria, shutdownRequest);
    if (messagesSent == 0) {
      LOGGER.error(String.format("Failed to send the %s message to the controller", shutdownRequest.getMsgSubType()));
    }
  }

  private void cleanUpClusterWorkDirectory(String clusterId) throws IOException {
    File appWorkDir = new File(GobblinClusterUtils.getAppWorkDirPath(this.clusterName, clusterId));

    if (appWorkDir.exists() && appWorkDir.isDirectory()) {
      LOGGER.info("Deleting application working directory " + appWorkDir);
      FileUtils.deleteDirectory(appWorkDir);
    }
  }

  private void sendEmailOnShutdown(Optional<String> report) {
    String subject = String.format("Gobblin AWS cluster %s completed", this.clusterName);

    StringBuilder messageBuilder = new StringBuilder("Gobblin AWS cluster was shutdown at: " + new Date());
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
