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

import org.apache.commons.io.FileUtils;
import org.apache.commons.mail.EmailException;
import org.apache.helix.Criteria;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.BlockDeviceMapping;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;
import com.amazonaws.services.autoscaling.model.Tag;
import com.amazonaws.services.autoscaling.model.TagDescription;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.Instance;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.annotation.Alpha;
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
@Alpha
public class GobblinAWSClusterLauncher {
  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinAWSClusterLauncher.class);

  public static final String CLUSTER_NAME_ASG_TAG = "ClusterName";
  public static final String CLUSTER_ID_ASG_TAG = "ClusterId";
  public static final String ASG_TYPE_ASG_TAG = "AsgType";
  public static final String ASG_TYPE_MASTER = "master";
  public static final String ASG_TYPE_WORKERS = "workers";

  public static final String MASTER_ASG_NAME_PREFIX = "GobblinMasterASG_";
  public static final String MASTER_LAUNCH_CONFIG_NAME_PREFIX = "GobblinMasterLaunchConfig_";
  public static final String WORKERS_ASG_NAME_PREFIX = "GobblinWorkerASG_";
  public static final String WORKERS_LAUNCH_CONFIG_PREFIX = "GobblinWorkerLaunchConfig_";

  private final Config config;

  private final HelixManager helixManager;
  private final EventBus eventBus = new EventBus(GobblinAWSClusterLauncher.class.getSimpleName());
  private volatile Optional<ServiceManager> serviceManager = Optional.absent();
  private AWSClusterSecurityManager awsClusterSecurityManager;
  private AWSSdkClient awsSdkClient;

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

    this.awsClusterSecurityManager = new AWSClusterSecurityManager(this.config);
    this.awsSdkClient = createAWSSdkClient();
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

  @VisibleForTesting
  AWSSdkClient createAWSSdkClient() {
    return new AWSSdkClient(this.awsClusterSecurityManager,
        Region.getRegion(Regions.fromName(this.awsRegion)));
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
    // List ASGs with Tag of cluster name
    final Tag clusterNameTag = new Tag()
        .withKey(CLUSTER_NAME_ASG_TAG)
        .withValue(this.clusterName);
    final List<AutoScalingGroup> autoScalingGroups = this.awsSdkClient.getAutoScalingGroupsWithTag(clusterNameTag);

    // If no auto scaling group is found, we don't have an existing cluster to connect to
    if (autoScalingGroups.size() == 0) {
      return Optional.absent();
    }

    // If more than 0 auto scaling groups are found, validate the setup
    if (autoScalingGroups.size() != 2) {
      throw new IOException("Expected 2 auto scaling groups (1 each for master and workers) but found: " +
        autoScalingGroups.size());
    }

    // Retrieve cluster information from ASGs
    Optional<String> clusterId = Optional.absent();
    Optional<AutoScalingGroup> masterAsg = Optional.absent();
    Optional<AutoScalingGroup> workersAsg = Optional.absent();

    for (TagDescription tagDescription : autoScalingGroups.get(0).getTags()) {
      LOGGER.info("Found tag: " + tagDescription);
      if (tagDescription.getKey().equalsIgnoreCase(CLUSTER_ID_ASG_TAG)) {
        clusterId = Optional.of(tagDescription.getValue());
      }
      if (tagDescription.getKey().equalsIgnoreCase(ASG_TYPE_ASG_TAG)) {
        if (tagDescription.getValue().equalsIgnoreCase(ASG_TYPE_MASTER)) {
          masterAsg = Optional.of(autoScalingGroups.get(0));
          workersAsg = Optional.of(autoScalingGroups.get(1));
        } else {
          masterAsg = Optional.of(autoScalingGroups.get(1));
          workersAsg = Optional.of(autoScalingGroups.get(0));
        }
      }
    }

    if (!clusterId.isPresent()) {
      throw new IOException("Found 2 auto scaling group names for: " + this.clusterName +
          " but tags seem to be corrupted, hence could not determine cluster id");
    }

    if (!masterAsg.isPresent() || !workersAsg.isPresent()) {
      throw new IOException("Found 2 auto scaling group names for: " + this.clusterName +
      " but tags seem to be corrupted, hence could not determine master and workers ASG");
    }

    // Get Master and Workers launch config name and auto scaling group name
    this.masterAutoScalingGroupName = masterAsg.get().getAutoScalingGroupName();
    this.masterLaunchConfigName = masterAsg.get().getLaunchConfigurationName();
    this.workerAutoScalingGroupName = workersAsg.get().getAutoScalingGroupName();
    this.workerLaunchConfigName = workersAsg.get().getLaunchConfigurationName();

    LOGGER.info("Trying to find cluster master public ip");
    this.masterPublicIp = getMasterPublicIp();
    LOGGER.info("Master public ip: "+ this.masterPublicIp);

    return clusterId;
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
    this.awsSdkClient.createSecurityGroup(securityGroupName, "Gobblin cluster security group");
    this.awsSdkClient.addPermissionsToSecurityGroup(securityGroupName,
        "0.0.0.0/0",
        "tcp",
        0,
        65535);

    // Create key value pair
    final String keyName = "GobblinKey_" + uuid;
    final String material = this.awsSdkClient.createKeyValuePair(keyName);
    LOGGER.debug("Material is: " + material);
    FileUtils.writeStringToFile(new File(keyName + ".pem"), material);

    // Get all availability zones in the region. Currently, we will only use first
    final List<AvailabilityZone> availabilityZones = this.awsSdkClient.getAvailabilityZones();

    // Launch Cluster Master
    final String clusterId = launchClusterMaster(uuid, keyName, securityGroupName, availabilityZones.get(0));

    // Launch WorkUnit runners
    launchWorkUnitRunners(uuid, keyName, securityGroupName, availabilityZones.get(0));

    return clusterId;
  }

  private String launchClusterMaster(String uuid, String keyName, String securityGroups,
      AvailabilityZone availabilityZone) {

    // Get cloud-init script to launch cluster master
    final String userData = CloudInitScriptBuilder.buildClusterMasterCommand(this.clusterName,
        this.nfsParentDir,
        this.sinkLogRootDir,
        this.awsConfDir,
        this.appWorkDir,
        this.masterS3ConfUri,
        this.masterS3ConfFiles,
        this.masterS3JarsUri,
        this.masterS3JarsFiles,
        this.masterJarsDir,
        this.masterJvmMemory,
        this.masterJvmArgs);

    // Create launch config for Cluster master
    this.masterLaunchConfigName = MASTER_LAUNCH_CONFIG_NAME_PREFIX + uuid;
    this.awsSdkClient.createLaunchConfig(this.masterLaunchConfigName,
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
    this.masterAutoScalingGroupName = MASTER_ASG_NAME_PREFIX + uuid;
    final int minNumMasters = 1;
    final int maxNumMasters = 1;
    final int desiredNumMasters = 1;
    final Tag clusterNameTag = new Tag().withKey(CLUSTER_NAME_ASG_TAG).withValue(this.clusterName);
    final Tag clusterUuidTag = new Tag().withKey(CLUSTER_ID_ASG_TAG).withValue(uuid);
    final Tag asgTypeTag = new Tag().withKey(ASG_TYPE_ASG_TAG).withValue(ASG_TYPE_MASTER);
    this.awsSdkClient.createAutoScalingGroup(this.masterAutoScalingGroupName,
        this.masterLaunchConfigName,
        minNumMasters,
        maxNumMasters,
        desiredNumMasters,
        Optional.of(availabilityZone.getZoneName()),
        Optional.<Integer>absent(),
        Optional.<Integer>absent(),
        Optional.<String>absent(),
        Optional.<String>absent(),
        Optional.<String>absent(), Lists.newArrayList(clusterNameTag, clusterUuidTag, asgTypeTag));

    LOGGER.info("Waiting for cluster master to launch");
    this.masterPublicIp = getMasterPublicIp();
    LOGGER.info("Master public ip: "+ this.masterPublicIp);

    return uuid;
  }

  private String getMasterPublicIp() {
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
      instanceIds = this.awsSdkClient.getInstancesForGroup(this.masterAutoScalingGroupName, "running");
      isMasterLaunched = instanceIds.size() > 0;
    }

    if (!isMasterLaunched) {
      throw new RuntimeException("Timed out while waiting for cluster master. "
          + "Check for issue manually for ASG: " + this.masterAutoScalingGroupName);
    }

    // This will change if cluster master restarts, but that will be handled by Helix events
    // TODO: Add listener to Helix / Zookeeper for master restart and update master public ip
    // .. although we do not use master public ip for anything
    return instanceIds.get(0).getPublicIpAddress();
  }

  private void launchWorkUnitRunners(String uuid, String keyName,
      String securityGroups,
      AvailabilityZone availabilityZone) {

    // Get cloud-init script to launch cluster worker
    final String userData = CloudInitScriptBuilder.buildClusterWorkerCommand(this.clusterName,
        this.nfsParentDir,
        this.sinkLogRootDir,
        this.awsConfDir,
        this.appWorkDir,
        this.masterPublicIp,
        this.workerS3ConfUri,
        this.workerS3ConfFiles,
        this.workerS3JarsUri,
        this.workerS3JarsFiles,
        this.workerJarsDir,
        this.workerJvmMemory,
        this.workerJvmArgs);

    // Create launch config for Cluster worker
    this.workerLaunchConfigName = WORKERS_LAUNCH_CONFIG_PREFIX + uuid;
    this.awsSdkClient.createLaunchConfig(this.workerLaunchConfigName,
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
    this.workerAutoScalingGroupName = WORKERS_ASG_NAME_PREFIX + uuid;
    final Tag clusterNameTag = new Tag().withKey(CLUSTER_NAME_ASG_TAG).withValue(this.clusterName);
    final Tag clusterUuidTag = new Tag().withKey(CLUSTER_ID_ASG_TAG).withValue(uuid);
    final Tag asgTypeTag = new Tag().withKey(ASG_TYPE_ASG_TAG).withValue(ASG_TYPE_WORKERS);
    this.awsSdkClient.createAutoScalingGroup(this.workerAutoScalingGroupName,
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
        Lists.newArrayList(clusterNameTag, clusterUuidTag, asgTypeTag));
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
    // Upon receiving shutdown response from workers, master will shut itself down and call back shutdownASG()
    final int messagesSent = this.helixManager.getMessagingService().send(criteria, shutdownRequest,
        shutdownASG(),timeout);
    if (messagesSent == 0) {
      LOGGER.error(String.format("Failed to send the %s message to the controller", shutdownRequest.getMsgSubType()));
    }
  }

  /***
   * Callback method that deletes {@link AutoScalingGroup}s
   * @return Callback method that deletes {@link AutoScalingGroup}s
   */
  private AsyncCallback shutdownASG() {
    Optional<List<String>> optionalLaunchConfigurationNames = Optional
        .of(Arrays.asList(this.masterLaunchConfigName, this.workerLaunchConfigName));
    Optional<List<String>> optionalAutoScalingGroupNames = Optional
        .of(Arrays.asList(this.masterAutoScalingGroupName, this.workerAutoScalingGroupName));

    return new AWSShutdownHandler(this.awsSdkClient,
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
