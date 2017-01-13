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

package gobblin.aws;

import java.io.File;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;

import gobblin.annotation.Alpha;
import gobblin.cluster.GobblinClusterConfigurationKeys;
import static gobblin.aws.GobblinAWSUtils.encodeBase64;


/**
 * Class to generate script for launching Gobblin cluster master and workers via cloud-init
 * on EC2 instance boot up.
 *
 * @author Abhishek Tiwari
 */
@Alpha
public class CloudInitScriptBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(CloudInitScriptBuilder.class);

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

  /***
   * This method generates the script that would be executed by cloud-init module in EC2 instance
   * upon boot up for {@link GobblinAWSClusterManager}.
   *
   * This will generate cloud init shell script that does the following:
   * 1. Mount NFS Server (TODO: To be replaced with EFS soon)
   * 2. Create all prerequisite directories
   * 3. Download cluster configuration from S3
   * 4. Download Gobblin application jars from S3 (TODO: To be replaced via baked in jars in custom Gobblin AMI)
   * 5. Download Gobblin custom jars from S3
   * 6. Launch {@link GobblinAWSClusterManager} java application
   * 7. TODO: Add cron that watches the {@link GobblinAWSClusterManager} application and restarts it if it dies
   *
   * @param clusterName Name of the cluster
   * @param nfsParentDir Directory within which NFS directory should be created and mounted
   * @param sinkLogRootDir Log sink root directory
   * @param awsConfDir Directory to save downloaded Gobblin cluster configuration files
   * @param appWorkDir Gobblin application work directory
   * @param masterS3ConfUri S3 URI to download cluster configuration files from
   * @param masterS3ConfFiles Comma separated list of configuration files to download from masterS3ConfUri
   * @param masterS3JarsUri S3 URI to download Gobblin jar files from
   * @param masterS3JarsFiles Comma separated list of jar files to download from masterS3JarUri
   * @param masterJarsDir Directory to save downloaded Gobblin jar files
   * @param masterJvmMemory Xmx memory setting for Gobblin master java application
   * @param masterJvmArgs JVM arguments for Gobblin master application
   * @param gobblinVersion Optional Gobblin version
   * @return Cloud-init script to launch {@link GobblinAWSClusterManager}
   */
  public static String buildClusterMasterCommand(String clusterName, String nfsParentDir, String sinkLogRootDir,
      String awsConfDir, String appWorkDir,
      String masterS3ConfUri, String masterS3ConfFiles,
      String masterS3JarsUri, String masterS3JarsFiles, String masterJarsDir,
      String masterJvmMemory, Optional<String> masterJvmArgs, Optional<String> gobblinVersion) {
    final StringBuilder cloudInitCmds = new StringBuilder().append("#!/bin/bash").append("\n");

    final String clusterMasterClassName = GobblinAWSClusterManager.class.getSimpleName();

    // Create NFS server
    // TODO: Replace with EFS (it went into GA on 6/30/2016)
    // Note: Until EFS availability, ClusterMaster is SPOF because we loose NFS when it's relaunched / replaced
    //       .. this can be worked around, but would be an un-necessary work
    final String nfsDir = nfsParentDir + clusterName;

    final String nfsShareDirCmd = String.format("echo '%s %s(%s)' | tee --append %s",
        nfsDir, NFS_SHARE_ALL_IPS, NFS_SHARE_DEFAULT_OPTS, NFS_CONF_FILE);
    cloudInitCmds.append("mkdir -p ").append(nfsDir).append(File.separator).append("1").append("\n");
    cloudInitCmds.append(NFS_SERVER_INSTALL_CMD).append("\n");
    cloudInitCmds.append(nfsShareDirCmd).append("\n");
    cloudInitCmds.append(NFS_SERVER_START_CMD).append("\n");
    cloudInitCmds.append(NFS_EXPORT_FS_CMD).append("\n");

    // Create various directories
    cloudInitCmds.append("mkdir -p ").append(sinkLogRootDir).append("\n");
    cloudInitCmds.append("chown -R ec2-user:ec2-user /home/ec2-user/*").append("\n");

    // Setup short variables to save cloud-init script space
    if (gobblinVersion.isPresent()) {
      cloudInitCmds.append("vr=").append(gobblinVersion.get()).append("\n");
    }
    cloudInitCmds.append("cgS3=").append(masterS3ConfUri).append("\n");
    cloudInitCmds.append("cg=").append(awsConfDir).append("\n");
    cloudInitCmds.append("jrS3=").append(masterS3JarsUri).append("\n");
    cloudInitCmds.append("jr=").append(masterJarsDir).append("\n");

    // Download configurations from S3
    final StringBuilder classpath = new StringBuilder();
    final List<String> awsConfs = SPLITTER.splitToList(masterS3ConfFiles);
    for (String awsConf : awsConfs) {
      cloudInitCmds.append(String.format("wget -P \"${cg}\" \"${cgS3}\"%s", awsConf)).append("\n");
    }
    classpath.append(awsConfDir);

    // Download jars from S3
    // TODO: Eventually limit only custom user jars to pulled from S3, load rest from AMI
    final List<String> awsJars = SPLITTER.splitToList(masterS3JarsFiles);
    for (String awsJar : awsJars) {
      cloudInitCmds.append(String.format("wget -P \"${jr}\" \"${jrS3}\"%s", awsJar)).append("\n");
    }
    classpath.append(":").append(masterJarsDir).append("*");

    // TODO: Add cron that brings back master if it dies
    // Launch Gobblin Cluster Master
    final StringBuilder launchGobblinClusterMasterCmd = new StringBuilder()
        .append("java")
        .append(" -cp ").append(classpath)
        .append(" -Xmx").append(masterJvmMemory)
        .append(" ").append(masterJvmArgs.or(""))
        .append(" ").append(GobblinAWSClusterManager.class.getName())
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
        .append(" ").append(clusterName)
        .append(" --").append(GobblinAWSConfigurationKeys.APP_WORK_DIR)
        .append(" ").append(appWorkDir)
        .append(" 1>").append(sinkLogRootDir)
        .append(clusterMasterClassName).append(".")
        .append("master").append(".")
        .append(CloudInitScriptBuilder.STDOUT)
        .append(" 2>").append(sinkLogRootDir)
        .append(clusterMasterClassName).append(".")
        .append("master").append(".")
        .append(CloudInitScriptBuilder.STDERR);
    cloudInitCmds.append(launchGobblinClusterMasterCmd).append("\n");

    final String cloudInitScript = cloudInitCmds.toString();
    LOGGER.info("Cloud-init script for master node: " + cloudInitScript);

    return encodeBase64(cloudInitScript);
  }

  /***
   * This method generates the script that would be executed by cloud-init module in EC2 instance
   * upon boot up for {@link GobblinAWSTaskRunner}.
   *
   * This will generate cloud init shell script that does the following:
   * 1. Mount NFS volume (TODO: To be replaced with EFS soon)
   * 2. Create all prerequisite directories
   * 3. Download cluster configuration from S3
   * 4. Download Gobblin application jars from S3 (TODO: To be replaced via baked in jars in custom Gobblin AMI)
   * 5. Download Gobblin custom jars from S3
   * 6. Launch {@link GobblinAWSTaskRunner} java application
   * 7. TODO: Add cron that watches the {@link GobblinAWSTaskRunner} application and restarts it if it dies
   *
   * @param clusterName Name of the cluster
   * @param nfsParentDir Directory within which NFS directory should be created and mounted
   * @param sinkLogRootDir Log sink root directory
   * @param awsConfDir Directory to save downloaded Gobblin cluster configuration files
   * @param appWorkDir Gobblin application work directory
   * @param masterPublicIp IP of Gobblin cluster worker
   * @param workerS3ConfUri S3 URI to download cluster configuration files from
   * @param workerS3ConfFiles Comma separated list of configuration files to download from workerS3ConfUri
   * @param workerS3JarsUri S3 URI to download Gobblin jar files from
   * @param workerS3JarsFiles Comma separated list of jar files to download from workerS3JarUri
   * @param workerJarsDir Directory to save downloaded Gobblin jar files
   * @param workerJvmMemory Xmx memory setting for Gobblin worker java application
   * @param workerJvmArgs JVM arguments for Gobblin worker application
   * @param gobblinVersion Optional Gobblin version
   * @return Cloud-init script to launch {@link GobblinAWSTaskRunner}
   */
  public static String buildClusterWorkerCommand(String clusterName, String nfsParentDir, String sinkLogRootDir,
      String awsConfDir, String appWorkDir, String masterPublicIp,
      String workerS3ConfUri, String workerS3ConfFiles,
      String workerS3JarsUri, String workerS3JarsFiles, String workerJarsDir,
      String workerJvmMemory, Optional<String> workerJvmArgs, Optional<String> gobblinVersion) {
    final StringBuilder cloudInitCmds = new StringBuilder().append("#!/bin/bash").append("\n");

    final String clusterWorkerClassName = GobblinAWSTaskRunner.class.getSimpleName();

    // Connect to NFS server
    // TODO: Replace with EFS (it went into GA on 6/30/2016)
    final String nfsDir = nfsParentDir + clusterName;
    final String nfsMountCmd = String.format("mount -t %s %s:%s %s", NFS_TYPE_4, masterPublicIp, nfsDir,
        nfsDir);
    cloudInitCmds.append("mkdir -p ").append(nfsDir).append("\n");
    cloudInitCmds.append(nfsMountCmd).append("\n");

    // Create various other directories
    cloudInitCmds.append("mkdir -p ").append(sinkLogRootDir).append("\n");
    cloudInitCmds.append("chown -R ec2-user:ec2-user /home/ec2-user/*").append("\n");

    // Setup short variables to save cloud-init script space
    if (gobblinVersion.isPresent()) {
      cloudInitCmds.append("vr=").append(gobblinVersion.get()).append("\n");
    }
    cloudInitCmds.append("cg0=").append(workerS3ConfUri).append("\n");
    cloudInitCmds.append("cg=").append(awsConfDir).append("\n");
    cloudInitCmds.append("jr0=").append(workerS3JarsUri).append("\n");
    cloudInitCmds.append("jr=").append(workerJarsDir).append("\n");

    // Download configurations from S3
    final StringBuilder classpath = new StringBuilder();
    final List<String> awsConfs = SPLITTER.splitToList(workerS3ConfFiles);
    for (String awsConf : awsConfs) {
      cloudInitCmds.append(String.format("wget -P \"${cg}\" \"${cg0}\"%s", awsConf)).append("\n");
    }
    classpath.append(awsConfDir);

    // Download jars from S3
    // TODO: Limit only custom user jars to pulled from S3, load rest from AMI
    final List<String> awsJars = SPLITTER.splitToList(workerS3JarsFiles);
    for (String awsJar : awsJars) {
      cloudInitCmds.append(String.format("wget -P \"${jr}\" \"${jr0}\"%s", awsJar)).append("\n");
    }
    classpath.append(":").append(workerJarsDir).append("*");

    // Get a random Helix instance name
    cloudInitCmds.append("pi=`curl http://169.254.169.254/latest/meta-data/local-ipv4`").append("\n");

    // TODO: Add cron that brings back worker if it dies
    // Launch Gobblin Worker
    final StringBuilder launchGobblinClusterWorkerCmd = new StringBuilder()
        .append("java")
        .append(" -cp ").append(classpath)
        .append(" -Xmx").append(workerJvmMemory)
        .append(" ").append(workerJvmArgs.or(""))
        .append(" ").append(GobblinAWSTaskRunner.class.getName())
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
        .append(" ").append(clusterName)
        .append(" --").append(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME)
        .append(" ").append("$pi")
        .append(" --").append(GobblinAWSConfigurationKeys.APP_WORK_DIR)
        .append(" ").append(appWorkDir)
        .append(" 1>").append(sinkLogRootDir)
        .append(clusterWorkerClassName).append(".")
        .append("$pi").append(".")
        .append(CloudInitScriptBuilder.STDOUT)
        .append(" 2>").append(sinkLogRootDir)
        .append(clusterWorkerClassName).append(".")
        .append("$pi").append(".")
        .append(CloudInitScriptBuilder.STDERR);
    cloudInitCmds.append(launchGobblinClusterWorkerCmd);

    final String cloudInitScript = cloudInitCmds.toString();
    LOGGER.info("Cloud-init script for worker node: " + cloudInitScript);

    return encodeBase64(cloudInitScript);
  }
}
