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

package org.apache.gobblin.yarn.helix;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.Criteria;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.Message;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterManager;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.cluster.GobblinHelixConstants;
import org.apache.gobblin.cluster.GobblinHelixMessagingService;
import org.apache.gobblin.cluster.HelixUtils;
import org.apache.gobblin.util.ConfigUtils;


/**
 * The HelixClusterLifecycleManager class is responsible for managing the lifecycle of a Helix cluster in a Gobblin application.
 * It provides methods to create and connect to a Helix cluster, send shutdown requests, and send token file updated messages.
 *
 * The class uses the GobblinClusterConfigurationKeys and GobblinHelixConstants classes for configuration keys and constants.
 * It also relies on the GobblinClusterManager and GobblinHelixMessagingService classes for managing the Gobblin cluster and
 * sending messages to the Helix cluster, respectively.
 *
 * To use this class, create an instance with a Config object containing the necessary configuration properties. The class will
 * automatically connect to the Helix cluster and create the cluster if necessary. To close the connection and send a shutdown
 * request, call the close() method.
 *
 * Note: This class requires a ZooKeeper connection string ({@link GobblinClusterConfigurationKeys#ZK_CONNECTION_STRING_KEY})
 * and a Helix cluster name ({@link GobblinClusterConfigurationKeys#HELIX_CLUSTER_NAME_KEY}) to be provided in the configuration.
 *
 * Example usage:
 * <pre>
 * {@code
 * Config config = ConfigFactory.load();
 * Closer closer = Closer.create();
 * HelixClusterLifecycleManager lifecycleManager = new HelixClusterLifecycleManager(config);
 * closer.register(lifecycleManager);
 * // Use the lifecycleManager instance...
 * closer.close();
 * }
 * </pre>
 */
@Slf4j
public class HelixClusterLifecycleManager implements Closeable {
  private final Config config;
  private final String helixInstanceName;

  @Getter
  private final AtomicBoolean isApplicationRunningFlag;

  /**
   * See {@link GobblinClusterConfigurationKeys#IS_HELIX_CLUSTER_MANAGED}
   */
  private final boolean isHelixClusterManaged;
  @Getter
  private final HelixManager helixManager;
  private final GobblinHelixMessagingService messagingService;

  public HelixClusterLifecycleManager(Config config) throws IOException {
    this.config = config;
    this.isApplicationRunningFlag = new AtomicBoolean(false);

    String zkConnectionString = config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    log.info("Using ZooKeeper connection string: " + zkConnectionString);

    this.isHelixClusterManaged = ConfigUtils.getBoolean(this.config, GobblinClusterConfigurationKeys.IS_HELIX_CLUSTER_MANAGED,
        GobblinClusterConfigurationKeys.DEFAULT_IS_HELIX_CLUSTER_MANAGED);

    this.helixManager = HelixManagerFactory.getZKHelixManager(
        config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY), GobblinClusterUtils.getHostname(),
        InstanceType.SPECTATOR, zkConnectionString);
    this.messagingService = new GobblinHelixMessagingService(this.helixManager);
    this.helixInstanceName = ConfigUtils.getString(config, GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY,
        GobblinClusterManager.class.getSimpleName());

    log.info("Starting Helix cluster");
    connectHelixManager();
    createHelixCluster();
  }

  @Override
  public void close()
      throws IOException {
    if (this.isApplicationRunningFlag.get()) {
      this.sendShutdownRequest();
    }
    this.disconnectHelixManager();
  }

  void createHelixCluster() {
    if (this.isHelixClusterManaged) {
      log.info("Helix cluster is managed; skipping creation of Helix cluster");
    } else {
      String clusterName = this.config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
      boolean overwriteExistingCluster = ConfigUtils.getBoolean(this.config, GobblinClusterConfigurationKeys.HELIX_CLUSTER_OVERWRITE_KEY,
          GobblinClusterConfigurationKeys.DEFAULT_HELIX_CLUSTER_OVERWRITE);
      log.info("Creating Helix cluster {} with overwrite: {}", clusterName, overwriteExistingCluster);
      HelixUtils.createGobblinHelixCluster(this.config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY),
          clusterName, overwriteExistingCluster);
      log.info("Created Helix cluster " + clusterName);
    }
  }

  private void connectHelixManager() {
    try {
      this.helixManager.connect();
    } catch (Exception e) {
      log.error("HelixManager failed to connect", e);
      Throwables.throwIfUnchecked(e);
    }
  }

  private void disconnectHelixManager() {
    if (this.helixManager.isConnected()) {
      this.helixManager.disconnect();
    }
  }

  private void sendShutdownRequest() {
    Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setPartition("%");
    criteria.setPartitionState("%");
    criteria.setResource("%");
    if (this.isHelixClusterManaged) {
      //In the managed mode, the Gobblin Yarn Application Master connects to the Helix cluster in the Participant role.
      criteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
      criteria.setInstanceName(this.helixInstanceName);
    } else {
      criteria.setRecipientInstanceType(InstanceType.CONTROLLER);
    }
    criteria.setSessionSpecific(true);

    Message shutdownRequest = new Message(GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE,
        HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString().toLowerCase() + UUID.randomUUID().toString());
    shutdownRequest.setMsgSubType(HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString());
    shutdownRequest.setMsgState(Message.MessageState.NEW);
    shutdownRequest.setTgtSessionId("*");

    int messagesSent = this.messagingService.send(criteria, shutdownRequest);
    if (messagesSent == 0) {
      log.error(String.format("Failed to send the %s message to the controller", shutdownRequest.getMsgSubType()));
    }
  }


  public void sendTokenFileUpdatedMessage() {
    // Send a message to the controller (when the cluster is not managed)
    // and all the participants if this is not the first login
    if (!this.isHelixClusterManaged) {
      sendTokenFileUpdatedMessage(InstanceType.CONTROLLER);
    }
    sendTokenFileUpdatedMessage(InstanceType.PARTICIPANT);
  }

  public void sendTokenFileUpdatedMessage(InstanceType instanceType) {
    Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setResource("%");
    criteria.setPartition("%");
    criteria.setPartitionState("%");
    criteria.setRecipientInstanceType(instanceType);
    /**
     * #HELIX-0.6.7-WORKAROUND
     * Add back when LIVESTANCES messaging is ported to 0.6 branch
     if (instanceType == InstanceType.PARTICIPANT) {
     criteria.setDataSource(Criteria.DataSource.LIVEINSTANCES);
     }
     **/
    criteria.setSessionSpecific(true);

    Message tokenFileUpdatedMessage = new Message(Message.MessageType.USER_DEFINE_MSG,
        HelixMessageSubTypes.TOKEN_FILE_UPDATED.toString().toLowerCase() + UUID.randomUUID().toString());
    tokenFileUpdatedMessage.setMsgSubType(HelixMessageSubTypes.TOKEN_FILE_UPDATED.toString());
    tokenFileUpdatedMessage.setMsgState(Message.MessageState.NEW);
    if (instanceType == InstanceType.CONTROLLER) {
      tokenFileUpdatedMessage.setTgtSessionId("*");
    }

    // #HELIX-0.6.7-WORKAROUND
    // Temporarily bypass the default messaging service to allow upgrade to 0.6.7 which is missing support
    // for messaging to instances
    //int messagesSent = this.helixManager.getMessagingService().send(criteria, tokenFileUpdatedMessage);
    GobblinHelixMessagingService messagingService = new GobblinHelixMessagingService(helixManager);

    int messagesSent = messagingService.send(criteria, tokenFileUpdatedMessage);
    log.info("Sent {} token file updated message(s) to the {}", messagesSent, instanceType);
  }
}
