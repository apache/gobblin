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

import java.util.Optional;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.Criteria;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.model.Message;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.typesafe.config.Config;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinHelixMessagingService;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.yarn.helix.HelixMessageSubTypes;

/**
 * <p>
 *   Inherits the {@link AbstractAppSecurityManager} and implements Helix
 *   specific mechanisms for refreshing security credentials.
 *
 *   NOTE: Although this class contains the term "Yarn", the class does not specifically
 *   rely on Yarn. It is a generic class that is used by Gobblin Yarn applications which
 *   are typically managed by Helix
 * </p>
 */
public abstract class AbstractYarnAppSecurityManager extends AbstractAppSecurityManager {

  protected final Optional<HelixManager> helixManager;

  private final boolean isHelixClusterManaged;

  public AbstractYarnAppSecurityManager(Config config, FileSystem fs, Path tokenFilePath) {
    this(config, null, fs, tokenFilePath);
  }

  public AbstractYarnAppSecurityManager(Config config, HelixManager helixManager, FileSystem fs, Path tokenFilePath) {
    super(config, fs, tokenFilePath);
    this.helixManager = Optional.ofNullable(helixManager);
    this.isHelixClusterManaged = ConfigUtils.getBoolean(config, GobblinClusterConfigurationKeys.IS_HELIX_CLUSTER_MANAGED,
        GobblinClusterConfigurationKeys.DEFAULT_IS_HELIX_CLUSTER_MANAGED);
  }

  protected void sendTokenFileUpdatedMessage() {
    // Send a message to the controller (when the cluster is not managed)
    // and all the participants if this is not the first login
    if (!this.isHelixClusterManaged) {
      sendTokenFileUpdatedMessage(InstanceType.CONTROLLER);
    }
    sendTokenFileUpdatedMessage(InstanceType.PARTICIPANT);
  }

  /**
   * This method is used to send TokenFileUpdatedMessage which will handle by {@link YarnContainerSecurityManager}
   */
  @VisibleForTesting
  protected void sendTokenFileUpdatedMessage(InstanceType instanceType) {
    sendTokenFileUpdatedMessage(instanceType, "");
  }

  @VisibleForTesting
  protected void sendTokenFileUpdatedMessage(InstanceType instanceType, String instanceName) {
    if (!this.helixManager.isPresent()) {
      return;
    }

    HelixManager helixManager = this.helixManager.get();
    Criteria criteria = new Criteria();
    criteria.setInstanceName(Strings.isNullOrEmpty(instanceName) ? "%" : instanceName);
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
    LOGGER.info(String.format("Sent %d token file updated message(s) to the %s", messagesSent, instanceType));
  }
}
