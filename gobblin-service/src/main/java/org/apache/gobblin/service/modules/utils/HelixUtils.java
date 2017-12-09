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

package org.apache.gobblin.service.modules.utils;

import com.google.common.annotations.VisibleForTesting;
import java.util.UUID;
import org.apache.helix.Criteria;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterSetup;

import org.apache.gobblin.annotation.Alpha;
import org.slf4j.Logger;


@Alpha
public class HelixUtils {

  /***
   * Build a Helix Manager (Helix Controller instance).
   *
   * @param helixInstanceName the Helix Instance name.
   * @param helixClusterName the Helix Cluster name.
   * @param zkConnectionString the ZooKeeper connection string.
   * @return HelixManager
   */
  public static HelixManager buildHelixManager(String helixInstanceName, String helixClusterName, String zkConnectionString) {
    return HelixManagerFactory.getZKHelixManager(helixClusterName, helixInstanceName,
        InstanceType.CONTROLLER, zkConnectionString);
  }

  /**
   * Create a Helix cluster for the Gobblin Cluster application.
   *
   * @param zkConnectionString the ZooKeeper connection string
   * @param clusterName the Helix cluster name
   */
  public static void createGobblinHelixCluster(String zkConnectionString, String clusterName) {
    createGobblinHelixCluster(zkConnectionString, clusterName, true);
  }

  /**
   * Create a Helix cluster for the Gobblin Cluster application.
   *
   * @param zkConnectionString the ZooKeeper connection string
   * @param clusterName the Helix cluster name
   * @param overwrite true to overwrite exiting cluster, false to reuse existing cluster
   */
  public static void createGobblinHelixCluster(String zkConnectionString, String clusterName, boolean overwrite) {
    ClusterSetup clusterSetup = new ClusterSetup(zkConnectionString);
    // Create the cluster and overwrite if it already exists
    clusterSetup.addCluster(clusterName, overwrite);
    // Helix 0.6.x requires a configuration property to have the form key=value.
    String autoJoinConfig = ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN + "=true";
    clusterSetup.setConfig(HelixConfigScope.ConfigScopeProperty.CLUSTER, clusterName, autoJoinConfig);
  }

  /**
   * Get a Helix instance name.
   *
   * @param namePrefix a prefix of Helix instance names
   * @param instanceId an integer instance ID
   * @return a Helix instance name that is a concatenation of the given prefix and instance ID
   */
  public static String getHelixInstanceName(String namePrefix, int instanceId) {
    return namePrefix + "_" + instanceId;
  }

  @VisibleForTesting
  public static void sendUserDefinedMessage(String messageSubType, String messageVal, String messageId,
      InstanceType instanceType, HelixManager helixManager, Logger logger) {
    Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setResource("%");
    criteria.setPartition("%");
    criteria.setPartitionState("%");
    criteria.setRecipientInstanceType(instanceType);
    criteria.setSessionSpecific(true);

    Message message = new Message(Message.MessageType.USER_DEFINE_MSG.toString(), messageId);
    message.setMsgSubType(messageSubType);
    message.setAttribute(Message.Attributes.INNER_MESSAGE, messageVal);
    message.setMsgState(Message.MessageState.NEW);
    message.setTgtSessionId("*");

    int messagesSent = helixManager.getMessagingService().send(criteria, message);
    if (messagesSent == 0) {
      logger.error(String.format("Failed to send the %s message to the participants", message));
    }
  }
}
