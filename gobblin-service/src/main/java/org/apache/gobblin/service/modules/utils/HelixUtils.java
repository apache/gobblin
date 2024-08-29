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

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;

import org.apache.helix.Criteria;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterSetup;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.linkedin.data.DataMap;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


@Alpha
@Slf4j
public class HelixUtils {
  public static final String HELIX_INSTANCE_NAME_SEPARATOR = "@";

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

  private static String getUrlFromHelixInstanceName(String helixInstanceName) {
    if (!helixInstanceName.contains(HELIX_INSTANCE_NAME_SEPARATOR)) {
      return null;
    } else {
      String url = helixInstanceName.substring(helixInstanceName.indexOf(HELIX_INSTANCE_NAME_SEPARATOR) + 1);
      try {
        return URLDecoder.decode(url, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException("Failed to decode URL from helix instance name", e);
      }
    }
  }

  private static String getLeaderUrl(HelixManager helixManager) {
    PropertyKey key = helixManager.getHelixDataAccessor().keyBuilder().controllerLeader();
    LiveInstance leader = helixManager.getHelixDataAccessor().getProperty(key);
    return getUrlFromHelixInstanceName(leader.getInstanceName());
  }

  /**
   * If this host is not the leader, throw a {@link RestLiServiceException}, and include the URL of the leader host in
   * the message and in the errorDetails under the key {@link ServiceConfigKeys#LEADER_URL}.
   */
  public static void throwErrorIfNotLeader(Optional<HelixManager> helixManager)  {
    if (helixManager.isPresent() && !helixManager.get().isLeader()) {
      String leaderUrl = getLeaderUrl(helixManager.get());
      if (leaderUrl == null) {
        throw new RuntimeException("Request sent to slave node but could not get leader node URL");
      }
      RestLiServiceException exception = new RestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Request must be sent to leader node at URL " + leaderUrl);
      exception.setErrorDetails(new DataMap(ImmutableMap.of(ServiceConfigKeys.LEADER_URL, leaderUrl)));
      throw exception;
    }
  }

  /**
   * Build helix instance name by getting {@link org.apache.gobblin.service.ServiceConfigKeys#HELIX_INSTANCE_NAME_KEY}
   * and appending the host, port, and service name with a separator
   */
  public static String buildHelixInstanceName(Config config, String defaultInstanceName) {
    String helixInstanceName = ConfigUtils
        .getString(config, ServiceConfigKeys.HELIX_INSTANCE_NAME_KEY, defaultInstanceName);

    String url = "";
    try {
      url = ConfigUtils.getString(config, ServiceConfigKeys.SERVICE_URL_PREFIX, "https://")
          + InetAddress.getLocalHost().getHostName() + ":" + ConfigUtils.getString(config, ServiceConfigKeys.SERVICE_PORT, "")
          + "/" + ConfigUtils.getString(config, ServiceConfigKeys.SERVICE_NAME, "");
      url = HELIX_INSTANCE_NAME_SEPARATOR + URLEncoder.encode(url, "UTF-8");
    } catch (UnknownHostException | UnsupportedEncodingException e) {
      log.warn("Failed to construct helix instance name", e);
    }

    return helixInstanceName + url;
  }
}
