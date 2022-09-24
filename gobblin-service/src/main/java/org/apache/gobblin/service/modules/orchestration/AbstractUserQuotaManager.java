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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.exception.QuotaExceededException;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;


/**
 * An abstract implementation of {@link UserQuotaManager} that has base implementation of checking quota and increasing/decreasing it.
 */
@Slf4j
abstract public class AbstractUserQuotaManager implements UserQuotaManager {
  public static final String PER_USER_QUOTA = DagManager.DAG_MANAGER_PREFIX + "perUserQuota";
  public static final String PER_FLOWGROUP_QUOTA = DagManager.DAG_MANAGER_PREFIX + "perFlowGroupQuota";
  public static final String USER_JOB_QUOTA_KEY = DagManager.DAG_MANAGER_PREFIX + "defaultJobQuota";
  public static final String QUOTA_SEPERATOR = ":";
  public static final Integer DEFAULT_USER_JOB_QUOTA = Integer.MAX_VALUE;
  private final Map<String, Integer> perUserQuota;
  private final Map<String, Integer> perFlowGroupQuota;

  private final int defaultQuota;

  public AbstractUserQuotaManager(Config config) {
    this.defaultQuota = ConfigUtils.getInt(config, USER_JOB_QUOTA_KEY, DEFAULT_USER_JOB_QUOTA);
    ImmutableMap.Builder<String, Integer> userMapBuilder = ImmutableMap.builder();
    ImmutableMap.Builder<String, Integer> flowGroupMapBuilder = ImmutableMap.builder();
    // Quotas will take form of user:<Quota> and flowGroup:<Quota>
    for (String flowGroupQuota : ConfigUtils.getStringList(config, PER_FLOWGROUP_QUOTA)) {
      flowGroupMapBuilder.put(flowGroupQuota.split(QUOTA_SEPERATOR)[0], Integer.parseInt(flowGroupQuota.split(QUOTA_SEPERATOR)[1]));
    }
    // Keep quotas per user as well in form user:<Quota> which apply for all flowgroups
    for (String userQuota : ConfigUtils.getStringList(config, PER_USER_QUOTA)) {
      userMapBuilder.put(userQuota.split(QUOTA_SEPERATOR)[0], Integer.parseInt(userQuota.split(QUOTA_SEPERATOR)[1]));
    }
    this.perUserQuota = userMapBuilder.build();
    this.perFlowGroupQuota = flowGroupMapBuilder.build();
  }

  abstract void addDagId(String dagId) throws IOException;

  abstract boolean containsDagId(String dagId) throws IOException;

  abstract boolean removeDagId(String dagId) throws IOException;

  // Implementations should return the current count and increase them by one
  abstract int incrementJobCount(String key, CountType countType) throws IOException;

  abstract void decrementJobCount(String key, CountType countType) throws IOException;

  public void checkQuota(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
    QuotaCheck quotaCheck = increaseAndCheckQuota(dagNode);

    // Throw errors for reach quota at the end to avoid inconsistent job counts
    if ((!quotaCheck.proxyUserCheck || !quotaCheck.requesterCheck || !quotaCheck.flowGroupCheck)) {
      // roll back the increased counts in this block
      rollbackIncrements(dagNode);
      throw new QuotaExceededException(quotaCheck.requesterMessage);
    }
  }

  private void rollbackIncrements(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
    String proxyUser = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), AzkabanProjectConfig.USER_TO_PROXY, null);
    String flowGroup = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), ConfigurationKeys.FLOW_GROUP_KEY, "");
    List<String> usersQuotaIncrement = DagManagerUtils.getDistinctUniqueRequesters(DagManagerUtils.getSerializedRequesterList(dagNode));

    decrementJobCount(DagManagerUtils.getUserQuotaKey(proxyUser, dagNode), CountType.USER_COUNT);
    decrementQuotaUsageForUsers(usersQuotaIncrement);
    decrementJobCount(DagManagerUtils.getFlowGroupQuotaKey(flowGroup, dagNode), CountType.FLOWGROUP_COUNT);
    removeDagId(DagManagerUtils.generateDagId(dagNode).toString());
  }

  protected QuotaCheck increaseAndCheckQuota(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
    QuotaCheck quotaCheck = new QuotaCheck(true, true, true, "");
    // Dag is already being tracked, no need to double increment for retries and multihop flows
    if (containsDagId(DagManagerUtils.generateDagId(dagNode).toString())) {
      return quotaCheck;
    } else {
      addDagId(DagManagerUtils.generateDagId(dagNode).toString());
    }

    String proxyUser = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), AzkabanProjectConfig.USER_TO_PROXY, null);
    String flowGroup = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(),
        ConfigurationKeys.FLOW_GROUP_KEY, "");
    String specExecutorUri = DagManagerUtils.getSpecExecutorUri(dagNode);
    StringBuilder requesterMessage = new StringBuilder();

    boolean proxyUserCheck;

    if (proxyUser != null && dagNode.getValue().getCurrentAttempts() <= 1) {
      int proxyQuotaIncrement = incrementJobCountAndCheckQuota(
          DagManagerUtils.getUserQuotaKey(proxyUser, dagNode), getQuotaForUser(proxyUser), CountType.USER_COUNT);
      proxyUserCheck = proxyQuotaIncrement >= 0;  // proxy user quota check succeeds
      quotaCheck.setProxyUserCheck(proxyUserCheck);
      if (!proxyUserCheck) {
        // add 1 to proxyUserIncrement since proxyQuotaIncrement is the count before the increment
        requesterMessage.append(String.format(
            "Quota exceeded for proxy user %s on executor %s : quota=%s, requests above quota=%d%n",
            proxyUser, specExecutorUri, getQuotaForUser(proxyUser), Math.abs(proxyQuotaIncrement) + 1 - getQuotaForUser(proxyUser)));
      }
    }

    String serializedRequesters = DagManagerUtils.getSerializedRequesterList(dagNode);
    boolean requesterCheck = true;

    if (dagNode.getValue().getCurrentAttempts() <= 1) {
      List<String> uniqueRequesters = DagManagerUtils.getDistinctUniqueRequesters(serializedRequesters);
      for (String requester : uniqueRequesters) {
        int userQuotaIncrement = incrementJobCountAndCheckQuota(
            DagManagerUtils.getUserQuotaKey(requester, dagNode), getQuotaForUser(requester), CountType.REQUESTER_COUNT);
        boolean thisRequesterCheck = userQuotaIncrement >= 0;  // user quota check succeeds
        requesterCheck = requesterCheck && thisRequesterCheck;
        quotaCheck.setRequesterCheck(requesterCheck);
        if (!thisRequesterCheck) {
          requesterMessage.append(String.format(
              "Quota exceeded for requester %s on executor %s : quota=%s, requests above quota=%d%n. ",
              requester, specExecutorUri, getQuotaForUser(requester), Math.abs(userQuotaIncrement) + 1 - getQuotaForUser(requester)));
        }
      }
    }

    boolean flowGroupCheck;

    if (dagNode.getValue().getCurrentAttempts() <= 1) {
      int flowGroupQuotaIncrement = incrementJobCountAndCheckQuota(
          DagManagerUtils.getFlowGroupQuotaKey(flowGroup, dagNode), getQuotaForFlowGroup(flowGroup), CountType.FLOWGROUP_COUNT);
      flowGroupCheck = flowGroupQuotaIncrement >= 0;
      quotaCheck.setFlowGroupCheck(flowGroupCheck);
      if (!flowGroupCheck) {
        requesterMessage.append(String.format("Quota exceeded for flowgroup %s on executor %s : quota=%s, requests above quota=%d%n",
            flowGroup, specExecutorUri, getQuotaForFlowGroup(flowGroup),
            Math.abs(flowGroupQuotaIncrement) + 1 - getQuotaForFlowGroup(flowGroup)));
      }
    }

    quotaCheck.setRequesterMessage(requesterMessage.toString());

    return quotaCheck;
  }

  /**
   * Decrement the quota by one for the proxy user and requesters corresponding to the provided {@link Dag.DagNode}.
   * Returns true if the dag existed in the set of running dags and was removed successfully
   */
  public boolean releaseQuota(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
    boolean val = removeDagId(DagManagerUtils.generateDagId(dagNode).toString());
    if (!val) {
      return false;
    }

    String proxyUser = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), AzkabanProjectConfig.USER_TO_PROXY, null);
    if (proxyUser != null) {
      String proxyUserKey = DagManagerUtils.getUserQuotaKey(proxyUser, dagNode);
      decrementJobCount(proxyUserKey, CountType.USER_COUNT);
    }

    String flowGroup = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(),
        ConfigurationKeys.FLOW_GROUP_KEY, "");
    decrementJobCount(DagManagerUtils.getFlowGroupQuotaKey(flowGroup, dagNode), CountType.FLOWGROUP_COUNT);

    String serializedRequesters = DagManagerUtils.getSerializedRequesterList(dagNode);
    try {
      for (String requester : DagManagerUtils.getDistinctUniqueRequesters(serializedRequesters)) {
        String requesterKey = DagManagerUtils.getUserQuotaKey(requester, dagNode);
        decrementJobCount(requesterKey, CountType.REQUESTER_COUNT);
      }
    } catch (IOException e) {
      log.error("Failed to release quota for requester list " + serializedRequesters, e);
      return false;
    }

    return true;
  }

  private int incrementJobCountAndCheckQuota(String key, int keyQuota, CountType countType) throws IOException {
    int currentCount = incrementJobCount(key, countType);
    if (currentCount >= keyQuota) {
      return -currentCount;
    } else {
      return currentCount;
    }
  }

  private void decrementQuotaUsageForUsers(List<String> requestersToDecreaseCount) throws IOException {
    for (String requester : requestersToDecreaseCount) {
      decrementJobCount(requester, CountType.REQUESTER_COUNT);
    }
  }

  private int getQuotaForUser(String user) {
    return this.perUserQuota.getOrDefault(user, defaultQuota);
  }

  private int getQuotaForFlowGroup(String flowGroup) {
    return this.perFlowGroupQuota.getOrDefault(flowGroup, defaultQuota);
  }

  @Setter
  @AllArgsConstructor
  protected static class QuotaCheck {
    boolean proxyUserCheck;
    boolean requesterCheck;
    boolean flowGroupCheck;
    String requesterMessage;
  }

  protected enum CountType {
    USER_COUNT,
    REQUESTER_COUNT,
    FLOWGROUP_COUNT
  }
}