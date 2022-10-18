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

import com.google.inject.Inject;
import com.typesafe.config.Config;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.exception.QuotaExceededException;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;

import static org.apache.gobblin.service.ExecutionStatus.RUNNING;


/**
 * An implementation of {@link UserQuotaManager} that stores quota usage in memory.
 */
@Slf4j
@Singleton
public class InMemoryUserQuotaManager extends AbstractUserQuotaManager {
  private final Map<String, Integer> proxyUserToJobCount = new ConcurrentHashMap<>();
  private final Map<String, Integer> flowGroupToJobCount = new ConcurrentHashMap<>();
  private final Map<String, Integer> requesterToJobCount = new ConcurrentHashMap<>();

  private final Set<String> runningDagIds;

  @Inject
  public InMemoryUserQuotaManager(Config config) {
    super(config);
    this.runningDagIds = ConcurrentHashMap.newKeySet();;
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

  protected void rollbackIncrements(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
    String proxyUser = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), AzkabanProjectConfig.USER_TO_PROXY, null);
    String flowGroup = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), ConfigurationKeys.FLOW_GROUP_KEY, "");
    List<String> usersQuotaIncrement = DagManagerUtils.getDistinctUniqueRequesters(DagManagerUtils.getSerializedRequesterList(dagNode));

    decrementJobCount(DagManagerUtils.getUserQuotaKey(proxyUser, dagNode), CountType.USER_COUNT);
    decrementQuotaUsageForUsers(usersQuotaIncrement);
    decrementJobCount(DagManagerUtils.getFlowGroupQuotaKey(flowGroup, dagNode), CountType.FLOWGROUP_COUNT);
    removeDagId(DagManagerUtils.generateDagId(dagNode).toString());
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

  void addDagId(String dagId) {
    this.runningDagIds.add(dagId);
  }

  @Override
  boolean containsDagId(String dagId) {
    return this.runningDagIds.contains(dagId);
  }

  boolean removeDagId(String dagId) {
    return this.runningDagIds.remove(dagId);
  }

  public void init(Collection<Dag<JobExecutionPlan>> dags) throws IOException {
    for (Dag<JobExecutionPlan> dag : dags) {
      for (Dag.DagNode<JobExecutionPlan> dagNode : dag.getNodes()) {
        if (DagManagerUtils.getExecutionStatus(dagNode) == RUNNING) {
          // Add all the currently running Dags to the quota limit per user
          increaseAndCheckQuota(dagNode);
        }
      }
    }
  }

  public void checkQuota(Collection<Dag.DagNode<JobExecutionPlan>> dagNodes) throws IOException {
    for (Dag.DagNode<JobExecutionPlan> dagNode : dagNodes) {
      QuotaCheck quotaCheck = increaseAndCheckQuota(dagNode);
      if ((!quotaCheck.proxyUserCheck || !quotaCheck.requesterCheck || !quotaCheck.flowGroupCheck)) {
        // roll back the increased counts in this block
        rollbackIncrements(dagNode);
        throw new QuotaExceededException(quotaCheck.requesterMessage);
      }
    }
  }

  private int incrementJobCount(String key, Map<String, Integer> quotaMap) {
    Integer currentCount;
    // Modifications must be thread safe since DAGs on DagManagerThreads may update the quota for the same user
    do {
      currentCount = quotaMap.get(key);
    } while (currentCount == null ? quotaMap.putIfAbsent(key, 1) != null : !quotaMap.replace(key, currentCount, currentCount + 1));

    if (currentCount == null) {
      currentCount = 0;
    }

    return currentCount;
  }

  private void decrementJobCount(String key, Map<String, Integer> quotaMap)  {
    Integer currentCount;
    if (key == null) {
      return;
    }
    do {
      currentCount = quotaMap.get(key);
    } while (currentCount != null && currentCount > 0 && !quotaMap.replace(key, currentCount, currentCount - 1));

    if (currentCount == null || currentCount == 0) {
      log.warn("Decrement job count was called for " + key + " when the count was already zero/absent.");
    }
  }

  int incrementJobCount(String user, CountType countType) throws IOException {
    switch (countType) {
      case USER_COUNT:
        return incrementJobCount(user, proxyUserToJobCount);
      case REQUESTER_COUNT:
        return incrementJobCount(user, requesterToJobCount);
      case FLOWGROUP_COUNT:
        return incrementJobCount(user, flowGroupToJobCount);
      default:
        throw new IOException("Invalid count type " + countType);
    }
  }

  void decrementJobCount(String user, CountType countType) throws IOException {
    switch (countType) {
      case USER_COUNT:
        decrementJobCount(user, proxyUserToJobCount);
        break;
      case REQUESTER_COUNT:
        decrementJobCount(user, requesterToJobCount);
        break;
      case FLOWGROUP_COUNT:
        decrementJobCount(user, flowGroupToJobCount);
        break;
      default:
        throw new IOException("Invalid count type " + countType);
    }
  }
}