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

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.service.RequesterService;
import org.apache.gobblin.service.ServiceRequester;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Manages the statically configured user quotas for both the proxy user in user.to.proxy configuration and the API requester(s)
 * Is used by the dag manager to ensure that the number of currently running jobs do not exceed the quota, if the quota
 * is exceeded, then the execution will fail without running on the underlying executor
 */
@Slf4j
public class UserQuotaManager {
  public static final String PER_USER_QUOTA = DagManager.DAG_MANAGER_PREFIX + "perUserQuota";
  public static final String USER_JOB_QUOTA_KEY = DagManager.DAG_MANAGER_PREFIX + "defaultJobQuota";
  public static final String QUOTA_SEPERATOR = ":";
  public static final Integer DEFAULT_USER_JOB_QUOTA = Integer.MAX_VALUE;
  private final Map<String, Integer> proxyUserToJobCount = new ConcurrentHashMap<>();
  private final Map<String, Integer> requesterToJobCount = new ConcurrentHashMap<>();
  private final Map<String, Integer> perUserQuota;
  Map<String, Boolean> runningDagIds = new ConcurrentHashMap<>();
  private final int defaultQuota;

  UserQuotaManager(Config config) {
    this.defaultQuota = ConfigUtils.getInt(config, USER_JOB_QUOTA_KEY, DEFAULT_USER_JOB_QUOTA);
    ImmutableMap.Builder<String, Integer> mapBuilder = ImmutableMap.builder();

    for (String userQuota : ConfigUtils.getStringList(config, PER_USER_QUOTA)) {
      mapBuilder.put(userQuota.split(QUOTA_SEPERATOR)[0], Integer.parseInt(userQuota.split(QUOTA_SEPERATOR)[1]));
    }
    this.perUserQuota = mapBuilder.build();
  }

  /**
   * Checks if the dagNode exceeds the statically configured user quota for both the proxy user and requester user
   * @throws IOException if the quota is exceeded, and logs a statement
   */
  public void checkQuota(Dag.DagNode<JobExecutionPlan> dagNode, boolean onInit) throws IOException {
    String proxyUser = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), AzkabanProjectConfig.USER_TO_PROXY, null);
    String specExecutorUri = DagManagerUtils.getSpecExecutorUri(dagNode);
    boolean proxyUserCheck = true;
    int proxyQuotaIncrement;
    Set<String> usersQuotaIncrement = new HashSet<>(); // holds the users for which quota is increased
    StringBuilder requesterMessage = new StringBuilder();
    runningDagIds.put(DagManagerUtils.generateDagId(dagNode), true);

    if (proxyUser != null) {
      proxyQuotaIncrement = incrementJobCountAndCheckUserQuota(proxyUserToJobCount, proxyUser, dagNode);
      proxyUserCheck = proxyQuotaIncrement >= 0;  // proxy user quota check succeeds
      if (!proxyUserCheck) {
        // add 1 to proxyUserIncrement since count starts at 0, and is negative if quota is exceeded
        requesterMessage.append(String.format(
            "Quota exceeded for proxy user %s on executor %s : quota=%s, requests above quota=%d%n",
            proxyUser, specExecutorUri, getQuotaForUser(proxyUser), Math.abs(proxyQuotaIncrement)+1-getQuotaForUser(proxyUser)));
      }
    }

    String serializedRequesters = DagManagerUtils.getSerializedRequesterList(dagNode);
    boolean requesterCheck = true;

    if (serializedRequesters != null) {
      List<String> uniqueRequesters = RequesterService.deserialize(serializedRequesters).stream()
          .map(ServiceRequester::getName).distinct().collect(Collectors.toList());
      for (String requester : uniqueRequesters) {
        int userQuotaIncrement = incrementJobCountAndCheckUserQuota(requesterToJobCount, requester, dagNode);
        boolean thisRequesterCheck = userQuotaIncrement >= 0;  // user quota check succeeds
        usersQuotaIncrement.add(requester);
        requesterCheck = requesterCheck && thisRequesterCheck;
        if (!thisRequesterCheck) {
          requesterMessage.append(String.format(
              "Quota exceeded for requester %s on executor %s : quota=%s, requests above quota=%d%n",
              requester, specExecutorUri, getQuotaForUser(requester), Math.abs(userQuotaIncrement)-getQuotaForUser(requester)));
        }
      }
    }

    // Throw errors for reach quota at the end to avoid inconsistent job counts
    if ((!proxyUserCheck || !requesterCheck) && !onInit) {
      // roll back the increased counts in this block
      String userKey = DagManagerUtils.getUserQuotaKey(proxyUser, dagNode);
      decrementQuotaUsage(proxyUserToJobCount, userKey);
      decrementQuotaUsageForUsers(usersQuotaIncrement);
      runningDagIds.remove(DagManagerUtils.generateDagId(dagNode));
      throw new IOException(requesterMessage.toString());
    }
  }

  /**
   * Increment quota by one for the given map and key.
   * @return a negative number if quota is already reached for this user
   *         a positive number if the quota is not reached for this user
   *         the absolute value of the number is the used quota before this increment request
   *         0 if quota usage is not changed
   */
  private int incrementJobCountAndCheckUserQuota(Map<String, Integer> quotaMap, String user, Dag.DagNode<JobExecutionPlan> dagNode) {
    String key = DagManagerUtils.getUserQuotaKey(user, dagNode);

    // Only increment job count for first attempt, since job is considered running between retries
    if (dagNode.getValue().getCurrentAttempts() != 1) {
      return 0;
    }

    Integer currentCount;
    // Modifications must be thread safe since DAGs on DagManagerThreads may update the quota for the same user
    do {
      currentCount = quotaMap.get(key);
    } while (currentCount == null ? quotaMap.putIfAbsent(key, 1) != null : !quotaMap.replace(key, currentCount, currentCount + 1));

    if (currentCount == null) {
      currentCount = 0;
    }

    if (currentCount >= getQuotaForUser(user)) {
      return -currentCount; // increment must have crossed the quota
    } else {
      return currentCount;
    }
  }

  /**
   * Decrement the quota by one for the proxy user and requesters corresponding to the provided {@link Dag.DagNode}.
   * Returns true if the dag existed in the set of running dags and was removed successfully
   */
  public boolean releaseQuota(Dag.DagNode<JobExecutionPlan> dagNode) {
    Boolean val = runningDagIds.remove(DagManagerUtils.generateDagId(dagNode));
    if (val == null) {
      return false;
    }
    String proxyUser = ConfigUtils.getString(dagNode.getValue().getJobSpec().getConfig(), AzkabanProjectConfig.USER_TO_PROXY, null);
    if (proxyUser != null) {
      String proxyUserKey = DagManagerUtils.getUserQuotaKey(proxyUser, dagNode);
      decrementQuotaUsage(proxyUserToJobCount, proxyUserKey);
    }
    String serializedRequesters = DagManagerUtils.getSerializedRequesterList(dagNode);
    if (serializedRequesters != null) {
      try {
        for (ServiceRequester requester : RequesterService.deserialize(serializedRequesters)) {
          String requesterKey = DagManagerUtils.getUserQuotaKey(requester.getName(), dagNode);
          decrementQuotaUsage(requesterToJobCount, requesterKey);
        }
      } catch (IOException e) {
        log.error("Failed to release quota for requester list " + serializedRequesters, e);
        return false;
      }
    }
    return true;
  }

  private void decrementQuotaUsage(Map<String, Integer> quotaMap, String user) {
    Integer currentCount;
    if (user == null) {
      return;
    }
    do {
      currentCount = quotaMap.get(user);
    } while (currentCount != null && currentCount > 0 && !quotaMap.replace(user, currentCount, currentCount - 1));
  }

  private void decrementQuotaUsageForUsers(Set<String> requestersToDecreaseCount) {
    for (String requester : requestersToDecreaseCount) {
      decrementQuotaUsage(requesterToJobCount, requester);
    }
  }

  private int getQuotaForUser(String user) {
    return this.perUserQuota.getOrDefault(user, defaultQuota);
  }

}
