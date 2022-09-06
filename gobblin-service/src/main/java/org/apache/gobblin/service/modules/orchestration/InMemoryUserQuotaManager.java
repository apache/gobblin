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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

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

  @Inject
  public InMemoryUserQuotaManager(Config config) {
    super(config);
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

  @Override
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

  @Override
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