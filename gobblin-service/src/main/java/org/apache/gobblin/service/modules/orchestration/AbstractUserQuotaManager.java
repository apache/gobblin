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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;

import lombok.AllArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
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
  protected MetricContext metricContext;

  private final int defaultQuota;

  public AbstractUserQuotaManager(Config config) {
    this.metricContext = Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(ConfigUtils.configToProperties(config)),
        this.getClass());
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

  abstract boolean containsDagId(String dagId) throws IOException;

  int getQuotaForUser(String user) {
    return this.perUserQuota.getOrDefault(user, defaultQuota);
  }

  int getQuotaForFlowGroup(String flowGroup) {
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