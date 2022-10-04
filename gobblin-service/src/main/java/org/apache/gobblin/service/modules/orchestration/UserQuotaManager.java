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
import java.util.Collection;

import org.apache.gobblin.exception.QuotaExceededException;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

/**
 * Manages the statically configured user quotas for the proxy user in user.to.proxy configuration, the API requester(s)
 * and the flow group.
 * It is used by the {@link DagManager} to ensure that the number of currently running jobs do not exceed the quota, if
 * the quota is exceeded, the execution will fail without running on the underlying executor.
 */
public interface UserQuotaManager {

  /**
   * Initialize with the provided set of dags.
   */
  void init(Collection<Dag<JobExecutionPlan>> dags) throws IOException;

  /**
   * Checks if the dagNode exceeds the statically configured user quota for the proxy user, requester user and flowGroup.
   * It also increases the quota usage for proxy user, requester and the flowGroup of the given DagNode by one.
   * @throws QuotaExceededException if the quota is exceeded
   */
  void checkQuota(Collection<Dag.DagNode<JobExecutionPlan>> dagNode) throws IOException;

  /**
   * Decrement the quota by one for the proxy user and requesters corresponding to the provided {@link Dag.DagNode}.
   * Returns true if successfully reduces the quota usage
   */
  boolean releaseQuota(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException;
}
