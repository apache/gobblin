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

package org.apache.gobblin.service.monitoring;

import java.util.Iterator;

import com.google.common.collect.Iterators;

import org.apache.gobblin.annotation.Alpha;


/**
 * Retriever for {@link JobStatus}.
 */
@Alpha
public abstract class JobStatusRetriever implements LatestFlowExecutionIdTracker {

  public abstract Iterator<JobStatus> getJobStatusesForFlowExecution(String flowName, String flowGroup,
      long flowExecutionId);

  /**
   * Get the latest {@link JobStatus}es that belongs to the same latest flow execution. Currently, latest flow execution
   * is decided by comparing {@link JobStatus#getFlowExecutionId()}.
   */
  public Iterator<JobStatus> getLatestJobStatusByFlowNameAndGroup(String flowName, String flowGroup) {
    long latestExecutionId = getLatestExecutionIdForFlow(flowName, flowGroup);

    return latestExecutionId == -1l ? Iterators.<JobStatus>emptyIterator()
        : getJobStatusesForFlowExecution(flowName, flowGroup, latestExecutionId);
  }
}
