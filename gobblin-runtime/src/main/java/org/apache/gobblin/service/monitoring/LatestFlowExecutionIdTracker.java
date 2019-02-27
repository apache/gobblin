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

import java.util.List;


/**
 * Tracks the latest flow execution Id.
 */
public interface LatestFlowExecutionIdTracker {

  /**
   * @param flowName
   * @param flowGroup
   * @return the latest flow execution id with the given flowName and flowGroup. -1 will be returned if no such execution found.
   */
  long getLatestExecutionIdForFlow(String flowName, String flowGroup);

  /**
   * @param flowName
   * @param flowGroup
   * @param count number of execution ids to return
   * @return the latest <code>count</code> execution ids with the given flowName and flowGroup. null will be returned if no such execution found.
   */
  List<Long> getLatestExecutionIdsForFlow(String flowName, String flowGroup, int count);
}
