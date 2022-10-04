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

package org.apache.gobblin.runtime.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import lombok.EqualsAndHashCode;
import lombok.Getter;


public interface DagActionStore {
  enum DagActionValue {
    KILL,
    RESUME
  }

  @Getter
  @EqualsAndHashCode
  class DagAction {
    String flowGroup;
    String flowName;
    String flowExecutionId;
    DagActionValue dagActionValue;
    public DagAction(String flowGroup, String flowName, String flowExecutionId, DagActionValue dagActionValue) {
      this.flowGroup = flowGroup;
      this.flowName = flowName;
      this.flowExecutionId = flowExecutionId;
      this.dagActionValue = dagActionValue;
    }
  }


  /**
   * Check if an action exists in dagAction store by flow group, flow name and flow execution id.
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @throws IOException
   */
  boolean exists(String flowGroup, String flowName, String flowExecutionId) throws IOException, SQLException;

  /**
   * Persist the dag action in {@link DagActionStore} for durability
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @param dagActionValue the value of the dag action
   * @throws IOException
   */
  void addDagAction(String flowGroup, String flowName, String flowExecutionId, DagActionValue dagActionValue) throws IOException;

  /**
   * delete the dag action from {@link DagActionStore}
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @throws IOException
   * @return true if we successfully delete one record, return false if the record does not exist
   */
  boolean deleteDagAction(String flowGroup, String flowName, String flowExecutionId) throws IOException;

  /***
   * Retrieve action value by the flow group, flow name and flow execution id from the {@link DagActionStore}.
   * @param flowGroup flow group for the dag action
   * @param flowName flow name for the dag action
   * @param flowExecutionId flow execution for the dag action
   * @throws IOException Exception in retrieving the {@link DagAction}.
   * @throws SpecNotFoundException If {@link DagAction} being retrieved is not present in store.
   */
  DagAction getDagAction(String flowGroup, String flowName, String flowExecutionId) throws IOException, SpecNotFoundException,
                                                                                           SQLException;

  /***
   * Get all {@link DagAction}s from the {@link DagActionStore}.
   * @throws IOException Exception in retrieving {@link DagAction}s.
   */
  Collection<DagAction> getDagActions() throws IOException;

}
