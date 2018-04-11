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
package org.apache.gobblin.cluster;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;

/**
 * #HELIX-0.6.7-WORKAROUND
 * Replacement TaskDriver methods to workaround bugs and changes in behavior for the 0.6.7 upgrade
 */
public class GobblinHelixTaskDriver {
  private final TaskDriver _taskDriver;

  public GobblinHelixTaskDriver(HelixManager manager) {
    this(manager.getClusterManagmentTool(), manager.getHelixDataAccessor(), manager
        .getConfigAccessor(), manager.getHelixPropertyStore(), manager.getClusterName());
  }

  public GobblinHelixTaskDriver(HelixAdmin admin, HelixDataAccessor accessor, ConfigAccessor cfgAccessor,
      HelixPropertyStore<ZNRecord> propertyStore, String clusterName) {
    _taskDriver = new TaskDriver(admin, accessor, cfgAccessor, propertyStore, clusterName);
  }

  /**
   * Delete the workflow
   *
   * @param workflow  The workflow name
   * @param timeout   The timeout for deleting the workflow/queue in seconds
   */
  public void deleteWorkflow(String workflow, long timeout) throws InterruptedException {
    WorkflowConfig workflowConfig = _taskDriver.getWorkflowConfig(workflow);

    // set the target state if not already set
    if (workflowConfig != null && workflowConfig.getTargetState() != TargetState.DELETE) {
      _taskDriver.delete(workflow);
    }

    long endTime = System.currentTimeMillis() + (timeout * 1000);

    // check for completion of deletion request
    while (System.currentTimeMillis() <= endTime) {
      WorkflowContext workflowContext = _taskDriver.getWorkflowContext(workflow);

      if (workflowContext != null) {
        Thread.sleep(1000);
      } else {
        // Successfully deleted
        return;
      }
    }

    // Failed to complete deletion within timeout
    throw new HelixException(String
        .format("Fail to delete the workflow/queue %s within %d seconds.", workflow, timeout));
  }
}
