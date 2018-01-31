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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * #HELIX-0.6.7-WORKAROUND
 * Replacement TaskDriver methods to workaround bugs and changes in behavior for the 0.6.7 upgrade
 */
public class GobblinHelixTaskDriver {
  /** For logging */
  private static final Logger LOG = Logger.getLogger(GobblinHelixTaskDriver.class);

  private final HelixDataAccessor _accessor;
  private final ConfigAccessor _cfgAccessor;
  private final HelixPropertyStore<ZNRecord> _propertyStore;
  private final HelixAdmin _admin;
  private final String _clusterName;
  private final TaskDriver _taskDriver;

  public GobblinHelixTaskDriver(HelixManager manager) {
    this(manager.getClusterManagmentTool(), manager.getHelixDataAccessor(), manager
        .getConfigAccessor(), manager.getHelixPropertyStore(), manager.getClusterName());
  }

  public GobblinHelixTaskDriver(ZkClient client, String clusterName) {
    this(client, new ZkBaseDataAccessor<ZNRecord>(client), clusterName);
  }

  public GobblinHelixTaskDriver(ZkClient client, ZkBaseDataAccessor<ZNRecord> baseAccessor, String clusterName) {
    this(new ZKHelixAdmin(client), new ZKHelixDataAccessor(clusterName, baseAccessor),
        new ConfigAccessor(client), new ZkHelixPropertyStore<ZNRecord>(baseAccessor,
            PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName), null), clusterName);
  }

  public GobblinHelixTaskDriver(HelixAdmin admin, HelixDataAccessor accessor, ConfigAccessor cfgAccessor,
      HelixPropertyStore<ZNRecord> propertyStore, String clusterName) {
    _admin = admin;
    _accessor = accessor;
    _cfgAccessor = cfgAccessor;
    _propertyStore = propertyStore;
    _clusterName = clusterName;
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
