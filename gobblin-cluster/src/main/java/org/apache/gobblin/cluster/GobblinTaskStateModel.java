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

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.helix.HelixManager;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModel;

import org.apache.gobblin.annotation.Alpha;


/**
 * A state model for a Gobblin task that implements all supported state transitions.
 *
 * <p>
 *   This class is currently not used but may get used in the future if we decide to plugin our own
 *   custom {@link TaskStateModel}. So currently this is like a place holder class.
 * </p>
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinTaskStateModel extends TaskStateModel {

  private final ScheduledExecutorService taskExecutor;

  public GobblinTaskStateModel(HelixManager helixManager, Map<String, TaskFactory> taskFactoryRegistry,
      ScheduledExecutorService taskExecutor) {
    super(helixManager, taskFactoryRegistry, taskExecutor);
    this.taskExecutor = taskExecutor;
  }
}
