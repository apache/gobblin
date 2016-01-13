/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModel;

import gobblin.runtime.TaskExecutor;


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
public class GobblinTaskStateModel extends TaskStateModel {

  private final TaskExecutor taskExecutor;

  public GobblinTaskStateModel(HelixManager helixManager, Map<String, TaskFactory> taskFactoryRegistry,
      TaskExecutor taskExecutor) {
    super(helixManager, taskFactoryRegistry);
    this.taskExecutor = taskExecutor;
  }
}
