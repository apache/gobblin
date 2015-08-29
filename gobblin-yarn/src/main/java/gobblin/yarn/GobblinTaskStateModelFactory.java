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
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModel;
import org.apache.helix.task.TaskStateModelFactory;

import gobblin.runtime.TaskExecutor;


/**
 * A {@link StateTransitionHandlerFactory} for {@link GobblinTaskStateModel}s.
 *
 * @author ynli
 */
public class GobblinTaskStateModelFactory extends TaskStateModelFactory {

  private final HelixManager helixManager;
  private final Map<String, TaskFactory> taskFactoryRegistry;
  private final TaskExecutor taskExecutor;

  public GobblinTaskStateModelFactory(HelixManager helixManager, Map<String, TaskFactory> taskFactoryRegistry,
      TaskExecutor taskExecutor) {
    super(helixManager, taskFactoryRegistry);
    this.taskExecutor = taskExecutor;
    this.helixManager = helixManager;
    this.taskFactoryRegistry = taskFactoryRegistry;
  }

  @Override
  public TaskStateModel createStateTransitionHandler(ResourceId resourceId, PartitionId partitionId) {
    return new GobblinTaskStateModel(this.helixManager, this.taskFactoryRegistry, this.taskExecutor);
  }
}
