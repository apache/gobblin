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

package gobblin.cluster;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.helix.HelixManager;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModel;
import org.apache.helix.task.TaskStateModelFactory;

import gobblin.annotation.Alpha;
import gobblin.runtime.TaskExecutor;


/**
 * A {@link TaskStateModelFactory} for {@link GobblinTaskStateModel}s.
 *
 * <p>
 *   This class is currently not used but may get used in the future if we decide to plugin our own
 *   custom {@link TaskStateModel}. So currently this is like a place holder class.
 * </p>
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinTaskStateModelFactory extends TaskStateModelFactory {

  private final HelixManager helixManager;
  private final Map<String, TaskFactory> taskFactoryRegistry;
  private final ScheduledExecutorService taskExecutor;

  public GobblinTaskStateModelFactory(HelixManager helixManager, Map<String, TaskFactory> taskFactoryRegistry,
      ScheduledExecutorService taskExecutor) {
    super(helixManager, taskFactoryRegistry);
    this.taskExecutor = taskExecutor;
    this.helixManager = helixManager;
    this.taskFactoryRegistry = taskFactoryRegistry;
  }

  @Override
  public TaskStateModel createNewStateModel(String resourceName, String partitionKey) {
    return new GobblinTaskStateModel(this.helixManager, this.taskFactoryRegistry, this.taskExecutor);
  }
}
