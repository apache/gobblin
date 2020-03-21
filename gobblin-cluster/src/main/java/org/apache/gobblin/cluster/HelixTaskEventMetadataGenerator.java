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

import com.google.common.collect.ImmutableMap;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.runtime.api.TaskEventMetadataGenerator;

@Alias("helixtask")
public class HelixTaskEventMetadataGenerator implements TaskEventMetadataGenerator  {
  public static final String HELIX_INSTANCE_KEY = "helixInstance";
  public static final String HOST_NAME_KEY = "containerNode";
  public static final String HELIX_JOB_ID_KEY = "helixJobId";
  public static final String HELIX_TASK_ID_KEY = "helixTaskId";
  public static final String CONTAINER_ID_KEY = "containerId";
  /**
   * Generate a map of additional metadata for the specified event name. For tasks running in Gobblin cluster
   * we add container info such as containerId, host name where the task is running to each event.
   *
   * @param taskState
   * @param eventName the event name used to determine which additional metadata should be emitted
   * @return {@link Map} with the additional metadata
   */
  @Override
  public Map<String, String> getMetadata(State taskState, String eventName) {
    String helixInstanceName = taskState.getProp(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY, "");
    String helixJobId = taskState.getProp(GobblinClusterConfigurationKeys.HELIX_JOB_ID_KEY, "");
    String helixTaskId = taskState.getProp(GobblinClusterConfigurationKeys.HELIX_TASK_ID_KEY, "");
    String hostName = taskState.getProp(GobblinClusterConfigurationKeys.TASK_RUNNER_HOST_NAME_KEY, "");
    String containerId = taskState.getProp(GobblinClusterConfigurationKeys.CONTAINER_ID_KEY, "");

    return ImmutableMap.of(HELIX_INSTANCE_KEY, helixInstanceName, HOST_NAME_KEY, hostName, HELIX_JOB_ID_KEY, helixJobId,
        HELIX_TASK_ID_KEY, helixTaskId, CONTAINER_ID_KEY, containerId);
  }
}
