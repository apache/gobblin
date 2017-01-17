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

package gobblin.yarn;

/**
 * YARN specific event constants to be used with an {@link gobblin.metrics.event.EventSubmitter}.
 */
public class GobblinYarnEventConstants {

  public static final String EVENT_NAMESPACE = "gobblin.yarn";
  public static final String EVENT_CONTEXT_NAME = "GobblinYarn";

  public static class EventMetadata {
    public static final String CONTAINER_STATUS_EXIT_STATUS = "containerStatus.exitStatus";
    public static final String CONTAINER_STATUS_EXIT_DIAGNOSTICS = "containerStatus.diagnostics";
    public static final String CONTAINER_STATUS_RETRY_ATTEMPT = "containerStatus.retryAttempt";
    public static final String CONTAINER_STATUS_CONTAINER_STATE = "containerStatus.state";
    public static final String ERROR_EXCEPTION = "errorException";
    public static final String HELIX_INSTANCE_ID = "helixInstanceId";
  }

  public static class EventNames {
    public static final String CONTAINER_ALLOCATION = "ContainerAllocation";
    public static final String CONTAINER_STARTED = "ContainerStarted";
    public static final String CONTAINER_STATUS_RECEIVED = "ContainerStatusReceived";
    public static final String CONTAINER_STOPPED = "ContainerStopped";
    public static final String CONTAINER_START_ERROR = "ContainerStartError";
    public static final String CONTAINER_GET_STATUS_ERROR = "ContainerGetStatusError";
    public static final String CONTAINER_STOP_ERROR = "ContainerStopError";
    public static final String ERROR = "Error";
    public static final String HELIX_INSTANCE_COMPLETION = "HelixInstanceCompletion";
    public static final String SHUTDOWN_REQUEST = "ShutdownRequest";
  }
}
