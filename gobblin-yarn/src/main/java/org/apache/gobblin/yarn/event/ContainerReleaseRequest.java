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

package org.apache.gobblin.yarn.event;

import java.util.Collection;

import org.apache.hadoop.yarn.api.records.Container;


/**
 * A type of event for container release requests to be used with a {@link com.google.common.eventbus.EventBus}.
 * This event is different than {@link ContainerShutdownRequest} because it releases the container through
 * the Resource Manager, while {@link ContainerShutdownRequest} shuts down a container through the
 * Node Manager
 */
public class ContainerReleaseRequest {
  private final Collection<Container> containers;

  public ContainerReleaseRequest(Collection<Container> containers) {
    this.containers = containers;
  }

  /**
   * Get the IDs of the containers to release.
   *
   * @return the IDs of the containers to release
   */
  public Collection<Container> getContainers() {
    return this.containers;
  }
}
