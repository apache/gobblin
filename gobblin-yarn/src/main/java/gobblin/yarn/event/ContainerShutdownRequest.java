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

package gobblin.yarn.event;

import java.util.Collection;

import org.apache.hadoop.yarn.api.records.Container;


/**
 * A type of events for container shutdown requests to be used with a {@link com.google.common.eventbus.EventBus}.
 *
 * @author Yinan Li
 */
public class ContainerShutdownRequest {

  private final Collection<Container> containers;

  public ContainerShutdownRequest(Collection<Container> containers) {
    this.containers = containers;
  }

  /**
   * Get the ID of the shutdown container.
   *
   * @return the ID of the shutdown container
   */
  public Collection<Container> getContainers() {
    return this.containers;
  }
}
