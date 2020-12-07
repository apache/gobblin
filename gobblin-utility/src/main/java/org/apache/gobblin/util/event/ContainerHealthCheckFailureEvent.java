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

/**
 * An event type to signal failure of a container health check. This event can be generated from anywhere
 * inside the application. This event is intended to be emitted
 * over an {@link com.google.common.eventbus.EventBus} instance.
 */
package org.apache.gobblin.util.event;

import java.util.Map;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import lombok.Getter;

public class ContainerHealthCheckFailureEvent {
  public static final String CONTAINER_HEALTH_CHECK_EVENT_BUS_NAME = "ContainerHealthCheckEventBus";

  // Context of emission of this event, like the task's state.
  @Getter
  private final Config config;

  /**
   * Name of the class that generated this failure event.
   */
  @Getter
  private final String className;

  @Getter
  private final Map<String, String> metadata = Maps.newHashMap();

  public ContainerHealthCheckFailureEvent(Config config, String className) {
    this.config = config;
    this.className = className;
  }

  public void addMetadata(String key, String value) {
    metadata.put(key, value);
  }
}
