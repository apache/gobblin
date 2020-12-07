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

package org.apache.gobblin.metrics.event;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import lombok.Getter;

import org.apache.gobblin.metrics.GobblinTrackingEvent;


/**
 * An event reporting missing any kind of entity, e.g. WorkUnit, Record, kafka topic
 */
public class EntityMissingEventBuilder extends GobblinEventBuilder {

  private static final String ENTITY_MISSING_EVENT_TYPE = "EntityMissingEvent";
  private static final String INSTANCE_KEY = "entityInstance";

  /**
   * The missing instance of this entity, e.g record id, topic name
   */
  @Getter
  private final String instance;

  public EntityMissingEventBuilder(String name, String instance) {
    this(name, null, instance);
  }

  public EntityMissingEventBuilder(String name, String namespace, String instance) {
    super(name, namespace);
    this.instance = instance;
    this.metadata.put(EVENT_TYPE, ENTITY_MISSING_EVENT_TYPE);
  }

  @Override
  public GobblinTrackingEvent build() {
    if (instance != null) {
      this.metadata.put(INSTANCE_KEY, instance);
    }
    return super.build();
  }

  public static boolean isEntityMissingEvent(GobblinTrackingEvent event) {
    String eventType = (event.getMetadata() == null) ? "" : event.getMetadata().get(EVENT_TYPE);
    return StringUtils.isNotEmpty(eventType) && eventType.equals(ENTITY_MISSING_EVENT_TYPE);
  }

  public static EntityMissingEventBuilder fromEvent(GobblinTrackingEvent event) {
    if(!isEntityMissingEvent(event)) {
      return null;
    }

    Map<String, String> metadata = event.getMetadata();
    String instance = metadata.get(INSTANCE_KEY);
    EntityMissingEventBuilder eventBuilder = new EntityMissingEventBuilder(
        event.getName(), event.getNamespace(), instance);
    metadata.forEach((key, value) -> {
      switch (key) {
        case INSTANCE_KEY:
          break;
        default:
          eventBuilder.addMetadata(key, value);
          break;
      }
    });

    return eventBuilder;
  }
}
