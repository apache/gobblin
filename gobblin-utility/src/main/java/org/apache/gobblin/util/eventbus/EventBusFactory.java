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

package org.apache.gobblin.util.eventbus;

import java.io.IOException;

import com.google.common.eventbus.EventBus;

import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;


/**
 * A {@link SharedResourceFactory} for creating {@link EventBus} instances.
 * @param <S>
 */
public class EventBusFactory<S extends ScopeType<S>> implements SharedResourceFactory<EventBus, EventBusKey, S> {
  public static final String FACTORY_NAME = "eventbus";

  @Override
  public String getName() {
    return FACTORY_NAME;
  }

  public static <S extends ScopeType<S>> EventBus get(String eventBusName, SharedResourcesBroker<S> broker)
      throws IOException {
    try {
      return broker.getSharedResource(new EventBusFactory<S>(), new EventBusKey(eventBusName));
    } catch (NotConfiguredException e) {
      throw new IOException(e);
    }
  }

  @Override
  public SharedResourceFactoryResponse<EventBus> createResource(SharedResourcesBroker<S> broker,
      ScopedConfigView<S, EventBusKey> config) {
    EventBusKey eventBusKey = config.getKey();
    EventBus eventBus = new EventBus(eventBusKey.getSourceClassName());
    return new ResourceInstance<>(eventBus);
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, EventBusKey> config) {
    return broker.selfScope().getType().rootScope();
  }
}
