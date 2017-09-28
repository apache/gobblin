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

package org.apache.gobblin.publisher;

import java.io.IOException;
import java.util.Collections;

import org.apache.gobblin.broker.ImmediatelyInvalidResourceEntry;
import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.capability.Capability;
import org.apache.gobblin.configuration.State;

import lombok.extern.slf4j.Slf4j;

/**
 * A {@link SharedResourceFactory} for creating {@link DataPublisher}s.
 *
 * The factory creates a {@link DataPublisher} with the publisher class name and state.
 */
@Slf4j
public class DataPublisherFactory<S extends ScopeType<S>>
    implements SharedResourceFactory<DataPublisher, DataPublisherKey, S> {

  public static final String FACTORY_NAME = "dataPublisher";

  public static <S extends ScopeType<S>> DataPublisher get(String publisherClassName, State state,
      SharedResourcesBroker<S> broker) throws IOException {
    try {
      return broker.getSharedResource(new DataPublisherFactory<S>(), new DataPublisherKey(publisherClassName, state));
    } catch (NotConfiguredException nce) {
      throw new IOException(nce);
    }
  }

  @Override
  public String getName() {
    return FACTORY_NAME;
  }

  @Override
  public SharedResourceFactoryResponse<DataPublisher> createResource(SharedResourcesBroker<S> broker,
      ScopedConfigView<S, DataPublisherKey> config) throws NotConfiguredException {
    try {
      DataPublisherKey key = config.getKey();
      String publisherClassName = key.getPublisherClassName();
      State state = key.getState();
      Class<? extends DataPublisher> dataPublisherClass =  (Class<? extends DataPublisher>) Class
          .forName(publisherClassName);

      DataPublisher publisher = DataPublisher.getInstance(dataPublisherClass, state);

      // If the publisher is threadsafe then it is shareable, so return it as a resource instance that may be cached
      // by the broker.
      // Otherwise, it is not shareable, so return it as an immediately invalidated resource that will only be returned
      // once from the broker.
      if (publisher.supportsCapability(Capability.THREADSAFE, Collections.EMPTY_MAP)) {
        return new ResourceInstance<>(publisher);
      } else {
        return new ImmediatelyInvalidResourceEntry<>(publisher);
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, DataPublisherKey> config) {
    return broker.selfScope().getType().rootScope();
  }
}
