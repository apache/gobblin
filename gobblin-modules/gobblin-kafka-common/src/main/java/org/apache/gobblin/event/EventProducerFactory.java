package org.apache.gobblin.event;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.broker.EmptyKey;
import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;


/**
 * Basic resource factory to create shared {@link EventProducer} instance
 */
@Slf4j
public abstract class EventProducerFactory<T, S extends ScopeType<S>> implements SharedResourceFactory<EventProducer<T>, EmptyKey, S> {
  private static final String FACTORY_NAME = "eventProducer";

  @Override
  public String getName() {
    return FACTORY_NAME;
  }

  @Override
  public SharedResourceFactoryResponse<EventProducer<T>> createResource(SharedResourcesBroker<S> broker,
      ScopedConfigView<S, EmptyKey> config)
      throws NotConfiguredException {
    return new ResourceInstance<>(new EventProducer<>(config.getConfig()));
  }
}
