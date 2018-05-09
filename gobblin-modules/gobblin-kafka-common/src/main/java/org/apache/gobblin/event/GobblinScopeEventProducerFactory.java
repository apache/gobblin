package org.apache.gobblin.event;

import org.apache.gobblin.broker.EmptyKey;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;


/**
 * An {@link EventProducerFactory} to create a shared {@link EventProducer} instance in {@link GobblinScopeTypes}
 */
public class GobblinScopeEventProducerFactory<T> extends EventProducerFactory<T, GobblinScopeTypes> {
  @Override
  public GobblinScopeTypes getAutoScope(SharedResourcesBroker<GobblinScopeTypes> broker,
      ConfigView<GobblinScopeTypes, EmptyKey> config) {
    // By default, a job level resource
    return GobblinScopeTypes.JOB;
  }
}
