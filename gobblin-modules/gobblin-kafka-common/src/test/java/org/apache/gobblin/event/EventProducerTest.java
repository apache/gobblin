package org.apache.gobblin.event;

import org.apache.gobblin.broker.EmptyKey;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;


public class EventProducerTest {
  public void testCreateDefaultEventProducer()
      throws NotConfiguredException {
    @SuppressWarnings("unchecked") SharedResourcesBroker<GobblinScopeTypes> broker =
        (SharedResourcesBroker<GobblinScopeTypes>) SharedResourcesBrokerFactory.getImplicitBroker();

    EventProducer<Object> eventProducer = broker.getSharedResource(new GobblinScopeEventProducerFactory<>(), EmptyKey.INSTANCE);
  }
}
