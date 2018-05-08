package org.apache.gobblin.event;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.broker.EmptyKey;
import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NoSuchScopeException;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;


/**
 * A factory to create shared {@link EventProducer} instance
 */
@Slf4j
public class EventProducerFactory<T, S extends ScopeType<S>> implements SharedResourceFactory<EventProducer<T>, EmptyKey, S> {
  private static final String FACTORY_NAME = "eventProducerFactory";
  private static final String CONF_NAMESPACE = "eventProducer";

  @Override
  public String getName() {
    return FACTORY_NAME;
  }

  @Override
  public SharedResourceFactoryResponse<EventProducer<T>> createResource(SharedResourcesBroker<S> broker,
      ScopedConfigView<S, EmptyKey> config)
      throws NotConfiguredException {
    return new ResourceInstance<>(createEventProducer(config.getConfig()));
  }

  public String getConfNamespace() {
    return CONF_NAMESPACE;
  }

  public EventProducer<T> createEventProducer(Config config) {
    return new EventProducer<>(config);
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, EmptyKey> config) {
    return broker.selfScope().getType();
  }

  private static SharedResourcesBroker<GobblinScopeTypes> getGobblinBroker(State  state) {
    SharedResourcesBroker<GobblinScopeTypes> broker = null;
    if (state instanceof SourceState) {
      broker = ((SourceState) state).getBroker();
    } else if (state instanceof WorkUnitState) {
      broker = ((WorkUnitState) state).getTaskBrokerNullable();
    }
    return broker;
  }

  public static <E> EventProducer<E> getJobLevelEventProducer(State state) {
    SharedResourcesBroker<GobblinScopeTypes> broker = getGobblinBroker(state);

    if (broker == null) {
      log.warn("Could not find a broker in state {}. Use default EventProducer.", state.getClass().getName());
      return new EventProducer<>(ConfigFactory.empty());
    }

    try {
      return broker.getSharedResourceAtScope(new EventProducerFactory<>(), EmptyKey.INSTANCE, GobblinScopeTypes.JOB);
    } catch (NotConfiguredException | NoSuchScopeException e) {
      throw new RuntimeException(e);
    }
  }
}
