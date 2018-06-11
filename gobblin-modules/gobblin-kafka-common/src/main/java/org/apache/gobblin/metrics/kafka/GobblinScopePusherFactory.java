package org.apache.gobblin.metrics.kafka;

import org.apache.gobblin.broker.StringNameSharedResourceKey;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;


/**
 * An {@link PusherFactory} to create a shared {@link Pusher} instance
 * in {@link GobblinScopeTypes}
 */
public class GobblinScopePusherFactory<T> extends PusherFactory<T, GobblinScopeTypes> {
  @Override
  public GobblinScopeTypes getAutoScope(SharedResourcesBroker<GobblinScopeTypes> broker,
      ConfigView<GobblinScopeTypes, StringNameSharedResourceKey> config) {
    // By default, a job level resource
    return GobblinScopeTypes.JOB;
  }
}
