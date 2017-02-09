/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.util.limiter.broker;

import java.util.Collection;

import com.typesafe.config.Config;

import gobblin.broker.ResourceInstance;
import gobblin.broker.iface.ConfigView;
import gobblin.broker.iface.NoSuchScopeException;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.ScopeType;
import gobblin.broker.iface.ScopedConfigView;
import gobblin.broker.iface.SharedResourceFactory;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.util.ClassAliasResolver;
import gobblin.util.limiter.Limiter;
import gobblin.util.limiter.LimiterFactory;
import gobblin.util.limiter.MultiLimiter;
import gobblin.util.limiter.NoopLimiter;


/**
 * A {@link SharedResourceFactory} to create shared {@link Limiter}s using a {@link SharedResourcesBroker}.
 *
 * <p>
 *   The factory creates a {@link MultiLimiter} combining a {@link Limiter} at the indicated scope with the
 *   {@link Limiter} at all immediate parent scopes (obtained from the broker itself). The factory reads the property
 *   {@link #LIMITER_CLASS_KEY} from the input {@link ConfigView} to determine the class of {@link Limiter} to create
 *   at the indicated scope. If the key is not found, a {@link NoopLimiter} is generated instead (to combine with
 *   parent {@link Limiter}s).
 * </p>
 */
public class SharedLimiterFactory<S extends ScopeType<S>> implements SharedResourceFactory<Limiter, SharedLimiterKey, S> {

  public static final String NAME = "limiter";
  public static final String LIMITER_CLASS_KEY = "class";

  private static final ClassAliasResolver<LimiterFactory> RESOLVER = new ClassAliasResolver<>(LimiterFactory.class);

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public ResourceInstance<Limiter> createResource(SharedResourcesBroker broker, ScopedConfigView<?, SharedLimiterKey> configView)
      throws NotConfiguredException{
    Config config = configView.getConfig();
    Limiter limiter;

    if (config.hasPath(LIMITER_CLASS_KEY)) {
      try {
        LimiterFactory factory = RESOLVER.resolveClass(config.getString(LIMITER_CLASS_KEY)).newInstance();
        limiter = factory.buildLimiter(config);
      } catch (ReflectiveOperationException roe) {
        throw new RuntimeException(roe);
      }
    } else {
      limiter = new NoopLimiter();
    }

    ScopeType<?> scope = configView.getScope();
    Collection<ScopeType<?>> parentScopes = (Collection<ScopeType<?>>) scope.parentScopes();
    if (parentScopes != null) {
      try {
        for (ScopeType<?> parentScope : parentScopes) {
          limiter = new MultiLimiter(limiter,
              (Limiter) broker.<Limiter, SharedLimiterKey>getSharedResourceAtScope(this, configView.getKey(), parentScope));
        }
      } catch (NoSuchScopeException nsse) {
        throw new RuntimeException("Could not get higher scope limiter. This is an error in code.", nsse);
      }
    }

    return new ResourceInstance<>(limiter);
  }

  /**
   * @return brokers self scope.
   */
  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, SharedLimiterKey> config) {
    return broker.selfScope().getType();
  }
}
