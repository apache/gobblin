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
import gobblin.broker.iface.SharedResourceFactoryResponse;
import gobblin.broker.ResourceCoordinate;
import gobblin.broker.ResourceInstance;
import gobblin.util.ClassAliasResolver;
import gobblin.util.limiter.Limiter;
import gobblin.util.limiter.LimiterFactory;
import gobblin.util.limiter.MultiLimiter;
import gobblin.util.limiter.NoopLimiter;

import lombok.extern.slf4j.Slf4j;


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
@Slf4j
public class SharedLimiterFactory<S extends ScopeType<S>> implements SharedResourceFactory<Limiter, SharedLimiterKey, S> {

  public static final String NAME = "limiter";
  public static final String LIMITER_CLASS_KEY = "class";
  public static final String FAIL_IF_NO_GLOBAL_LIMITER_KEY = "failIfNoGlobalLimiter";
  public static final String FAIL_ON_UNKNOWN_RESOURCE_ID = "faiOnUnknownResourceId";

  private static final ClassAliasResolver<LimiterFactory> RESOLVER = new ClassAliasResolver<>(LimiterFactory.class);

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public SharedResourceFactoryResponse<Limiter>
    createResource(SharedResourcesBroker broker, ScopedConfigView<?, SharedLimiterKey> configView)
      throws NotConfiguredException{

    Config config = configView.getConfig();
    SharedLimiterKey.GlobalLimiterPolicy globalLimiterPolicy = configView.getKey().getGlobalLimiterPolicy();

    if (config.hasPath(FAIL_IF_NO_GLOBAL_LIMITER_KEY) && config.getBoolean(FAIL_IF_NO_GLOBAL_LIMITER_KEY) &&
        globalLimiterPolicy != SharedLimiterKey.GlobalLimiterPolicy.USE_GLOBAL) {
      // if user has specified FAIL_IF_NO_GLOBAL_LIMITER_KEY, promote the policy from USE_GLOBAL_IF_CONFIGURED to USE_GLOBAL
      // e.g. fail if no GLOBAL configuration is present
      SharedLimiterKey modifiedKey = new SharedLimiterKey(configView.getKey().getResourceLimited(),
          SharedLimiterKey.GlobalLimiterPolicy.USE_GLOBAL);
      return new ResourceCoordinate<>(this, modifiedKey, (S) configView.getScope());
    }

    Limiter limiter;

    if (!configView.getScope().isLocal() && !globalLimiterPolicy.equals(SharedLimiterKey.GlobalLimiterPolicy.LOCAL_ONLY)) {
      try {
        Class<?> klazz = Class.forName("gobblin.util.limiter.RestliLimiterFactory");
        return new ResourceCoordinate<>((SharedResourceFactory<Limiter, SharedLimiterKey, S>) klazz.newInstance(),
            configView.getKey(), (S) configView.getScope());
      } catch (ReflectiveOperationException roe) {
        if (globalLimiterPolicy.equals(SharedLimiterKey.GlobalLimiterPolicy.USE_GLOBAL)) {
          throw new RuntimeException("There is no Global limiter factory in the classpath.");
        }
      }
    }

    if (config.hasPath(LIMITER_CLASS_KEY)) {
      try {
        LimiterFactory factory = RESOLVER.resolveClass(config.getString(LIMITER_CLASS_KEY)).newInstance();
        limiter = factory.buildLimiter(config);
      } catch (ReflectiveOperationException roe) {
        throw new RuntimeException(roe);
      }
    } else {
      if (config.hasPath(FAIL_ON_UNKNOWN_RESOURCE_ID) && config.getBoolean(FAIL_ON_UNKNOWN_RESOURCE_ID)) {
        throw new NotConfiguredException();
      }
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
