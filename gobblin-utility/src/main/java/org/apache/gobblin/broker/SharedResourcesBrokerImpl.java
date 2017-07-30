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

package gobblin.broker;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.iface.NoSuchScopeException;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.ScopeInstance;
import gobblin.broker.iface.ScopeType;
import gobblin.broker.iface.SharedResourceFactory;
import gobblin.broker.iface.SharedResourceKey;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.util.ConfigUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Data;


/**
 * An implementation of {@link SharedResourcesBroker} using a {@link DefaultBrokerCache} for storing shared objects.
 *
 * Instances of this broker must be created as follows:
 * <pre>
 *  SharedResourcesBrokerImpl<MyScopes> rootBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(myConfig);
 *  SharedResourcesBrokerImpl<MyScopes> scopeBroker = topBroker.newSubscopedBuilder(scope, "scopeId").build();
 * </pre>
 */
public class SharedResourcesBrokerImpl<S extends ScopeType<S>> implements SharedResourcesBroker<S> {

  private final DefaultBrokerCache<S> brokerCache;
  private final ScopeWrapper<S> selfScopeWrapper;
  private final List<ScopedConfig<S>> scopedConfigs;
  private final ImmutableMap<S, ScopeWrapper<S>> ancestorScopesByType;

  SharedResourcesBrokerImpl(DefaultBrokerCache<S> brokerCache, ScopeWrapper<S> selfScope,
      List<ScopedConfig<S>> scopedConfigs, Map<S, ScopeWrapper<S>> ancestorScopesByType) {
    this.brokerCache = brokerCache;
    this.selfScopeWrapper = selfScope;
    this.scopedConfigs = scopedConfigs;
    this.ancestorScopesByType = ImmutableMap.copyOf(ancestorScopesByType);
  }

  @Override
  public ScopeInstance<S> selfScope() {
    return this.selfScopeWrapper.getScope();
  }

  @Override
  public ScopeInstance<S> getScope(S scopeType) throws NoSuchScopeException {
    return getWrappedScope(scopeType).getScope();
  }

  @Override
  public <T, K extends SharedResourceKey> T getSharedResource(SharedResourceFactory<T, K, S> factory, K key)
      throws NotConfiguredException {
    try {
      return this.brokerCache.getAutoScoped(factory, key, this);
    } catch (ExecutionException ee) {
      Throwable cause = ee.getCause();
      if (cause instanceof NotConfiguredException) {
        throw (NotConfiguredException) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  @Override
  public <T, K extends SharedResourceKey> T getSharedResourceAtScope(SharedResourceFactory<T, K, S> factory, K key,
      S scope) throws NotConfiguredException, NoSuchScopeException {
    try {
      return this.brokerCache.getScoped(factory, key, getWrappedScope(scope), this);
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  @Override
  public <T, K extends SharedResourceKey> void bindSharedResourceAtScope(SharedResourceFactory<T, K, S> factory,
      K key, S scopeType, T instance) throws NoSuchScopeException {
    this.brokerCache.put(factory, key, getWrappedScope(scopeType), instance);
  }

  /**
   * Get a {@link gobblin.broker.iface.ConfigView} for the input scope, key, and factory.
   */
  public <K extends SharedResourceKey> KeyedScopedConfigViewImpl<S, K> getConfigView(S scope, K key, String factoryName) {

    Config config = ConfigFactory.empty();
    for (ScopedConfig<S> scopedConfig : this.scopedConfigs) {
      if (scopedConfig.getScopeType().equals(scopedConfig.getScopeType().rootScope())) {
        config = ConfigUtils.getConfigOrEmpty(scopedConfig.getConfig(), factoryName).withFallback(config);
      } else if (scope != null && SharedResourcesBrokerUtils.isScopeTypeAncestor(scope, scopedConfig.getScopeType())) {
        Config tmpConfig = ConfigUtils.getConfigOrEmpty(scopedConfig.getConfig(), factoryName);
        tmpConfig = ConfigUtils.getConfigOrEmpty(tmpConfig, scope.name());
        config = tmpConfig.atKey(scope.name()).withFallback(config);
      }
    }

    return new KeyedScopedConfigViewImpl<>(scope, key, factoryName, config);
  }

  NonExtendableBrokerView<S> getScopedView(final S scope) throws NoSuchScopeException {
    return new NonExtendableBrokerView<>(this.brokerCache, getWrappedScope(scope), this.scopedConfigs,
        Maps.filterKeys(this.ancestorScopesByType, new Predicate<S>() {
          @Override
          public boolean apply(@Nullable S input) {
            return SharedResourcesBrokerUtils.isScopeTypeAncestor(scope, input);
          }
        }));
  }

  ScopeWrapper<S> getWrappedScope(S scopeType) throws NoSuchScopeException {
    if (!this.ancestorScopesByType.containsKey(scopeType)) {
      throw new NoSuchScopeException(scopeType);
    }
    return this.ancestorScopesByType.get(scopeType);
  }

  ScopeWrapper<S> getWrappedSelfScope() {
    return this.selfScopeWrapper;
  }

  /**
   * Stores overrides of {@link Config} applicable to a specific {@link ScopeType} and its descendants.
   */
  @Data
  static class ScopedConfig<T extends ScopeType<T>> {
    private final T scopeType;
    private final Config config;
  }

  /**
   * Get a builder to create a descendant {@link SharedResourcesBrokerImpl} (i.e. its leaf scope is a descendant of this
   * broker's leaf scope) and the same backing {@link DefaultBrokerCache}.
   *
   * @param subscope the {@link ScopeInstance} of the new {@link SharedResourcesBroker}.
   * @return a {@link SubscopedBrokerBuilder}.
   */
  @Override
  public SubscopedBrokerBuilder newSubscopedBuilder(ScopeInstance<S> subscope) {
    return new SubscopedBrokerBuilder(subscope);
  }

  /**
   * A builder used to create a descendant {@link SharedResourcesBrokerImpl} with the same backing {@link DefaultBrokerCache}.
   */
  @NotThreadSafe
  public class SubscopedBrokerBuilder implements gobblin.broker.iface.SubscopedBrokerBuilder<S, SharedResourcesBrokerImpl<S>> {
    private final ScopeInstance<S> scope;
    private final Map<S, ScopeWrapper<S>> ancestorScopes = Maps.newHashMap();
    private Config config = ConfigFactory.empty();

    private SubscopedBrokerBuilder(ScopeInstance<S> scope) {
      Preconditions.checkNotNull(scope, "Subscope instance cannot be null.");

      this.scope = scope;

      if (SharedResourcesBrokerImpl.this.selfScopeWrapper != null) {
        ancestorScopes.put(SharedResourcesBrokerImpl.this.selfScopeWrapper.getType(), SharedResourcesBrokerImpl.this.selfScopeWrapper);
      }
    }

    /**
     * Specify additional ancestor {@link SharedResourcesBrokerImpl}. Useful when a {@link ScopeType} has multiple parents.
     */
    public SubscopedBrokerBuilder withAdditionalParentBroker(SharedResourcesBroker<S> broker) {
      if (!(broker instanceof SharedResourcesBrokerImpl) ||
          !((SharedResourcesBrokerImpl) broker).brokerCache.equals(SharedResourcesBrokerImpl.this.brokerCache)) {
        throw new IllegalArgumentException("Additional parent broker is not compatible.");
      }

      this.ancestorScopes.put(broker.selfScope().getType(), ((SharedResourcesBrokerImpl<S>) broker).selfScopeWrapper);
      return this;
    }

    /**
     * Specify {@link Config} overrides. Note these overrides will only be applicable at the new leaf scope and descendant
     * scopes. {@link Config} entries must start with {@link BrokerConstants#GOBBLIN_BROKER_CONFIG_PREFIX} (any entries
     * not satisfying that condition will be ignored).
     */
    public SubscopedBrokerBuilder withOverridingConfig(Config config) {
      this.config = ConfigUtils.getConfigOrEmpty(config, BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX).withFallback(this.config);
      return this;
    }

    /**
     * @return the new {@link SharedResourcesBrokerImpl}.
     */
    public SharedResourcesBrokerImpl<S> build() {

      Map<S, ScopeWrapper<S>> scopeMap = Maps.newHashMap();
      for (ScopeWrapper<S> scopeWrapper : this.ancestorScopes.values()) {
        addScopeAndAncestorsToScopeMap(scopeMap, scopeWrapper);
      }

      ScopeWrapper<S> newScope = createWrappedScope(this.scope, scopeMap, this.scope.getType());

      if (SharedResourcesBrokerImpl.this.selfScopeWrapper != null && !SharedResourcesBrokerUtils.isScopeAncestor(newScope, SharedResourcesBrokerImpl.this.selfScopeWrapper)) {
        throw new IllegalArgumentException(String.format("Child scope %s must be a child of leaf scope %s.", newScope.getType(),
            SharedResourcesBrokerImpl.this.selfScopeWrapper.getType()));
      }

      List<ScopedConfig<S>> scopedConfigs = Lists.newArrayList(SharedResourcesBrokerImpl.this.scopedConfigs);
      if (!this.config.isEmpty()) {
        scopedConfigs.add(new ScopedConfig<>(newScope.getType(), this.config));
      }

      return new SharedResourcesBrokerImpl<>(SharedResourcesBrokerImpl.this.brokerCache, newScope, scopedConfigs, scopeMap);
    }

    private ScopeWrapper<S> createWrappedScope(ScopeInstance<S> scope, Map<S, ScopeWrapper<S>> scopeMap, S mainScopeType)
        throws IllegalArgumentException {

      List<ScopeWrapper<S>> parentScopes = Lists.newArrayList();

      ScopeType<S> scopeType = scope.getType();

      if (scopeType.parentScopes() != null) {
        for (S tpe : scopeType.parentScopes()) {
          if (scopeMap.containsKey(tpe)) {
            parentScopes.add(scopeMap.get(tpe));
          } else if (tpe.defaultScopeInstance() != null) {
            ScopeInstance<S> defaultInstance = tpe.defaultScopeInstance();
            if (!defaultInstance.getType().equals(tpe)) {
              throw new RuntimeException(String.format("Default scope instance %s for scope type %s is not of type %s.",
                  defaultInstance, tpe, tpe));
            }
            parentScopes.add(createWrappedScope(tpe.defaultScopeInstance(), scopeMap, mainScopeType));
          } else {
            throw new IllegalArgumentException(String.format(
                "Scope %s is an ancestor of %s, however it does not have a default id and is not provided as an acestor scope.",
                tpe, mainScopeType));
          }
        }
      }

      ScopeWrapper<S> wrapper = new ScopeWrapper<>(scope.getType(), scope, parentScopes);
      scopeMap.put(wrapper.getType(), wrapper);
      return wrapper;
    }

    private void addScopeAndAncestorsToScopeMap(Map<S, ScopeWrapper<S>> scopeMap, ScopeWrapper<S> scope) {

      if (scope == null) {
        return;
      }

      Queue<ScopeWrapper<S>> ancestors = new LinkedList<>();
      ancestors.add(scope);

      while (!ancestors.isEmpty()) {
        ScopeWrapper<S> thisScope = ancestors.poll();
        if (!scopeMap.containsKey(thisScope.getType())) {
          scopeMap.put(thisScope.getType(), thisScope);
        } else if (!scopeMap.get(thisScope.getType()).equals(thisScope)) {
          throw new IllegalStateException(String.format("Multiple scopes found with type %s but different identity: %s and %s.",
              thisScope.getType(), thisScope.getScope(), scopeMap.get(thisScope.getType()).getScope()));
        }
        ancestors.addAll(thisScope.getParentScopes());
      }
    }

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SharedResourcesBrokerImpl<?> that = (SharedResourcesBrokerImpl<?>) o;

    if (!brokerCache.equals(that.brokerCache)) {
      return false;
    }
    if (!ancestorScopesByType.equals(that.ancestorScopesByType)) {
      return false;
    }
    return selfScopeWrapper != null ? selfScopeWrapper.equals(that.selfScopeWrapper) : that.selfScopeWrapper == null;
  }

  @Override
  public int hashCode() {
    int result = brokerCache.hashCode();
    result = 31 * result + ancestorScopesByType.hashCode();
    result = 31 * result + (selfScopeWrapper != null ? selfScopeWrapper.hashCode() : 0);
    return result;
  }

  @Override
  public void close()
      throws IOException {
    this.brokerCache.close(this.selfScopeWrapper);
  }
}
