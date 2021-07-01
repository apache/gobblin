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

package org.apache.gobblin.broker;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Striped;

import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.broker.iface.NoSuchScopeException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourceKey;


/**
 * A backing cache for shared resources used by {@link SharedResourcesBrokerImpl}. Stores created objects in a guava
 * cache keyed by factory, scope, and key.
 * @param <S> the {@link ScopeType} class for the scopes topology used in this tree of brokers.
 */
@Slf4j
class DefaultBrokerCache<S extends ScopeType<S>> {

  private final Cache<RawJobBrokerKey, Object> sharedResourceCache;
  private final Cache<RawJobBrokerKey, ScopeWrapper<S>> autoScopeCache;
  private final Striped<Lock> invalidationLock;

  public DefaultBrokerCache() {
    this.sharedResourceCache = CacheBuilder.newBuilder().build();
    this.autoScopeCache = CacheBuilder.newBuilder().build();
    this.invalidationLock = Striped.lazyWeakLock(20);
  }

  /**
   * The key for shared resources in the cache.
   */
  @Data
  class RawJobBrokerKey {
    // Left if the key represents
    private final ScopeWrapper<S> scope;
    private final String factoryName;
    private final SharedResourceKey key;
  }

  /**
   * Get an object for the specified factory, key, and broker at the scope selected by the factory. {@link DefaultBrokerCache}
   * guarantees that calling this method from brokers with the same leaf scope will return the same object.
   */
  @SuppressWarnings(value = "unchecked")
  <T, K extends SharedResourceKey> T getAutoScoped(final SharedResourceFactory<T, K, S> factory, final K key,
      final SharedResourcesBrokerImpl<S> broker)
      throws ExecutionException {

    // figure out auto scope
    RawJobBrokerKey autoscopeCacheKey = new RawJobBrokerKey(broker.getWrappedSelfScope(), factory.getName(), key);
    ScopeWrapper<S> selectedScope = this.autoScopeCache.get(autoscopeCacheKey, new Callable<ScopeWrapper<S>>() {
      @Override
      public ScopeWrapper<S> call() throws Exception {
        return broker.getWrappedScope(factory.getAutoScope(broker, broker.getConfigView(null, key, factory.getName())));
      }
    });

    // get actual object
    return getScoped(factory, key, selectedScope, broker);
  }

  /**
   * Get a scoped object from the cache.
   */
  @SuppressWarnings(value = "unchecked")
  <T, K extends SharedResourceKey> SharedResourceFactoryResponse<T> getScopedFromCache(
      final SharedResourceFactory<T, K, S> factory, @Nonnull final K key,
      @Nonnull final ScopeWrapper<S> scope, final SharedResourcesBrokerImpl<S> broker)
      throws ExecutionException {
    RawJobBrokerKey fullKey = new RawJobBrokerKey(scope, factory.getName(), key);
    Object obj = this.sharedResourceCache.get(fullKey, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return factory.createResource(broker.getScopedView(scope.getType()), broker.getConfigView(scope.getType(), key,
            factory.getName()));
      }
    });

    return (SharedResourceFactoryResponse<T>)obj;
  }

  /**
   * Get an object for the specified factory, key, scope, and broker. {@link DefaultBrokerCache}
   * guarantees that calling this method for the same factory, key, and scope will return the same object.
   */
  @SuppressWarnings(value = "unchecked")
  <T, K extends SharedResourceKey> T getScoped(final SharedResourceFactory<T, K, S> factory, @Nonnull final K key,
      @Nonnull final ScopeWrapper<S> scope, final SharedResourcesBrokerImpl<S> broker)
      throws ExecutionException {
    SharedResourceFactory<T, K, S> currentFactory = factory;
    K currentKey = key;
    ScopeWrapper<S> currentScope = scope;

    Object obj = getScopedFromCache(currentFactory, currentKey, currentScope, broker);

    // this loop is to continue looking up objects through redirection or reloading until a valid resource is found
    while (true) {
      if (obj instanceof ResourceCoordinate) {
        ResourceCoordinate<T, K, S> resourceCoordinate = (ResourceCoordinate<T, K, S>) obj;
        if (!SharedResourcesBrokerUtils.isScopeTypeAncestor((ScopeType) currentScope.getType(), ((ResourceCoordinate) obj).getScope())) {
          throw new RuntimeException(String
              .format("%s returned an invalid coordinate: scope %s is not an ancestor of %s.", currentFactory.getName(),
                  ((ResourceCoordinate) obj).getScope(), currentScope.getType()));
        }
        try {
          obj = getScopedFromCache(resourceCoordinate.getFactory(), resourceCoordinate.getKey(),
              broker.getWrappedScope(resourceCoordinate.getScope()), broker);
        } catch (NoSuchScopeException nsse) {
          throw new RuntimeException(String
              .format("%s returned an invalid coordinate: scope %s is not available.", factory.getName(),
                  resourceCoordinate.getScope().name()), nsse);
        }
      } else if (obj instanceof ResourceEntry) {
        T resource = ((ResourceEntry<T>) obj).getResourceIfValid();

        // valid resource found
        if (resource != null) {
          return resource;
        }

        // resource is invalid. The lock in this block is to reduce the chance of starvation where a thread keeps
        // getting objects that are invalidated by another thread.
        Lock lock = this.invalidationLock.get(key);
        try {
          lock.lock();
          RawJobBrokerKey fullKey = new RawJobBrokerKey(currentScope, currentFactory.getName(), currentKey);
          safeInvalidate(fullKey);
          obj = getScopedFromCache(currentFactory, currentKey, currentScope, broker);
        } finally {
          lock.unlock();
        }
      } else {
        throw new RuntimeException(String.format("Invalid response from %s: %s.", factory.getName(), obj.getClass()));
      }
    }
  }

  <T, K extends SharedResourceKey> void put(final SharedResourceFactory<T, K, S> factory, @Nonnull final K key,
      @Nonnull final ScopeWrapper<S> scope, T instance) {
    RawJobBrokerKey fullKey = new RawJobBrokerKey(scope, factory.getName(), key);
    this.sharedResourceCache.put(fullKey, new ResourceInstance<>(instance));
  }

  private void safeInvalidate(RawJobBrokerKey key) {
    Lock lock = this.invalidationLock.get(key);
    lock.lock();
    try {
      Object obj = this.sharedResourceCache.getIfPresent(key);

      if (obj != null && obj instanceof ResourceEntry && !((ResourceEntry) obj).isValid()) {
        this.sharedResourceCache.invalidate(key);
        ((ResourceEntry) obj).onInvalidate();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Invalidate all objects at scopes which are descendant of the input scope. Any such invalidated object that is a
   * {@link Closeable} will be closed, and any such object which is a {@link Service} will be shutdown.
   * @throws IOException
   */
  public void close(ScopeWrapper<S> scope)
      throws IOException {
    List<Throwable> exceptionsList = Lists.newArrayList();
    List<Service> awaitShutdown = Lists.newArrayList();

    for (Map.Entry<RawJobBrokerKey, Object> entry : Maps.filterKeys(this.sharedResourceCache.asMap(),
        new ScopeIsAncestorFilter(scope)).entrySet()) {

      this.sharedResourceCache.invalidate(entry.getKey());

      if (entry.getValue() instanceof ResourceInstance) {
        Object obj = ((ResourceInstance) entry.getValue()).getResource();
        // Catch unchecked exception while closing resources, make sure all resources managed by cache are closed.
        try {
          SharedResourcesBrokerUtils.shutdownObject(obj, log);
        } catch (Throwable t) {
          exceptionsList.add(t);
        }
        if (obj instanceof Service) {
          awaitShutdown.add((Service) obj);
        }
      }
    }

    for (Service service : awaitShutdown) {
      try {
        service.awaitTerminated(10, TimeUnit.SECONDS);
      } catch (TimeoutException te) {
        log.error("Failed to shutdown {}.", service);
      }
    }

    // log exceptions while closing resources up.
    if (exceptionsList.size() > 0) {
      log.error(exceptionsList.stream()
          .map(Throwables::getStackTraceAsString).collect(Collectors.joining("\n")));
    }
  }

  /**
   * Filter {@link RawJobBrokerKey} that are not descendants of the input {@link ScopeWrapper}.
   */
  @AllArgsConstructor
  private class ScopeIsAncestorFilter implements Predicate<RawJobBrokerKey> {
    private final ScopeWrapper<S> scope;

    @Override
    public boolean apply(RawJobBrokerKey input) {
      if (this.scope == null) {
        return true;
      }
      if (input.getScope() == null) {
        return false;
      }
      return SharedResourcesBrokerUtils.isScopeAncestor(input.getScope(), this.scope);
    }
  }
}
