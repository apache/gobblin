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

package gobblin.broker.iface;

import java.io.Closeable;
import java.io.IOException;

/**
 * A class that provides access to objects shared by multiple components within a process, as well as objects virtually
 * shared among different processes (i.e. objects that synchronize with equivalent objects in other processes).
 *
 * Different parts of an application may require objects that are ideally shared. For example, if multiple objects are
 * writing to the same file, it is convenient to have a single file handle. Instead of passing around all shared objects,
 * using static objects, or doing dependency injection, a {@link SharedResourcesBroker} provides a way to acquire such shared
 * objects as needed, letting the broker manage the lifecycle of the objects.
 *
 * Objects are created using {@link SharedResourceFactory}s,
 * and are specific to a particular {@link ScopeInstance} and
 * {@link SharedResourceKey}. {@link ScopeInstance}s represent a DAG of relevant scopes in the application, for example
 * GLOBAL -> JOB -> TASK. {@link SharedResourceKey} identify different objects created by the same factory, for example
 * handles to different files could be keyed by the file path.
 *
 * {@link SharedResourcesBroker} guarantees that multiple requests for objects with the same factory,
 * {@link ScopeInstance} and {@link SharedResourceKey} return the same object, even if called for different {@link SharedResourcesBroker}
 * instances. This guarantee requires that new brokers are created using {@link #newSubscopedBuilder(ScopeInstance)} only.
 *
 * @param <S> the {@link ScopeType} tree used by this {@link SharedResourcesBroker}.
 */
public interface SharedResourcesBroker<S extends ScopeType<S>> extends Closeable {

  /**
   * @return The lowest defined {@link ScopeInstance} in this {@link SharedResourcesBroker}.
   * This is provides the lowest {@link ScopeType} at which the {@link SharedResourcesBroker} can return shared objects.
   */
  ScopeInstance<S> selfScope();

  /**
   * Get the {@link ScopeInstance} in this brokers topology at the provided {@link ScopeType}.
   * @throws NoSuchScopeException if the input {@link ScopeType} is lower than that of {@link #selfScope()}.
   */
  ScopeInstance<S> getScope(S scopeType) throws NoSuchScopeException;

  /**
   * Get a shared resource created by the input {@link SharedResourceFactory}. The resource will be shared
   * at least by all brokers with the same {@link #selfScope()}, but the factory may chose to create the resource
   * at a higher scope.
   *
   * @param factory The {@link SharedResourceFactory} used to create the shared object.
   * @param key Identifies different objects from the same factory in the same {@link ScopeInstance}.
   * @param <T> type of object created by the factory.
   * @param <K> type of factory accepted by the factory.
   * @return an object of type T.
   * @throws NotConfiguredException
   */
  <T, K extends SharedResourceKey> T getSharedResource(SharedResourceFactory<T, K, S> factory, K key)
      throws NotConfiguredException;

  /**
   * Get a shared resource created by the input {@link SharedResourceFactory} at the {@link ScopeInstance}
   * returned by {@link #getScope)} on the input {@link ScopeType}.
   *
   * @param factory The {@link SharedResourceFactory} used to create the shared object.
   * @param key Identifies different objects from the same factory in the same {@link ScopeInstance}.
   * @param scopeType {@link ScopeType} at which the object will be obtained.
   * @param <T> type of object created by the factory.
   * @param <K> type of factory accepted by the factory.
   * @return an object of type T.
   * @throws NotConfiguredException
   * @throws NoSuchScopeException
   */
  <T, K extends SharedResourceKey> T getSharedResourceAtScope(SharedResourceFactory<T, K, S> factory, K key,
      S scopeType) throws NotConfiguredException, NoSuchScopeException;

  /**
   * Close all resources at this and descendant scopes, meaning {@link Closeable}s will be closed and
   * {@link com.google.common.util.concurrent.Service}s will be shut down. Future calls to get the same resource will
   * instantiate a new resource instead.
   *
   * Best practice guideline: this method should only be called by the object who created the {@link SharedResourcesBroker}.
   * Objects or methods which received an already built {@link SharedResourcesBroker} should not call this method.
   * @throws IOException
   */
  @Override
  void close()
      throws IOException;

  /**
   * Get a builder to create a descendant {@link SharedResourcesBroker}.
   *
   * @param subscope the {@link ScopeInstance} of the new {@link SharedResourcesBroker}.
   * @return a {@link SubscopedBrokerBuilder}.
   */
  SubscopedBrokerBuilder<S, ?> newSubscopedBuilder(ScopeInstance<S> subscope);
}
