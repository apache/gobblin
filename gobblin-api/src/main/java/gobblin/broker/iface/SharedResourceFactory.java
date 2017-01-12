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

/**
 * A factory that creates {@link ScopeInstance} specific objects.
 * @param <T> type of objects this factory creates.
 * @param <K> type of {@link SharedResourceKey} this factory uses.
 */
public interface SharedResourceFactory<T, K extends SharedResourceKey, S extends ScopeType<S>> {
  /**
   * @return An identifier for this factory. Users will configure shared resources using this name.
   */
  String getName();

  /**
   * Create an object for the provided {@link SharedResourceKey}, with the provided configuration.
   */
  T createResource(SharedResourcesBroker broker, ScopedConfigView<?, K> config);

  /**
   * @return The {@link ScopeType} at which an auto scoped resource should be created. A good default is to return
   *         broker.selfScope()
   */
  S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, K> config);
}
