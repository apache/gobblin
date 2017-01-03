/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.broker.iface;

import gobblin.util.Either;


/**
 * A factory that creates auto-scoped objects (that is, the factory, as opposed to the user,
 * decides the scope at which the resource is created).
 * @param <T> type of objects this factory creates.
 * @param <K> type of {@link SharedResourceKey} this factory uses.
 */
public interface AutoScopedSharedResourceFactory<T, K extends SharedResourceKey, S extends ScopeType<S>> {

  /**
   * @return An identifier for this factory. Users will configure shared resources using this name.
   */
  String getName();

  /**
   * Create an object with the provided configuration at a factory chosen {@link ScopeType}.
   *
   * If the factory decides the object should be created at the {@link SharedResourcesBroker#leafScope()}, it can simply
   * return the created object.
   *
   * If the factory decides the object should be created at a scope other than the {@link SharedResourcesBroker#leafScope()},
   * it must return the {@link ScopeType} at which the object should be created, and delegate the creation of the object
   * to the broker. Note this option should only be used if the {@link AutoScopedSharedResourceFactory} implementation
   * is also a {@link ScopedSharedResourceFactory}.
   *
   * @return Either the created object, indicating the broker that the object is at the {@link SharedResourcesBroker#leafScope()}
   *         or a {@link ScopeType} indicating the broker should use the object at the provided {@link ScopeType}.
   */
  Either<T, S> createAutoscopedResource(SharedResourcesBroker broker, ConfigView<S, K> config);
}
