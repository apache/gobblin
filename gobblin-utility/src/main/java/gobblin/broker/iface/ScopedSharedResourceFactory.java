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

/**
 * A factory that creates {@link ScopeInstance} specific objects.
 * @param <T> type of objects this factory creates.
 * @param <K> type of {@link SharedResourceKey} this factory uses.
 */
public interface ScopedSharedResourceFactory<T, K extends SharedResourceKey> {
  /**
   * Create an object for the provided {@link SharedResourceKey}, with the provided configuration.
   */
  T createResource(SharedResourcesBroker broker, ScopedConfigView<?, K> config);
}
