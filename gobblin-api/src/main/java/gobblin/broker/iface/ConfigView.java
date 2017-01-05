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

import com.typesafe.config.Config;


/**
 * Provides a view of a {@link Config} specific to a {@link SharedResourceKey} and factory.
 */
public interface ConfigView<S extends ScopeType<S>, K extends SharedResourceKey> {

  /**
   * @return The name of the factory this {@link ConfigView} was created for.
   */
  String getFactoryName();

  /**
   * @return The {@link SharedResourceKey} this {@link ConfigView} was created for.
   */
  K getKey();

  /**
   * @return the {@link Config} to use for creation of the new resource.
   */
  Config getConfig();

  /**
   * @return get a view of this {@link ConfigView} at a specific {@link ScopeType}.
   */
  ScopedConfigView<S, K> getScopedView(S scopeType);
}
