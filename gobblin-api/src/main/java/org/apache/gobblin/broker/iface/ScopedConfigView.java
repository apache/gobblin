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

package org.apache.gobblin.broker.iface;

import com.typesafe.config.Config;


/**
 * Provides a view of a {@link Config} specific to a factory, {@link SharedResourceKey} and {@link ScopeInstance}.
 */
public interface ScopedConfigView<S extends ScopeType<S>, K extends SharedResourceKey>
    extends ConfigView<S, K> {

  /**
   * @return The {@link ScopeInstance} this {@link ScopedConfigView} was created for.
   */
  S getScope();

}
