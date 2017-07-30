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

package gobblin.broker.iface;

import com.typesafe.config.Config;

import gobblin.broker.BrokerConstants;


/**
 * A builder used to create new {@link SharedResourcesBroker} compatible with an existing {@link SharedResourcesBroker}
 * (i.e. guaranteeing objects are correctly shared among scopes).
 */
public interface SubscopedBrokerBuilder<S extends ScopeType<S>, B extends SharedResourcesBroker<S>> {

  /**
   * Specify additional ancestor {@link SharedResourcesBroker}. Useful when a {@link ScopeType} has multiple parents.
   */
  SubscopedBrokerBuilder<S, B> withAdditionalParentBroker(SharedResourcesBroker<S> broker);

  /**
   * Specify {@link Config} overrides. Note these overrides will only be applicable at the new leaf scope and descendant
   * scopes. {@link Config} entries must start with {@link BrokerConstants#GOBBLIN_BROKER_CONFIG_PREFIX} (any entries
   * not satisfying that condition will be ignored).
   */
  SubscopedBrokerBuilder<S, B> withOverridingConfig(Config config);

  /**
   * @return the new {@link SharedResourcesBroker}.
   */
  B build();
}
