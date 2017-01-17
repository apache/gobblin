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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.broker.iface.ScopeInstance;
import gobblin.broker.iface.ScopeType;
import gobblin.util.ConfigUtils;


/**
 * Used to create a default implementation of {@link gobblin.broker.iface.SharedResourcesBroker}.
 */
public class SharedResourcesBrokerFactory {

  public static <S extends ScopeType<S>> SharedResourcesBrokerImpl<S> createDefaultTopLevelBroker(Config config,
      ScopeInstance<S> globalScope) {

    if (!globalScope.getType().equals(globalScope.getType().rootScope())) {
      throw new IllegalArgumentException(String.format("The top level broker must be created at the root scope type. "
          + "%s is not a root scope type.", globalScope.getType()));
    }

    ScopeWrapper<S> scopeWrapper = new ScopeWrapper<>(globalScope.getType(), globalScope, Lists.<ScopeWrapper<S>>newArrayList());

    return new SharedResourcesBrokerImpl<>(new DefaultBrokerCache<S>(),
        scopeWrapper,
        Lists.newArrayList(new SharedResourcesBrokerImpl.ScopedConfig<>(globalScope.getType(),
            ConfigUtils.getConfigOrEmpty(config, BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX))),
        ImmutableMap.of(globalScope.getType(), scopeWrapper));
  }

}
