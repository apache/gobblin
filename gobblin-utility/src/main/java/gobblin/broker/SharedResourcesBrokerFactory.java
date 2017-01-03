/*
 * Copyright (C) 2014-2017 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.broker;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.broker.iface.ScopeType;
import gobblin.util.ConfigUtils;


/**
 * Used to create a default implementation of {@link gobblin.broker.iface.SharedResourcesBroker}.
 */
public class SharedResourcesBrokerFactory {

  public static <S extends ScopeType<S>> SharedResourcesBrokerImpl<S> createDefaultTopLevelBroker(Config config) {
    return new SharedResourcesBrokerImpl<>(new DefaultBrokerCache<S>(), null,
        Lists.newArrayList(new SharedResourcesBrokerImpl.ScopedConfig<S>(null,
            ConfigUtils.getConfigOrEmpty(config, BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX))));
  }

}
