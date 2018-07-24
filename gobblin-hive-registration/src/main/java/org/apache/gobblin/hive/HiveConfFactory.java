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

package org.apache.gobblin.hive;

import org.apache.gobblin.broker.EmptyKey;
import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopeType;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.hadoop.hive.conf.HiveConf;


/**
 * The factory that creates a {@link HiveConf} as shared resource.
 * {@link EmptyKey} is fair since {@link HiveConf} seems to be read-only.
 */
public class HiveConfFactory<S extends ScopeType<S>> implements SharedResourceFactory<HiveConf, EmptyKey, S> {
  public static final String FACTORY_NAME = "hiveConfFactory";

  @Override
  public String getName() {
    return FACTORY_NAME;
  }

  @Override
  public SharedResourceFactoryResponse<HiveConf> createResource(SharedResourcesBroker<S> broker,
      ScopedConfigView<S, EmptyKey> config)
      throws NotConfiguredException {
    // We could extend the constructor of HiveConf to accept arguments in the future.
    return new ResourceInstance<>(new HiveConf());
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, EmptyKey> config) {
    return broker.selfScope().getType().rootScope();
  }
}
