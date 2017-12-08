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

package org.apache.gobblin.metrics.broker;

import org.apache.gobblin.broker.EmptyKey;
import org.apache.gobblin.broker.ResourceInstance;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.apache.gobblin.broker.iface.SharedResourceFactory;
import org.apache.gobblin.broker.iface.SharedResourceFactoryResponse;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;


/**
 * A {@link SharedResourceFactory} to share a job level {@link LineageInfo} instance
 */
public class LineageInfoFactory implements SharedResourceFactory<LineageInfo, EmptyKey, GobblinScopeTypes> {
  public static final String FACTORY_NAME = "lineageInfo";

  @Override
  public String getName() {
    return FACTORY_NAME;
  }

  @Override
  public SharedResourceFactoryResponse<LineageInfo> createResource(SharedResourcesBroker<GobblinScopeTypes> broker,
      ScopedConfigView<GobblinScopeTypes, EmptyKey> config)
      throws NotConfiguredException {
    return new ResourceInstance<>(new LineageInfo(config.getConfig()));
  }

  @Override
  public GobblinScopeTypes getAutoScope(SharedResourcesBroker<GobblinScopeTypes> broker, ConfigView<GobblinScopeTypes, EmptyKey> config) {
    return GobblinScopeTypes.JOB;
  }
}
