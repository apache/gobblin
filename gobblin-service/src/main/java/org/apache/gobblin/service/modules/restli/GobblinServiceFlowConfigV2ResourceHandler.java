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

package org.apache.gobblin.service.modules.restli;

import org.apache.helix.HelixManager;

import com.google.common.base.Optional;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.gobblin.service.FlowConfigV2ResourceLocalHandler;
import org.apache.gobblin.service.FlowConfigsV2ResourceHandler;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;
import org.apache.gobblin.runtime.util.InjectionNames;


public class GobblinServiceFlowConfigV2ResourceHandler extends GobblinServiceFlowConfigResourceHandler
    implements FlowConfigsV2ResourceHandler {

  @Inject
  public GobblinServiceFlowConfigV2ResourceHandler(@Named(InjectionNames.SERVICE_NAME) String serviceName,
      @Named(InjectionNames.FLOW_CATALOG_LOCAL_COMMIT) boolean flowCatalogLocalCommit,
      FlowConfigV2ResourceLocalHandler handler, Optional<HelixManager> manager, GobblinServiceJobScheduler jobScheduler,
      @Named(InjectionNames.FORCE_LEADER) boolean forceLeader) {
    super(serviceName, flowCatalogLocalCommit, handler, manager, jobScheduler, forceLeader);
  }
}
