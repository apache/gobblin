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

package gobblin.service;

import gobblin.runtime.api.SpecCatalog;
import gobblin.runtime.api.SpecStore;
import gobblin.service.modules.flow.IdentityFlowToJobSpecCompiler;


public class ServiceConfigKeys {

  // FlowSpec Store Keys
  public static final String GOBBLIN_SERVICE_FLOWSPEC_STORE_CLASS_KEY = "gobblin.service.flowSpec.store.class";
  public static final String DEFAULT_GOBBLIN_SERVICE_FLOWSPEC_STORE_CLASS = SpecCatalog.class.getCanonicalName();

  // TopologySpec Store Keys
  public static final String GOBBLIN_SERVICE_TOPOLOGYSPEC_STORE_CLASS_KEY = "gobblin.service.topologySpec.store.class";
  public static final String DEFAULT_GOBBLIN_SERVICE_TOPOLOGYSPEC_STORE_CLASS = SpecStore.class.getCanonicalName();

  // Flow Compiler Keys
  public static final String GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY = "gobblin.service.flowCompiler.class";
  public static final String DEFAULT_GOBBLIN_SERVICE_FLOWCOMPILER_CLASS = IdentityFlowToJobSpecCompiler.class.getCanonicalName();

  // Flow specific keys
  public static final String FLOW_SOURCE_IDENTIFIER_KEY = "gobblin.flow.sourceIdentifier";
  public static final String FLOW_DESTINATION_IDENTIFIER_KEY = "gobblin.flow.destinationIdentifier";

}
