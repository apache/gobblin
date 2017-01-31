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

package gobblin.service.modules.flow;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import gobblin.runtime.api.FlowSpec;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.Spec;
import gobblin.runtime.api.SpecCatalogListener;
import gobblin.runtime.api.SpecCompiler;
import gobblin.runtime.api.SpecExecutorInstance;
import gobblin.runtime.api.TopologySpec;


/***
 * Take in a logical {@link Spec} ie flow and compile corresponding materialized job {@link Spec}
 * and its mapping to {@link SpecExecutorInstance}.
 */
public class IdentityFlowToJobSpecCompiler implements SpecCompiler, SpecCatalogListener {

  public static final String FLOW_SOURCE_IDENTIFIER_KEY = "gobblin.flow.sourceIdentifier";
  public static final String FLOW_DESTINATION_IDENTIFIER_KEY = "gobblin.flow.destinationIdentifier";

  private final Map<URI, TopologySpec> topologySpecMap;

  public IdentityFlowToJobSpecCompiler() {
    topologySpecMap = Maps.newConcurrentMap();
  }

  @Override
  public Map<Spec, SpecExecutorInstance> compileFlow(Spec spec) {
    Preconditions.checkNotNull(spec);
    Preconditions.checkArgument(spec instanceof FlowSpec, "IdentityFlowToJobSpecCompiler only converts FlowSpec to JobSpec");

    Map<Spec, SpecExecutorInstance> specExecutorInstanceMap = Maps.newHashMap();

    FlowSpec flowSpec = (FlowSpec) spec;
    String source = flowSpec.getConfig().getString(FLOW_SOURCE_IDENTIFIER_KEY);
    String destination = flowSpec.getConfig().getString(FLOW_DESTINATION_IDENTIFIER_KEY);

    JobSpec jobSpec = JobSpec.builder(flowSpec.getUri())
        .withConfig(flowSpec.getConfig())
        .withTemplate(flowSpec.getTemplateURIs().get().iterator().next())
        .withDescription(flowSpec.getDescription())
        .build();

    for (TopologySpec topologySpec : topologySpecMap.values()) {
      try {
        Map<String, String> capabilities = (Map<String, String>) topologySpec.getSpecExecutorInstance().getCapabilities().get();
        for (Map.Entry<String, String> capability : capabilities.entrySet()) {
          if (source.equals(capability.getKey()) && destination.equals(capability.getValue())) {
            specExecutorInstanceMap.put(jobSpec, topologySpec.getSpecExecutorInstance());
          }
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException("Cannot determine topology capabilities", e);
      }
    }

    return specExecutorInstanceMap;
  }

  @Override
  public void onAddSpec(Spec addedSpec) {
    topologySpecMap.put(addedSpec.getUri(), (TopologySpec) addedSpec);
  }

  @Override
  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion) {
    topologySpecMap.remove(deletedSpecURI);
  }

  @Override
  public void onUpdateSpec(Spec updatedSpec) {
    topologySpecMap.put(updatedSpec.getUri(), (TopologySpec) updatedSpec);
  }
}
