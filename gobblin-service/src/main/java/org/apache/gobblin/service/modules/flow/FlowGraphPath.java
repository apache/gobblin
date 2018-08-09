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

package org.apache.gobblin.service.modules.flow;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import lombok.Getter;

import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.dataset.DatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.FlowEdge;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.service.modules.template.FlowTemplate;


/**
 * A class that encapsulates a path in the {@link org.apache.gobblin.service.modules.flowgraph.FlowGraph}.
 */
public class FlowGraphPath {
  @Getter
  private List<List<FlowEdgeContext>> paths;
  private FlowSpec flowSpec;
  private Long flowExecutionId;

  public FlowGraphPath(FlowSpec flowSpec, Long flowExecutionId) {
    this.flowSpec = flowSpec;
    this.flowExecutionId = flowExecutionId;
  }

  public void addPath(List<FlowEdgeContext> path) {
    if (this.paths == null) {
      this.paths = new ArrayList<>();
    }
    this.paths.add(path);
  }

  public Dag<JobExecutionPlan> asDag() throws SpecNotFoundException, JobTemplate.TemplateException, URISyntaxException {
    Dag<JobExecutionPlan> flowDag = new Dag<>(new ArrayList<>());

    for(List<FlowEdgeContext> path: paths) {
      Dag<JobExecutionPlan> pathDag = new Dag<>(new ArrayList<>());
      Iterator<FlowEdgeContext> pathIterator = path.iterator();
      while (pathIterator.hasNext()) {
        Dag<JobExecutionPlan> flowEdgeDag = convertHopToDag(pathIterator.next());
        pathDag = pathDag.concatenate(flowEdgeDag);
      }
      flowDag = flowDag.merge(pathDag);
    }
    return flowDag;
  }

  /**
   * Given an instance of {@link FlowEdge}, this method returns a {@link Dag < JobExecutionPlan >} that moves data
   * from the source of the {@link FlowEdge} to the destination of the {@link FlowEdge}.
   * @param flowEdgeContext an instance of {@link FlowEdgeContext}.
   * @return a {@link Dag} of {@link JobExecutionPlan}s associated with the {@link FlowEdge}.
   */
  private Dag<JobExecutionPlan> convertHopToDag(FlowEdgeContext flowEdgeContext)
      throws SpecNotFoundException, JobTemplate.TemplateException, URISyntaxException {
    FlowTemplate flowTemplate = flowEdgeContext.getEdge().getFlowTemplate();
    DatasetDescriptor inputDatasetDescriptor = flowEdgeContext.getInputDatasetDescriptor();
    DatasetDescriptor outputDatasetDescriptor = flowEdgeContext.getOutputDatasetDescriptor();
    Config mergedConfig = flowEdgeContext.getMergedConfig();
    SpecExecutor specExecutor = flowEdgeContext.getSpecExecutor();

    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();

    //Get resolved job configs from the flow template
    List<Config> resolvedJobConfigs = flowTemplate.getResolvedJobConfigs(mergedConfig, inputDatasetDescriptor, outputDatasetDescriptor);
    //Iterate over each resolved job config and convert the config to a JobSpec.
    for (Config resolvedJobConfig : resolvedJobConfigs) {
      jobExecutionPlans.add(new JobExecutionPlan.Factory().createPlan(flowSpec, resolvedJobConfig, specExecutor, flowExecutionId));
    }
    return new JobExecutionPlanDagFactory().createDag(jobExecutionPlans);
  }
}
