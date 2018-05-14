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

package org.apache.gobblin.service.modules.template;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.annotation.Alpha;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.service.modules.flowgraph.Dag;

import lombok.extern.slf4j.Slf4j;


/**
 * A Factory class used for constructing a {@link Dag} of {@link org.apache.gobblin.runtime.api.JobTemplate}s from
 * a {@link URI} of a {@link FlowTemplate}.
 */
@Alpha
@Slf4j
public class JobTemplateDagFactory {
  public static final String JOB_TEMPLATE_FILE_SUFFIX = ".conf";

  public static Dag<JobTemplate> createDagFromJobTemplates(List<JobTemplate> jobTemplates) {
    Map<URI, Dag.DagNode<JobTemplate>> uriJobTemplateMap = new HashMap<>();
    List<Dag.DagNode<JobTemplate>> dagNodeList = new ArrayList<>();
    /**
     * Create a {@link Dag.DagNode<JobTemplate>} for every {@link JobTemplate} in the flow. Add this node
     * to a {@link Map<URI,JobTemplate>}.
     */
    for (JobTemplate template : jobTemplates) {
      Dag.DagNode<JobTemplate> dagNode = new Dag.DagNode<>(template);
      dagNodeList.add(dagNode);
      uriJobTemplateMap.put(template.getUri(), dagNode);
    }

    /**
     * Iterate over each {@link JobTemplate} to get the dependencies of each {@link JobTemplate}.
     * For each {@link JobTemplate}, get the corresponding {@link Dag.DagNode} and
     * set the {@link Dag.DagNode}s corresponding to the dependencies as its parent nodes.
     *
     * TODO: we likely do not need 2 for loops and we can do this in 1 pass.
     */
    Path templateDirPath = new Path(jobTemplates.get(0).getUri()).getParent();
    for (JobTemplate template : jobTemplates) {
      URI templateUri = template.getUri();
      Dag.DagNode<JobTemplate> node = uriJobTemplateMap.get(templateUri);
      Collection<String> dependencies = template.getDependencies();
      for (String dependency : dependencies) {
        URI dependencyUri = new Path(templateDirPath, dependency).suffix(JOB_TEMPLATE_FILE_SUFFIX).toUri();
        Dag.DagNode<JobTemplate> parentNode = uriJobTemplateMap.get(dependencyUri);
        node.addParentNode(parentNode);
      }
    }
    Dag<JobTemplate> dag = new Dag<>(dagNodeList);
    return dag;
  }
}
