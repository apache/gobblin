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

package org.apache.gobblin.service.modules.spec;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.io.Files;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.service.modules.flowgraph.Dag;


/**
 * A Factory class used for constructing a {@link Dag} of {@link JobExecutionPlan}s from
 * a {@link List} of {@link JobExecutionPlan}s.
 */
@Alpha
@Slf4j
public class JobExecutionPlanDagFactory {

  public Dag<JobExecutionPlan> createDag(List<JobExecutionPlan> jobExecutionPlans) {
    //Maintain a mapping between job name and the corresponding JobExecutionPlan.
    Map<String, Dag.DagNode<JobExecutionPlan>> JobExecutionPlanMap = new HashMap<>();
    List<Dag.DagNode<JobExecutionPlan>> dagNodeList = new ArrayList<>();
    /**
     * Create a {@link Dag.DagNode<JobExecutionPlan>} for every {@link JobSpec} in the flow. Add this node
     * to a HashMap.
     */
    for (JobExecutionPlan jobExecutionPlan : jobExecutionPlans) {
      Dag.DagNode<JobExecutionPlan> dagNode = new Dag.DagNode<>(jobExecutionPlan);
      dagNodeList.add(dagNode);
      String jobName = getJobName(jobExecutionPlan);
      if (jobName != null) {
        JobExecutionPlanMap.put(jobName, dagNode);
      }
    }

    /**
     * Iterate over each {@link JobSpec} to get the dependencies of each {@link JobSpec}.
     * For each {@link JobSpec}, get the corresponding {@link Dag.DagNode} and
     * set the {@link Dag.DagNode}s corresponding to its dependencies as its parent nodes.
     *
     * TODO: we likely do not need 2 for loops and we can do this in 1 pass.
     */
    for (JobExecutionPlan jobExecutionPlan : jobExecutionPlans) {
      String jobName = getJobName(jobExecutionPlan);
      if (jobName == null) {
        continue;
      }
      Dag.DagNode<JobExecutionPlan> node = JobExecutionPlanMap.get(jobName);
      Collection<String> dependencies = getDependencies(jobExecutionPlan.getJobSpec().getConfig());
      for (String dependency : dependencies) {
        Dag.DagNode<JobExecutionPlan> parentNode = JobExecutionPlanMap.get(dependency);
        node.addParentNode(parentNode);
      }
    }
    Dag<JobExecutionPlan> dag = new Dag<>(dagNodeList);
    return dag;
  }

  /**
   * Get job dependencies of a given job from its config.
   * @param config of a job.
   * @return a list of dependencies of the job.
   */
  private static List<String> getDependencies(Config config) {
    return config.hasPath(ConfigurationKeys.JOB_DEPENDENCIES) ? Arrays
        .asList(config.getString(ConfigurationKeys.JOB_DEPENDENCIES).split(",")) : new ArrayList<>();
  }

  /**
   * The job name is derived from the {@link org.apache.gobblin.runtime.api.JobTemplate} URI. It is the
   * simple name of the path component of the URI.
   * @param jobExecutionPlan
   * @return the simple name from the URI path.
   */
  private static String getJobName(JobExecutionPlan jobExecutionPlan) {
    Optional<URI> jobTemplateUri = jobExecutionPlan.getJobSpec().getTemplateURI();
    if (jobTemplateUri.isPresent()) {
      return Files.getNameWithoutExtension(new Path(jobTemplateUri.get()).getName());
    } else {
      return null;
    }
  }
}