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

import com.google.common.io.Files;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.service.modules.flowgraph.Dag;


/**
 * A Factory class used for constructing a {@link Dag} of {@link JobSpec}s from
 * a {@link List} of {@link JobSpec}s.
 */
@Alpha
@Slf4j
public class JobSpecDagFactory {

  public static Dag<JobSpecWithExecutor> createDagFromJobSpecs(List<JobSpecWithExecutor> jobSpecs) {
    //Maintain a mapping between jobspec name and the
    Map<String, Dag.DagNode<JobSpecWithExecutor>> jobSpecMap = new HashMap<>();
    List<Dag.DagNode<JobSpecWithExecutor>> dagNodeList = new ArrayList<>();
    /**
     * Create a {@link Dag.DagNode<JobSpecWithExecutor>} for every {@link JobSpec} in the flow. Add this node
     * to a HashMap.
     */
    for (JobSpecWithExecutor jobSpecWithExecutor : jobSpecs) {
      Dag.DagNode<JobSpecWithExecutor> dagNode = new Dag.DagNode<>(jobSpecWithExecutor);
      dagNodeList.add(dagNode);
      jobSpecMap.put(getJobFileNameFromJobSpec(jobSpecWithExecutor), dagNode);
    }

    /**
     * Iterate over each {@link JobSpec} to get the dependencies of each {@link JobSpec}.
     * For each {@link JobSpec}, get the corresponding {@link Dag.DagNode} and
     * set the {@link Dag.DagNode}s corresponding to its dependencies as its parent nodes.
     *
     * TODO: we likely do not need 2 for loops and we can do this in 1 pass.
     */
    for (JobSpecWithExecutor jobSpecWithExecutor : jobSpecs) {
      Dag.DagNode<JobSpecWithExecutor> node = jobSpecMap.get(getJobFileNameFromJobSpec(jobSpecWithExecutor));
      Collection<String> dependencies = getDependencies(jobSpecWithExecutor.getJobSpec().getConfig());
      for (String dependency : dependencies) {
        Dag.DagNode<JobSpecWithExecutor> parentNode = jobSpecMap.get(dependency);
        node.addParentNode(parentNode);
      }
    }
    Dag<JobSpecWithExecutor> dag = new Dag<>(dagNodeList);
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
   * Extract the filename (without extensions) of job properties file from the {@link JobSpec}. The properties filename
   * is derived from the {@link org.apache.gobblin.runtime.api.JobTemplate} URI.
   * @param jobSpec
   * @return the file name without extension of the properties file from which the JobSpec is derived.
   */
  private static String getJobFileNameFromJobSpec(JobSpecWithExecutor jobSpec) {
    URI jobTemplateUri = jobSpec.getJobSpec().getTemplateURI().get();
    return Files.getNameWithoutExtension(new Path(jobTemplateUri).getName());
  }
}