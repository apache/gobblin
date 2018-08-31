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

package org.apache.gobblin.service.modules.orchestration;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

@Alpha
@Slf4j
public class FSDagStateStore implements DagStateStore {
  private final String dagCheckpointDir;

  public FSDagStateStore(Config config) {
    this.dagCheckpointDir = config.getString(ConfigurationKeys.DAG_MANAGER_CHECKPOINT_DIR);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeCheckpoint(Dag<JobExecutionPlan> dag) throws IOException {
    // write to a temporary name then rename to make the operation atomic when the file system allows a file to be
    // replaced
    String fileName = generateDagId(dag);
    String serializedDag = serializeDag(dag);

    File tmpCheckpointFile = new File(this.dagCheckpointDir, fileName + ".tmp");
    File checkpointFile = new File(this.dagCheckpointDir, fileName);

    Files.write(serializedDag, tmpCheckpointFile, Charsets.UTF_8);
    Files.move(tmpCheckpointFile, checkpointFile);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void cleanUp(Dag<JobExecutionPlan> dag) {
    String dagId = generateDagId(dag);

    //Delete the dag checkpoint file from the checkpoint directory
    File checkpointFile = new File(this.dagCheckpointDir, dagId);
    if (!checkpointFile.delete()) {
      log.error("Could not delete checkpoint file: {}", checkpointFile.getName());
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Dag<JobExecutionPlan>> getRunningDags() throws IOException {
    List<Dag<JobExecutionPlan>> runningDags = Lists.newArrayList();
    File dagCheckpointFolder = new File(this.dagCheckpointDir);
    for (File file : dagCheckpointFolder.listFiles()) {
        runningDags.add(getDag(file));
    }
    return runningDags;
  }

  /**
   * Return a {@link Dag} given a file name.
   * @param dagFile
   * @return the {@link Dag} associated with the dagFiel.
   */
  private Dag<JobExecutionPlan> getDag(File dagFile) throws IOException {
    String serializedDag = Files.toString(dagFile, Charsets.UTF_8);
    return deserializeDag(serializedDag);
  }

  /**
   * Serialize a {@link Dag<JobExecutionPlan>}.
   * @param dag A Dag parametrized by type {@link JobExecutionPlan}.
   * @return a JSON string representation of the Dag object.
   */
  private String serializeDag(Dag<JobExecutionPlan> dag) {
    Type dagType = new TypeToken<Dag<JobExecutionPlan>>() {}.getType();
    return new Gson().toJson(dag, dagType);
  }

  /**
   * De-serialize a Dag.
   * @param jsonDag A string representation of a Dag.
   * @return a {@link Dag} parametrized by {@link JobExecutionPlan}.
   */
  private Dag<JobExecutionPlan> deserializeDag(String jsonDag) {
    Type dagType = new TypeToken<Dag<JobExecutionPlan>>() {}.getType();
    return new Gson().fromJson(jsonDag, dagType);
  }

  /**
   *
   * {@inheritDoc}
   */
  @Override
  public String generateDagId(Dag<JobExecutionPlan> dag) {
    Config jobConfig = dag.getStartNodes().get(0).getValue().getJobSpec().getConfig();
    String flowGroup = jobConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = jobConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    Long flowExecutionId = jobConfig.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
    return Joiner.on("_").join(flowGroup, flowName, flowExecutionId);
  }
}
