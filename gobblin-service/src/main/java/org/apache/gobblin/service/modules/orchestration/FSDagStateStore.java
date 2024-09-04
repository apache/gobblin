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
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_serde.GsonSerDe;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanListDeserializer;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanListSerializer;
import org.apache.gobblin.util.ConfigUtils;


@Alpha
@Slf4j
public class FSDagStateStore implements DagStateStore {
  public static final String DAG_FILE_EXTENSION = ".dag";
  static final String DAG_STATESTORE_DIR = "dagStateStoreDir";

  private final String dagCheckpointDir;
  private final GsonSerDe<List<JobExecutionPlan>> serDe;

  private final ContextAwareCounter totalDagCount;

  public FSDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap) throws IOException {
    this.dagCheckpointDir = config.getString(DAG_STATESTORE_DIR);
    File checkpointDir = new File(this.dagCheckpointDir);
    if (!checkpointDir.exists()) {
      if (!checkpointDir.mkdirs()) {
        throw new IOException("Could not create dag state store dir - " + this.dagCheckpointDir);
      }
    }

    JsonSerializer<List<JobExecutionPlan>> serializer = new JobExecutionPlanListSerializer();
    JsonDeserializer<List<JobExecutionPlan>> deserializer = new JobExecutionPlanListDeserializer(topologySpecMap);

    /* {@link Type} object will need to strictly match with the generic arguments being used
     * to define {@link GsonSerDe}
     * Due to type erasure, the {@link Type} needs to initialized here instead of inside {@link GsonSerDe}.
     * */
    Type typeToken = new TypeToken<List<JobExecutionPlan>>(){}.getType();
    this.serDe = new GsonSerDe<>(typeToken, serializer, deserializer);
    MetricContext metricContext = Instrumented.getMetricContext(
        new org.apache.gobblin.configuration.State(ConfigUtils.configToProperties(config)), this.getClass());
    this.totalDagCount = metricContext.contextAwareCounter(ServiceMetricNames.DAG_COUNT_FS_DAG_STATE_COUNT);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void writeCheckpoint(Dag<JobExecutionPlan> dag) throws IOException {
    // write to a temporary name then rename to make the operation atomic when the file system allows a file to be
    // replaced
    String fileName = DagUtils.generateDagId(dag) + DAG_FILE_EXTENSION;
    String serializedDag = serializeDag(dag);

    File tmpCheckpointFile = new File(this.dagCheckpointDir, fileName + ".tmp");
    File checkpointFile = new File(this.dagCheckpointDir, fileName);

    Files.write(serializedDag, tmpCheckpointFile, Charsets.UTF_8);
    Files.move(tmpCheckpointFile, checkpointFile);
    this.totalDagCount.inc();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void cleanUp(Dag<JobExecutionPlan> dag) {
    cleanUp(DagUtils.generateDagId(dag).toString());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void cleanUp(String dagId) {
    String fileName = dagId + DAG_FILE_EXTENSION;

    //Delete the dag checkpoint file from the checkpoint directory
    File checkpointFile = new File(this.dagCheckpointDir, fileName);
    if (!checkpointFile.delete()) {
      log.error("Could not delete checkpoint file: {}", checkpointFile.getName());
    } else {

      this.totalDagCount.dec();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Dag<JobExecutionPlan>> getDags() throws IOException {
    List<Dag<JobExecutionPlan>> runningDags = Lists.newArrayList();
    File dagCheckpointFolder = new File(this.dagCheckpointDir);

    for (File file : dagCheckpointFolder.listFiles((dir, name) -> name.endsWith(DAG_FILE_EXTENSION))) {
      runningDags.add(getDag(file));
    }
    return runningDags;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Dag<JobExecutionPlan> getDag(String dagId) throws IOException {
    File file = new File(this.dagCheckpointDir, dagId + DAG_FILE_EXTENSION);
    if (!file.exists()) {
      return null;
    }
    return getDag(file);
  }

  /**
   * Return a {@link Dag} given a file name.
   * @param dagFile
   * @return the {@link Dag} associated with the dagFile.
   */
  @VisibleForTesting
  public Dag<JobExecutionPlan> getDag(File dagFile) throws IOException {
    String serializedDag = Files.toString(dagFile, Charsets.UTF_8);
    return deserializeDag(serializedDag);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<String> getDagIds() {
    Set<String> dagIds = new HashSet<>();
    File dagCheckpointFolder = new File(this.dagCheckpointDir);

    for (File file : dagCheckpointFolder.listFiles((dir, name) -> name.endsWith(DAG_FILE_EXTENSION))) {
      dagIds.add(StringUtils.removeEnd(file.getName(), DAG_FILE_EXTENSION));
    }
    return dagIds;
  }

  /**
   * Serialize a {@link Dag<JobExecutionPlan>}.
   * @param dag A Dag parametrized by type {@link JobExecutionPlan}.
   * @return a JSON string representation of the Dag object.
   */
  private String serializeDag(Dag<JobExecutionPlan> dag) {
    List<JobExecutionPlan> jobExecutionPlanList = dag.getNodes().stream().map(Dag.DagNode::getValue).collect(Collectors.toList());
    return serDe.serialize(jobExecutionPlanList);
  }

  /**
   * De-serialize a Dag.
   * @param jsonDag A string representation of a Dag.
   * @return a {@link Dag} parametrized by {@link JobExecutionPlan}.
   */
  private Dag<JobExecutionPlan> deserializeDag(String jsonDag) {
    return new JobExecutionPlanDagFactory().createDag(serDe.deserialize(jsonDag));
  }
}
