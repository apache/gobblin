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

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.MysqlDagStateStoreFactory;
import org.apache.gobblin.metastore.MysqlStateStore;
import org.apache.gobblin.metastore.MysqlStateStoreEntryManager;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metastore.predicates.StateStorePredicate;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_serde.GsonSerDe;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanListDeserializer;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanListSerializer;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import com.typesafe.config.Config;

import static org.apache.gobblin.service.ServiceConfigKeys.GOBBLIN_SERVICE_PREFIX;
import static org.apache.gobblin.service.modules.orchestration.DagManagerUtils.generateDagId;


/**
 * A implementation of {@link DagStateStore} using MySQL as a backup, leverage {@link MysqlStateStore}.
 * It implements interfaces of {@link DagStateStore} but delegating responsibilities to methods provided
 * in {@link MysqlStateStore}.
 * It also implements conversion between {@link Dag<JobExecutionPlan>} to {@link State}.
 *
 * The schema of this will simply be:
 * | storeName | tableName | State |
 * where storeName represents FlowId, a combination of FlowGroup and FlowName, and tableName represents FlowExecutionId.
 * State is a pocket for serialized {@link Dag} object.
 *
 *
 */
public class MysqlDagStateStore implements DagStateStore {

  public static final String CONFIG_PREFIX = GOBBLIN_SERVICE_PREFIX + "mysqlDagStateStore";
  public static final String DAG_KEY_IN_STATE = "dag";

  /**
   * The schema of {@link MysqlStateStore} is fixed but the columns are semantically projected into Dag's context:
   * - The 'storeName' is FlowId.
   * - The 'tableName' is FlowExecutionId.
   */
  private MysqlStateStore<State> mysqlStateStore;
  private final GsonSerDe<List<JobExecutionPlan>> serDe;
  private JobExecutionPlanDagFactory jobExecPlanDagFactory;

  public MysqlDagStateStore(Config config, Map<URI, TopologySpec> topologySpecMap) {
    if (config.hasPath(CONFIG_PREFIX)) {
      config = config.getConfig(CONFIG_PREFIX).withFallback(config);
    }

    this.mysqlStateStore = (MysqlStateStore<State>) createStateStore(config);

    JsonSerializer<List<JobExecutionPlan>> serializer = new JobExecutionPlanListSerializer();
    JsonDeserializer<List<JobExecutionPlan>> deserializer = new JobExecutionPlanListDeserializer(topologySpecMap);
    Type typeToken = new TypeToken<List<JobExecutionPlan>>() {
    }.getType();
    this.serDe = new GsonSerDe<>(typeToken, serializer, deserializer);
    this.jobExecPlanDagFactory = new JobExecutionPlanDagFactory();
  }

  /**
   * Creating an instance of StateStore.
   */
  protected StateStore<State> createStateStore(Config config) {
    try {
      return (MysqlDagStateStoreFactory.class.newInstance()).createStateStore(config, State.class);
    } catch (ReflectiveOperationException rfoe) {
      throw new RuntimeException("A MySQL StateStore cannot be correctly initialized due to:", rfoe);
    }
  }

  @Override
  public void writeCheckpoint(Dag<JobExecutionPlan> dag)
      throws IOException {
    mysqlStateStore.put(getStoreNameFromDagId(generateDagId(dag).toString()), getTableNameFromDagId(generateDagId(dag).toString()), convertDagIntoState(dag));
  }

  @Override
  public void cleanUp(Dag<JobExecutionPlan> dag)
      throws IOException {
    cleanUp(generateDagId(dag).toString());
  }

  @Override
  public void cleanUp(String dagId)
      throws IOException {
    mysqlStateStore.delete(getStoreNameFromDagId(dagId), getTableNameFromDagId(dagId));
  }

  @Override
  public List<Dag<JobExecutionPlan>> getDags()
      throws IOException {
    return mysqlStateStore.getAll().stream().map(this::convertStateObjIntoDag).collect(Collectors.toList());
  }

  @Override
  public Dag<JobExecutionPlan> getDag(String dagId) throws IOException {
    List<State> states = mysqlStateStore.getAll(getStoreNameFromDagId(dagId), getTableNameFromDagId(dagId));
    if (states.isEmpty()) {
      return null;
    }
    return convertStateObjIntoDag(states.get(0));
  }

  @Override
  public Set<String> getDagIds() throws IOException {
    List<MysqlStateStoreEntryManager> entries = (List<MysqlStateStoreEntryManager>) mysqlStateStore
        .getMetadataForTables(new StateStorePredicate(Predicates.alwaysTrue()));
    return entries.stream().map(entry -> entryToDagId(entry.getStoreName(), entry.getTableName())).collect(Collectors.toSet());
  }

  /**
   * Convert a state store entry into a dag ID
   * e.g. storeName = group1_name1, tableName = 1234 gives dagId group1_name1_1234
   */
  private String entryToDagId(String storeName, String tableName) {
    return Joiner.on(ServiceConfigKeys.DAG_STORE_KEY_SEPARATION_CHARACTER).join(storeName, tableName);
  }

  /**
   * Return a storeName given a dagId. Store name is defined as flowGroup_flowName.
   */
  private String getStoreNameFromDagId(String dagId) {
    return dagId.substring(0, dagId.lastIndexOf(ServiceConfigKeys.DAG_STORE_KEY_SEPARATION_CHARACTER));
  }

  /**
   * Return a tableName given a dagId. Table name is defined as the flowExecutionId.
   */
  private String getTableNameFromDagId(String dagId) {
    return dagId.substring(dagId.lastIndexOf(ServiceConfigKeys.DAG_STORE_KEY_SEPARATION_CHARACTER) + 1);
  }

  /**
   * For {@link Dag} to work with {@link MysqlStateStore}, it needs to be packaged into a {@link State} object.
   * The way that it does is simply serialize the {@link Dag} first and use the key {@link #DAG_KEY_IN_STATE}
   * to be pair with it.
   *
   * The serialization step is required for readability and portability of serde lib.
   * @param dag The dag to be converted.
   * @return An {@link State} object that contains a single k-v pair for {@link Dag}.
   */
  private State convertDagIntoState(Dag<JobExecutionPlan> dag) {
    State outputState = new State();

    // Make sure the object has been serialized.
    List<JobExecutionPlan> jobExecutionPlanList =
        dag.getNodes().stream().map(Dag.DagNode::getValue).collect(Collectors.toList());
    outputState.setProp(DAG_KEY_IN_STATE, serDe.serialize(jobExecutionPlanList));
    return outputState;
  }

  /**
   * Get the {@link Dag} out of a {@link State} pocket.
   */
  private Dag<JobExecutionPlan> convertStateObjIntoDag(State state) {
    String serializedJobExecPlanList = state.getProp(DAG_KEY_IN_STATE);
    return jobExecPlanDagFactory.createDag(serDe.deserialize(serializedJobExecPlanList));
  }
}
