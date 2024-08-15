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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.jgit.errors.NotSupportedException;

import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import com.typesafe.config.Config;

import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metastore.MysqlDataSourceFactory;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_serde.GsonSerDe;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanDagFactory;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanListDeserializer;
import org.apache.gobblin.service.modules.spec.JobExecutionPlanListSerializer;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.DBStatementExecutor;

import static org.apache.gobblin.service.modules.orchestration.DagUtils.generateDagId;


/**
 * An implementation of {@link DagStateStoreWithDagNodes} using MySQL as a backup.
 */
@Slf4j
public class MysqlDagStateStoreWithDagNodes implements DagStateStoreWithDagNodes {

  public static final String CONFIG_PREFIX = MysqlDagStateStore.CONFIG_PREFIX;
  protected final DBStatementExecutor dbStatementExecutor;
  protected final String tableName;
  protected final GsonSerDe<List<JobExecutionPlan>> serDe;
  private final JobExecutionPlanDagFactory jobExecPlanDagFactory;

  // todo add a column that tells if it is a running dag or a failed dag
  protected static final String CREATE_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s ("
      + "dag_node_id VARCHAR(" + ServiceConfigKeys.MAX_DAG_NODE_ID_LENGTH + ") CHARACTER SET latin1 COLLATE latin1_bin NOT NULL, "
      + "parent_dag_id VARCHAR(" + ServiceConfigKeys.MAX_DAG_ID_LENGTH + ") NOT NULL, "
      + "dag_node JSON NOT NULL, "
      + "modified_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
      + "PRIMARY KEY (dag_node_id), "
      + "UNIQUE INDEX dag_node_index (dag_node_id), "
      + "INDEX dag_index (parent_dag_id))";

  protected static final String INSERT_DAG_NODE_STATEMENT = "INSERT INTO %s (dag_node_id, parent_dag_id, dag_node) VALUES (?, ?, ?)";
  protected static final String UPDATE_DAG_NODE_STATEMENT = "UPDATE %s SET dag_node = ? WHERE dag_node_id = ?";
  protected static final String GET_DAG_NODES_STATEMENT = "SELECT dag_node FROM %s WHERE parent_dag_id = ?";
  protected static final String GET_DAG_NODE_STATEMENT = "SELECT dag_node FROM %s WHERE dag_node_id = ?";
  protected static final String DELETE_DAG_STATEMENT = "DELETE FROM %s WHERE parent_dag_id = ?";

  public MysqlDagStateStoreWithDagNodes(Config config, Map<URI, TopologySpec> topologySpecMap) throws IOException {
    if (config.hasPath(CONFIG_PREFIX)) {
      config = config.getConfig(CONFIG_PREFIX).withFallback(config);
    }

    String DEFAULT_TABLE_NAME = "dag_node_state_store";
    this.tableName = ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, DEFAULT_TABLE_NAME);
    // create table if it does not exist
    DataSource dataSource = MysqlDataSourceFactory.get(config, SharedResourcesBrokerFactory.getImplicitBroker());

    try (Connection connection = dataSource.getConnection();
        PreparedStatement createStatement = connection.prepareStatement(String.format(CREATE_TABLE_STATEMENT, tableName))) {
      createStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("Failure creation table " + tableName, e);
    }
    this.dbStatementExecutor = new DBStatementExecutor(dataSource, log);

    JsonSerializer<List<JobExecutionPlan>> serializer = new JobExecutionPlanListSerializer();
    JsonDeserializer<List<JobExecutionPlan>> deserializer = new JobExecutionPlanListDeserializer(topologySpecMap);
    Type typeToken = new TypeToken<List<JobExecutionPlan>>() {
    }.getType();
    this.serDe = new GsonSerDe<>(typeToken, serializer, deserializer);
    this.jobExecPlanDagFactory = new JobExecutionPlanDagFactory();
    MetricContext metricContext =
        Instrumented.getMetricContext(new State(ConfigUtils.configToProperties(config)), this.getClass());
    log.info("Instantiated {}", getClass().getSimpleName());
  }

  @Override
  public void writeCheckpoint(Dag<JobExecutionPlan> dag) throws IOException {
    String dagId = DagUtils.generateDagId(dag).toString();
    dbStatementExecutor.withPreparedStatement(String.format(INSERT_DAG_NODE_STATEMENT, tableName), insertStatement -> {

      for (Dag.DagNode<JobExecutionPlan> dagNode : dag.getNodes()) {
        insertStatement.setObject(1, dagNode.getValue().getId().toString());
        insertStatement.setObject(2, dagId);
        insertStatement.setObject(3, this.serDe.serialize(Collections.singletonList(dagNode.getValue())));
        insertStatement.addBatch();
      }

      try {
        return insertStatement.executeBatch();
      } catch (SQLException e) {
        throw new IOException(String.format("Failure adding dag for %s", dagId), e);
      }}, true);
  }

  @Override
  public void cleanUp(Dag<JobExecutionPlan> dag) throws IOException {
    cleanUp(generateDagId(dag));
  }

  @Override
  public boolean cleanUp(Dag.DagId dagId) throws IOException {
    return dbStatementExecutor.withPreparedStatement(String.format(DELETE_DAG_STATEMENT, tableName),
        deleteStatement -> {
          try {
            deleteStatement.setString(1, dagId.toString());
            return deleteStatement.executeUpdate() != 0;
          } catch (SQLException e) {
            throw new IOException(String.format("Failure deleting dag for %s", dagId), e);
          }
        }, true);
  }

  @Override
  public void cleanUp(String dagId) throws IOException {
    throw new NotSupportedException(getClass().getSimpleName() + " does not need this legacy API that originated with "
        + "the DagManager that is replaced by DagProcessingEngine");
  }

  @Override
  public List<Dag<JobExecutionPlan>> getDags() throws IOException {
    throw new NotSupportedException(getClass().getSimpleName() + " does not need this legacy API that originated with "
        + "the DagManager that is replaced by DagProcessingEngine");  }

  @Override
  public Dag<JobExecutionPlan> getDag(Dag.DagId dagId) throws IOException {
    return convertDagNodesIntoDag(getDagNodes(dagId));
  }

  @Override
  public Dag<JobExecutionPlan> getDag(String dagId) throws IOException {
    throw new NotSupportedException(getClass().getSimpleName() + " does not need this API");
  }

  @Override
  public Set<String> getDagIds() throws IOException {
    throw new NotSupportedException(getClass().getSimpleName() + " does not need this API");
  }

  /**
   * Get the {@link Dag} out of a {@link State} pocket.
   */
  private Dag<JobExecutionPlan> convertDagNodesIntoDag(Set<Dag.DagNode<JobExecutionPlan>> dagNodes) {
    if (dagNodes.isEmpty()) {
      return null;
    }
    return jobExecPlanDagFactory.createDag(dagNodes.stream().map(Dag.DagNode::getValue).collect(Collectors.toList()));
  }

  @Override
  public boolean updateDagNode(Dag.DagNode<JobExecutionPlan> dagNode) throws IOException {
    String dagNodeId = dagNode.getValue().getId().toString();
    return dbStatementExecutor.withPreparedStatement(String.format(UPDATE_DAG_NODE_STATEMENT, tableName), updateStatement -> {
      try {
        updateStatement.setString(1, this.serDe.serialize(Collections.singletonList(dagNode.getValue())));
        updateStatement.setString(2, dagNodeId);
        return updateStatement.executeUpdate() == 1;
      } catch (SQLException e) {
        throw new IOException(String.format("Failure updating dag node for %s", dagNodeId), e);
      }}, true);
  }

  @Override
  public Set<Dag.DagNode<JobExecutionPlan>> getDagNodes(Dag.DagId parentDagId) throws IOException {
    return dbStatementExecutor.withPreparedStatement(String.format(GET_DAG_NODES_STATEMENT, tableName), getStatement -> {
      getStatement.setString(1, parentDagId.toString());
      HashSet<Dag.DagNode<JobExecutionPlan>> dagNodes = new HashSet<>();
      try (ResultSet rs = getStatement.executeQuery()) {
        while (rs.next()) {
          dagNodes.add(new Dag.DagNode<>(this.serDe.deserialize(rs.getString(1)).get(0)));
        }
        return dagNodes;
      } catch (SQLException e) {
        throw new IOException(String.format("Failure get dag nodes for dag %s", parentDagId), e);
      }
    }, true);
  }

  @Override
  public Optional<Dag.DagNode<JobExecutionPlan>> getDagNode(DagNodeId dagNodeId) throws IOException {
    return dbStatementExecutor.withPreparedStatement(String.format(GET_DAG_NODE_STATEMENT, tableName), getStatement -> {
      getStatement.setString(1, dagNodeId.toString());
      try (ResultSet rs = getStatement.executeQuery()) {
        if (rs.next()) {
          return Optional.of(new Dag.DagNode<>(this.serDe.deserialize(rs.getString(1)).get(0)));
        }
        return Optional.empty();
      } catch (SQLException e) {
        throw new IOException(String.format("Failure get dag node for %s", dagNodeId), e);
      }
    }, true);
  }
}
