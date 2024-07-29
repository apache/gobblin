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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.typesafe.config.Config;

import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlDataSourceFactory;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.DBStatementExecutor;


@Slf4j
public class MysqlDagActionStore implements DagActionStore {

  public static final String CONFIG_PREFIX = "MysqlDagActionStore";

  protected final DataSource dataSource;
  private final DBStatementExecutor dbStatementExecutor;
  private final String tableName;
  private static final String EXISTS_STATEMENT = "SELECT EXISTS(SELECT * FROM %s WHERE flow_group = ? AND flow_name = ? AND flow_execution_id = ? AND job_name = ? AND dag_action = ?)";

  protected static final String INSERT_STATEMENT = "INSERT INTO %s (flow_group, flow_name, flow_execution_id, job_name, dag_action) "
      + "VALUES (?, ?, ?, ?, ?)";
  private static final String DELETE_STATEMENT = "DELETE FROM %s WHERE flow_group = ? AND flow_name =? AND flow_execution_id = ? AND job_name = ? AND dag_action = ?";
  private static final String GET_ALL_STATEMENT = "SELECT flow_group, flow_name, flow_execution_id, job_name, dag_action FROM %s";
  private static final String CREATE_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s (" +
  "flow_group varchar(" + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, flow_name varchar(" + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, "
      + "flow_execution_id varchar(" + ServiceConfigKeys.MAX_FLOW_EXECUTION_ID_LENGTH + ") NOT NULL, "
      + "job_name varchar(" + ServiceConfigKeys.MAX_JOB_NAME_LENGTH + ") NOT NULL, "
      + "dag_action varchar(50) NOT NULL, modified_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP  on update CURRENT_TIMESTAMP NOT NULL, "
      + "PRIMARY KEY (flow_group,flow_name,flow_execution_id,job_name,dag_action))";

  // Deletes rows older than retention time period (in seconds) to prevent this table from growing unbounded.
  private static final String RETENTION_STATEMENT = "DELETE FROM %s WHERE modified_time < DATE_SUB(CURRENT_TIMESTAMP, INTERVAL %s SECOND)";
  private final DagProcessingEngineMetrics dagProcessingEngineMetrics;

  @Inject
  public MysqlDagActionStore(Config config, DagProcessingEngineMetrics dagProcessingEngineMetrics) throws IOException {
    if (config.hasPath(CONFIG_PREFIX)) {
      config = config.getConfig(CONFIG_PREFIX).withFallback(config);
    } else {
      throw new IOException("Please specify the config for MysqlDagActionStore");
    }
    this.tableName = ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_DB_TABLE_KEY,
        ConfigurationKeys.DEFAULT_STATE_STORE_DB_TABLE);
    long retentionPeriodSeconds =
        ConfigUtils.getLong(config, ConfigurationKeys.MYSQL_DAG_ACTION_STORE_TABLE_RETENTION_PERIOD_SECONDS_KEY,
            ConfigurationKeys.DEFAULT_MYSQL_DAG_ACTION_STORE_TABLE_RETENTION_PERIOD_SEC_KEY);
    this.dataSource = MysqlDataSourceFactory.get(config,
        SharedResourcesBrokerFactory.getImplicitBroker());
    try (Connection connection = dataSource.getConnection();
        PreparedStatement createStatement = connection.prepareStatement(String.format(CREATE_TABLE_STATEMENT, tableName))) {
      createStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("Failure creation table " + tableName, e);
    }
    this.dbStatementExecutor = new DBStatementExecutor(this.dataSource, log);
    String thisTableRetentionStatement = String.format(RETENTION_STATEMENT, this.tableName, retentionPeriodSeconds);
    // Periodically deletes all rows in the table last_modified before the retention period defined by config.
    dbStatementExecutor.repeatSqlCommandExecutionAtInterval(thisTableRetentionStatement, 6L, TimeUnit.HOURS);
    this.dagProcessingEngineMetrics = dagProcessingEngineMetrics;
  }

  @Override
  public boolean exists(String flowGroup, String flowName, long flowExecutionId, String jobName, DagActionType dagActionType) throws IOException {
    return dbStatementExecutor.withPreparedStatement(String.format(EXISTS_STATEMENT, tableName), existStatement -> {
      fillPreparedStatement(flowGroup, flowName, flowExecutionId, jobName, dagActionType, existStatement);
      try (ResultSet rs = existStatement.executeQuery()) {
        rs.next();
        return rs.getBoolean(1);
      } catch (SQLException e) {
        throw new IOException(String.format("Failure checking existence of DagAction: %s in table %s",
            new DagAction(flowGroup, flowName, flowExecutionId, jobName, dagActionType), tableName), e);
      }
    }, true);
  }

  @Override
  public boolean exists(String flowGroup, String flowName, long flowExecutionId, DagActionType dagActionType) throws IOException, SQLException {
    return exists(flowGroup, flowName, flowExecutionId, NO_JOB_NAME_DEFAULT, dagActionType);
  }

  @Override
  public void addJobDagAction(String flowGroup, String flowName, long flowExecutionId, String jobName, DagActionType dagActionType)
      throws IOException {
    dbStatementExecutor.withPreparedStatement(String.format(INSERT_STATEMENT, tableName), insertStatement -> {
    try {
      fillPreparedStatement(flowGroup, flowName, flowExecutionId, jobName, dagActionType, insertStatement);
      return insertStatement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(String.format("Failure adding action for DagAction: %s in table %s",
          new DagAction(flowGroup, flowName, flowExecutionId, jobName, dagActionType), tableName), e);
    }}, true);
    this.dagProcessingEngineMetrics.markDagActionsStored(dagActionType);
  }

  @Override
  public boolean deleteDagAction(DagAction dagAction) throws IOException {
    return dbStatementExecutor.withPreparedStatement(String.format(DELETE_STATEMENT, tableName), deleteStatement -> {
    try {
      fillPreparedStatement(dagAction.getFlowGroup(), dagAction.getFlowName(), dagAction.getFlowExecutionId(),
          dagAction.getJobName(), dagAction.getDagActionType(), deleteStatement);
      this.dagProcessingEngineMetrics.markDagActionsDeleted(dagAction.getDagActionType(), true);
      return deleteStatement.executeUpdate() != 0;
    } catch (SQLException e) {
      this.dagProcessingEngineMetrics.markDagActionsDeleted(dagAction.getDagActionType(), false);
      throw new IOException(String.format("Failure deleting action for DagAction: %s in table %s", dagAction,
          tableName), e);
    }}, true);
  }

  @Override
  public Collection<DagAction> getDagActions() throws IOException {
    return dbStatementExecutor.withPreparedStatement(String.format(GET_ALL_STATEMENT, tableName), getAllStatement -> {
      HashSet<DagAction> result = new HashSet<>();
      try (ResultSet rs = getAllStatement.executeQuery()) {
        while (rs.next()) {
          result.add(new DagAction(rs.getString(1), rs.getString(2), rs.getLong(3), rs.getString(4), DagActionType.valueOf(rs.getString(5))));
        }
        return result;
      } catch (SQLException e) {
        throw new IOException(String.format("Failure get dag actions from table %s ", tableName), e);
      }
    }, true);
  }

  private static void fillPreparedStatement(String flowGroup, String flowName, long flowExecutionId, String jobName,
      DagActionType dagActionType, PreparedStatement statement)
      throws SQLException {
    int i = 0;
    statement.setString(++i, flowGroup);
    statement.setString(++i, flowName);
    statement.setString(++i, String.valueOf(flowExecutionId));
    statement.setString(++i, jobName);
    statement.setString(++i, dagActionType.toString());
  }
}
