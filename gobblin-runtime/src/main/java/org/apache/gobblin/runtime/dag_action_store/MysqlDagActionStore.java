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

package org.apache.gobblin.runtime.dag_action_store;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;

import com.google.inject.Inject;
import com.typesafe.config.Config;

import javax.sql.DataSource;

import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlDataSourceFactory;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExponentialBackoff;

@Slf4j
public class MysqlDagActionStore implements DagActionStore {

  public static final String CONFIG_PREFIX = "MysqlDagActionStore";

  protected final DataSource dataSource;
  private final String tableName;
  private static final String EXISTS_STATEMENT = "SELECT EXISTS(SELECT * FROM %s WHERE flow_group = ? AND flow_name =? AND flow_execution_id = ? AND dag_action = ?)";

  protected static final String INSERT_STATEMENT = "INSERT INTO %s (flow_group, flow_name, flow_execution_id, dag_action) "
      + "VALUES (?, ?, ?, ?)";
  private static final String DELETE_STATEMENT = "DELETE FROM %s WHERE flow_group = ? AND flow_name =? AND flow_execution_id = ? AND dag_action = ?";
  private static final String GET_STATEMENT = "SELECT flow_group, flow_name, flow_execution_id, dag_action FROM %s WHERE flow_group = ? AND flow_name =? AND flow_execution_id = ? AND dag_action = ?";
  private static final String GET_ALL_STATEMENT = "SELECT flow_group, flow_name, flow_execution_id, dag_action FROM %s";
  private static final String CREATE_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %s (" +
  "flow_group varchar(" + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, flow_name varchar(" + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, "
      + "flow_execution_id varchar(" + ServiceConfigKeys.MAX_FLOW_EXECUTION_ID_LENGTH + ") NOT NULL, "
      + "dag_action varchar(100) NOT NULL, modified_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP  on update CURRENT_TIMESTAMP NOT NULL, "
      + "PRIMARY KEY (flow_group,flow_name,flow_execution_id, dag_action))";

  private final int getDagActionMaxRetries;

  @Inject
  public MysqlDagActionStore(Config config) throws IOException {
    if (config.hasPath(CONFIG_PREFIX)) {
      config = config.getConfig(CONFIG_PREFIX).withFallback(config);
    } else {
      throw new IOException("Please specify the config for MysqlDagActionStore");
    }
    this.tableName = ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_DB_TABLE_KEY,
        ConfigurationKeys.DEFAULT_STATE_STORE_DB_TABLE);
    this.getDagActionMaxRetries = ConfigUtils.getInt(config, ConfigurationKeys.MYSQL_GET_MAX_RETRIES, ConfigurationKeys.DEFAULT_MYSQL_GET_MAX_RETRIES);

    this.dataSource = MysqlDataSourceFactory.get(config,
        SharedResourcesBrokerFactory.getImplicitBroker());
    try (Connection connection = dataSource.getConnection();
        PreparedStatement createStatement = connection.prepareStatement(String.format(CREATE_TABLE_STATEMENT, tableName))) {
      createStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("Failure creation table " + tableName, e);
    }
  }

  @Override
  public boolean exists(String flowGroup, String flowName, String flowExecutionId, FlowActionType flowActionType) throws IOException, SQLException {
    ResultSet rs = null;
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement existStatement = connection.prepareStatement(String.format(EXISTS_STATEMENT, tableName))) {
      int i = 0;
      existStatement.setString(++i, flowGroup);
      existStatement.setString(++i, flowName);
      existStatement.setString(++i, flowExecutionId);
      existStatement.setString(++i, flowActionType.toString());
      rs = existStatement.executeQuery();
      rs.next();
      return rs.getBoolean(1);
    } catch (SQLException e) {
      throw new IOException(String.format("Failure checking existence of DagAction: %s in table %s",
          new DagAction(flowGroup, flowName, flowExecutionId, flowActionType), tableName), e);
    } finally {
      if (rs != null) {
        rs.close();
      }
    }
  }

  @Override
  public void addDagAction(String flowGroup, String flowName, String flowExecutionId, FlowActionType flowActionType)
      throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement insertStatement = connection.prepareStatement(String.format(INSERT_STATEMENT, tableName))) {
      int i = 0;
      insertStatement.setString(++i, flowGroup);
      insertStatement.setString(++i, flowName);
      insertStatement.setString(++i, flowExecutionId);
      insertStatement.setString(++i, flowActionType.toString());
      insertStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException(String.format("Failure adding action for DagAction: %s in table %s",
          new DagAction(flowGroup, flowName, flowExecutionId, flowActionType), tableName), e);
    }
  }

  @Override
  public boolean deleteDagAction(DagAction dagAction) throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement deleteStatement = connection.prepareStatement(String.format(DELETE_STATEMENT, tableName))) {
      int i = 0;
      deleteStatement.setString(++i, dagAction.getFlowGroup());
      deleteStatement.setString(++i, dagAction.getFlowName());
      deleteStatement.setString(++i, dagAction.getFlowExecutionId());
      deleteStatement.setString(++i, dagAction.getFlowActionType().toString());
      int result = deleteStatement.executeUpdate();
      connection.commit();
      return result != 0;
    } catch (SQLException e) {
      throw new IOException(String.format("Failure deleting action for DagAction: %s in table %s", dagAction,
          tableName), e);
    }
  }

  // TODO: later change this to getDagActions relating to a particular flow execution if it makes sense
  private DagAction getDagActionWithRetry(String flowGroup, String flowName, String flowExecutionId, FlowActionType flowActionType, ExponentialBackoff exponentialBackoff)
      throws IOException, SQLException {
    ResultSet rs = null;
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement getStatement = connection.prepareStatement(String.format(GET_STATEMENT, tableName))) {
      int i = 0;
      getStatement.setString(++i, flowGroup);
      getStatement.setString(++i, flowName);
      getStatement.setString(++i, flowExecutionId);
      getStatement.setString(++i, flowActionType.toString());
      rs = getStatement.executeQuery();
      if (rs.next()) {
        return new DagAction(rs.getString(1), rs.getString(2), rs.getString(3), FlowActionType.valueOf(rs.getString(4)));
      } else {
        if (exponentialBackoff.awaitNextRetryIfAvailable()) {
          return getDagActionWithRetry(flowGroup, flowName, flowExecutionId, flowActionType, exponentialBackoff);
        } else {
          log.warn(String.format("Can not find dag action: %s with flowGroup: %s, flowName: %s, flowExecutionId: %s",
              flowActionType, flowGroup, flowName, flowExecutionId));
          return null;
        }
      }
    } catch (SQLException | InterruptedException e) {
      throw new IOException(String.format("Failure get %s from table %s", new DagAction(flowGroup, flowName, flowExecutionId,
          flowActionType), tableName), e);
    } finally {
      if (rs != null) {
        rs.close();
      }
    }

  }

  @Override
  public Collection<DagAction> getDagActions() throws IOException {
    HashSet<DagAction> result = new HashSet<>();
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement getAllStatement = connection.prepareStatement(String.format(GET_ALL_STATEMENT, tableName));
        ResultSet rs = getAllStatement.executeQuery()) {
      while (rs.next()) {
        result.add(
            new DagAction(rs.getString(1), rs.getString(2), rs.getString(3), FlowActionType.valueOf(rs.getString(4))));
      }
      if (rs != null) {
        rs.close();
      }
      return result;
    } catch (SQLException e) {
      throw new IOException(String.format("Failure get dag actions from table %s ", tableName), e);
    }
  }
}
