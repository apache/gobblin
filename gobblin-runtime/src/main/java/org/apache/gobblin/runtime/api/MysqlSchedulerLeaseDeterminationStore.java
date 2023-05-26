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

package org.apache.gobblin.runtime.api;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.google.inject.Inject;
import com.typesafe.config.Config;

import javax.sql.DataSource;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlDataSourceFactory;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


public class MysqlSchedulerLeaseDeterminationStore implements SchedulerLeaseDeterminationStore {
  public static final String CONFIG_PREFIX = "MysqlSchedulerLeaseDeterminationStore";

  protected final DataSource dataSource;
  private final DagActionStore dagActionStore;
  private final String tableName;
  private final long epsilon;
  private final long linger;
  /* TODO:
     - define retention on this table
     - initialize table with epsilon and linger if one already doesn't exist using these configs
     - join with table above to ensure epsilon/linger values are consistent across hosts (in case hosts are deployed with different configs)
   */
  protected static final String WHERE_CLAUSE_TO_MATCH_ROW = "WHERE flow_group=? AND flow_name=? AND flow_execution_id=? "
      + "AND flow_action=? AND ABS(trigger_event_timestamp-?) <= %s";
  protected static final String ATTEMPT_INSERT_AND_GET_PURSUANT_TIMESTAMP_STATEMENT = "INSERT INTO %s (flow_group, "
      + "flow_name, flow_execution_id, flow_action, trigger_event_timestamp) VALUES (?, ?, ?, ?, ?) WHERE NOT EXISTS ("
      + "SELECT * FROM %s " + WHERE_CLAUSE_TO_MATCH_ROW + "; SELECT ROW_COUNT() AS rows_inserted_count, "
      + "pursuant_timestamp FROM %s " + WHERE_CLAUSE_TO_MATCH_ROW;

  protected static final String UPDATE_PURSUANT_TIMESTAMP_STATEMENT = "UPDATE %s SET pursuant_timestamp = NULL "
      + WHERE_CLAUSE_TO_MATCH_ROW;
  private static final String CREATE_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %S (" + "flow_group varchar("
      + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, flow_name varchar("
      + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, " + "flow_execution_id varchar("
      + ServiceConfigKeys.MAX_FLOW_EXECUTION_ID_LENGTH + ") NOT NULL, flow_action varchar(100) NOT NULL, "
      + "trigger_event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
      + "pursuant_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
      + "PRIMARY KEY (flow_group,flow_name,flow_execution_id,flow_action,trigger_event_timestamp)";

  @Inject
  public MysqlSchedulerLeaseDeterminationStore(Config config, DagActionStore dagActionStore) throws IOException {
    if (config.hasPath(CONFIG_PREFIX)) {
      config = config.getConfig(CONFIG_PREFIX).withFallback(config);
    } else {
      throw new IOException("Please specify the config for MysqlSchedulerLeaseDeterminationStore");
    }

    this.tableName = ConfigUtils.getString(config, ConfigurationKeys.SCHEDULER_LEASE_DETERMINATION_STORE_DB_TABLE_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_LEASE_DETERMINATION_STORE_DB_TABLE);
    this.epsilon = ConfigUtils.getLong(config, ConfigurationKeys.SCHEDULER_TRIGGER_EVENT_EPSILON_MILLIS_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_TRIGGER_EVENT_EPSILON_MILLIS);
    this.linger = ConfigUtils.getLong(config, ConfigurationKeys.SCHEDULER_TRIGGER_EVENT_EPSILON_MILLIS_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_TRIGGER_EVENT_EPSILON_MILLIS);

    this.dataSource = MysqlDataSourceFactory.get(config, SharedResourcesBrokerFactory.getImplicitBroker());
    try (Connection connection = dataSource.getConnection();
        PreparedStatement createStatement = connection.prepareStatement(String.format(CREATE_TABLE_STATEMENT, tableName))) {
      createStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("Table creation failure for " + tableName, e);
    }
    this.dagActionStore = dagActionStore;
  }

  @Override
  public LeaseAttemptStatus attemptInsertAndGetPursuantTimestamp(String flowGroup, String flowName,
      String flowExecutionId, FlowActionType flowActionType, long triggerTimeMillis)
      throws IOException {
    Timestamp triggerTimestamp = new Timestamp(triggerTimeMillis);
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement insertStatement = connection.prepareStatement(
            String.format(ATTEMPT_INSERT_AND_GET_PURSUANT_TIMESTAMP_STATEMENT, tableName, tableName, epsilon, tableName,
                epsilon))) {
      int i = 0;
      // Values to set in new row
      insertStatement.setString(++i, flowGroup);
      insertStatement.setString(++i, flowName);
      insertStatement.setString(++i, flowExecutionId);
      insertStatement.setString(++i, flowActionType.toString());
      insertStatement.setTimestamp(++i, triggerTimestamp);
      // Values to check if existing row matches
      insertStatement.setString(++i, flowGroup);
      insertStatement.setString(++i, flowName);
      insertStatement.setString(++i, flowExecutionId);
      insertStatement.setString(++i, flowActionType.toString());
      insertStatement.setTimestamp(++i, triggerTimestamp);
      // Values to make select statement to read row
      insertStatement.setString(++i, flowGroup);
      insertStatement.setString(++i, flowName);
      insertStatement.setString(++i, flowExecutionId);
      insertStatement.setString(++i, flowActionType.toString());
      insertStatement.setTimestamp(++i, triggerTimestamp);
      ResultSet resultSet = insertStatement.executeQuery();
      connection.commit();

      if (!resultSet.next()) {
        resultSet.close();
        throw new IOException(String.format("Unexpected error where no result returned while trying to obtain lease. "
                + "This error indicates that no entry existed for trigger flow event for table %s flow group: %s, flow "
                + "name: %s flow execution id: %s and trigger timestamp: %s when one should have been inserted",
            tableName, flowGroup, flowName, flowExecutionId, triggerTimestamp));
      }
      // If a row was inserted, then we have obtained the lease
      int rowsUpdated = resultSet.getInt(1);
      if (rowsUpdated == 1) {
        // If the pursuing flow launch has been persisted to the {@link DagActionStore} we have completed lease obtainment
        this.dagActionStore.addDagAction(flowGroup, flowName, flowExecutionId, DagActionStore.DagActionValue.LAUNCH);
        if (this.dagActionStore.exists(flowGroup, flowName, flowExecutionId, DagActionStore.DagActionValue.LAUNCH)) {
          if (updatePursuantTimestamp(flowGroup, flowName, flowExecutionId, flowActionType, triggerTimestamp)) {
            // TODO: potentially add metric here to count number of flows scheduled by each scheduler
            LOG.info("Host completed obtaining lease for flow group: %s, flow name: %s flow execution id: %s and "
                + "trigger timestamp: %s", flowGroup, flowName, flowExecutionId, triggerTimestamp);
            resultSet.close();
            return LeaseAttemptStatus.LEASE_OBTAINED;
          } else {
            LOG.warn("Unable to update pursuant timestamp after persisting flow launch to DagActionStore for flow "
                + "group: %s, flow name: %s flow execution id: %s and trigger timestamp: %s.", flowGroup, flowName,
                flowExecutionId, triggerTimestamp);
          }
        } else {
          LOG.warn("Did not find flow launch action in DagActionStore after adding it for flow group: %s, flow name: "
                  + "%s flow execution id: %s and trigger timestamp: %s.", flowGroup, flowName, flowExecutionId,
              triggerTimestamp);
        }
      } else if (rowsUpdated > 1) {
        resultSet.close();
        throw new IOException(String.format("Expect at most 1 row in table for a given trigger event. %s rows "
            + "exist for the trigger flow event for table %s flow group: %s, flow name: %s flow execution id: %s "
            + "and trigger timestamp: %s.", i, tableName, flowGroup, flowName, flowExecutionId, triggerTimestamp));
      }
      Timestamp pursuantTimestamp = resultSet.getTimestamp(2);
      resultSet.close();
      long currentTimeMillis = System.currentTimeMillis();
      // Another host has obtained lease and no further steps required
      if (pursuantTimestamp == null) {
        LOG.info("Another host has already successfully obtained lease for flow group: %s, flow name: %s flow execution "
            + "id: %s and trigger timestamp: %s", flowGroup, flowName, flowExecutionId, triggerTimeMillis);
        return LeaseAttemptStatus.LEASE_OBTAINED;
      } else if (pursuantTimestamp.getTime() + linger <= currentTimeMillis) {
        return LeaseAttemptStatus.PREVIOUS_LEASE_EXPIRED;
      }
      // Previous lease owner still has valid lease (pursuant + linger > current timestamp)
        return LeaseAttemptStatus.PREVIOUS_LEASE_VALID;
    } catch (SQLException e) {
      throw new IOException(String.format("Error encountered while trying to obtain lease on trigger flow event for "
              + "table %s flow group: %s, flow name: %s flow execution id: %s and trigger timestamp: %s", tableName,
          flowGroup, flowName, flowExecutionId, triggerTimestamp), e);
    }
  }

  @Override
  public boolean updatePursuantTimestamp(String flowGroup, String flowName, String flowExecutionId,
      FlowActionType flowActionType, Timestamp triggerTimestamp)
      throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement updateStatement = connection.prepareStatement(
            String.format(UPDATE_PURSUANT_TIMESTAMP_STATEMENT, tableName, epsilon))) {
        int i = 0;
        updateStatement.setString(++i, flowGroup);
        updateStatement.setString(++i, flowName);
        updateStatement.setString(++i, flowExecutionId);
        updateStatement.setString(++i, flowActionType.toString());
        updateStatement.setTimestamp(++i, triggerTimestamp);
        i = updateStatement.executeUpdate();
        connection.commit();

        if (i != 1) {
          LOG.warn("Expected to update 1 row's pursuant timestamp for a flow trigger event but instead updated {}", i);
        }
        return i >= 1;
    } catch (SQLException e) {
      throw new IOException(String.format("Encountered exception while trying to update pursuant timestamp to null for "
              + "flowGroup: %s flowName: %s flowExecutionId: %s flowAction: %s triggerTimestamp: %s. Exception is %s",
          flowGroup, flowName, flowExecutionId, flowActionType, triggerTimestamp, e));
    }
  }
}
