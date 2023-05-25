package org.apache.gobblin.runtime.api;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.typesafe.config.Config;

import javax.sql.DataSource;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlDataSourceFactory;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * TODO: multiActiveScheduler change here write doc for this class
 */
public class MysqlSchedulerLeaseDeterminationStore implements SchedulerLeaseDeterminationStore {
  public static final String CONFIG_PREFIX = "MysqlSchedulerLeaseDeterminationStore";

  protected final DataSource dataSource;
  private final String tableName;
  private final long epsilon;
  private final long linger;
  // TODO: define retention eventually on this table
  // TODO: also add to primary key the type of event "launch"
  // initialize table with one entry only if it doesn't exist. these configs
  // another table with epsilon and linger then join the two tables
  protected static final String WHERE_CLAUSE_TO_MATCH_ROW = "WHERE flow_group=? AND flow_name=? AND flow_execution_id=? "
      + "AND flow_action=? AND ABS(trigger_event_timestamp-?) <= %s";
  protected static final String ATTEMPT_INSERT_AND_GET_PURSUANT_TIMESTAMP_STATEMENT = "INSERT INTO %s (flow_group, "
      + "flow_name, flow_execution_id, flow_action, trigger_event_timestamp) VALUES (?, ?, ?, ?, ?) WHERE NOT EXISTS ("
      + "SELECT * FROM %s " + WHERE_CLAUSE_TO_MATCH_ROW + "; SELECT ROW_COUNT() AS rows_inserted_count, "
      + "pursuant_timestamp FROM %s " + WHERE_CLAUSE_TO_MATCH_ROW;

  protected static final String UPDATE_PURSUANT_TIMESTAMP_STATEMENT = "UPDATE %s SET pursuant_timestamp = NULL "
      + WHERE_CLAUSE_TO_MATCH_ROW;

  /* TODO: Potentially use the following statement that obtains the lease with the insert, otherwise returns the original
   value, but it's a bit hard to reason about this statement working.
  protected static final String ATTEMPT_OBTAINING_LEASE_ELSE_RETURN_EXISTING_ROW_STATEMENT = "SELECT * FROM "
      + "(SELECT ? AS flow_group, ? AS flow_name, ? AS flow_execution_id, ? AS trigger_event_timestamp) AS new_rows"
      + "WHERE EXISTS (SELECT * FROM %s WHERE %s.flow_group = new_rows.flow_group AND %s.flow_name = new_rows.flow_name)"
      + " AND %s.flow_execution_id = new_rows.flow_execution_id AND ABS(trigger_event_timestamp-?) <= %s)";
  */
  private static final String CREATE_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %S (" + "flow_group varchar("
      + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, flow_name varchar("
      + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, " + "flow_execution_id varchar("
      + ServiceConfigKeys.MAX_FLOW_EXECUTION_ID_LENGTH + ") NOT NULL, flow_action varchar(100) NOT NULL, "
      + "trigger_event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
      + "pursuant_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
      + "PRIMARY KEY (flow_group,flow_name,flow_execution_id,flow_action,trigger_event_timestamp)";

  public MysqlSchedulerLeaseDeterminationStore(Config config) throws IOException {
    if (config.hasPath(CONFIG_PREFIX)) {
      config = config.getConfig(CONFIG_PREFIX).withFallback(config);
    } else {
      throw new IOException("Please specify the config for MysqlDagActionStore");
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
  }

  @Override
  public LeaseAttemptStatus attemptInsertAndGetPursuantTimestamp(String flowGroup, String flowName,
      String flowExecutionId, FlowActionType flowActionType, long triggerTimeMillis)
      throws IOException {
    Timestamp triggerTimestamp = new Timestamp(triggerTimeMillis);
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement insertStatement = connection.prepareStatement(
            String.format(ATTEMPT_INSERT_AND_GET_PURSUANT_TIMESTAMP_STATEMENT, tableName, tableName, epsilon,
                tableName, epsilon))) {
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
        throw new IOException(String.format("Unexpected error where no result returned while trying to obtain lease. "
                + "This error indicates that no row was inserted but also no entry existed for trigger flow event for "
                + "table %s flow group: %s, flow name: %s flow execution id: %s and trigger timestamp: %s", tableName,
            flowGroup, flowName, flowExecutionId, triggerTimestamp));
      }
      // If a row was inserted, then we have obtained the lease
      int rowsUpdated = resultSet.getInt(0);
      if (rowsUpdated == 1) {
        // TODO: write to dagactionstore then update pursuant to null
        if (updatePursuantTimestamp(flowGroup, flowName, flowExecutionId, flowActionType, triggerTimestamp)) {
          return LeaseAttemptStatus.LEASE_OBTAINED;
        }
      } else if (rowsUpdated > 1) {
        throw new IOException(String.format("Expect at most 1 row in table for a given trigger event. %s rows "
            + "exist for the trigger flow event for table %s flow group: %s, flow name: %s flow execution id: %s "
            + "and trigger timestamp: %s.", i, tableName, flowGroup, flowName, flowExecutionId, triggerTimestamp));
      }
      Timestamp pursuantTimestamp = resultSet.getTimestamp(1);
      long currentTimeMillis = System.currentTimeMillis();
      // Another host has obtained lease and no further steps required
      if (pursuantTimestamp == null) {
        return LeaseAttemptStatus.LEASE_OBTAINED;
      } else if (pursuantTimestamp.getTime() + linger <= currentTimeMillis) {
        return LeaseAttemptStatus.PREVIOUS_LEASE_EXPIRED;
      }
      // Previous lease owner still has valid lease (pursuant + linger > current timestamp)
        return LeaseAttemptStatus.PREVIOUS_LEASE_VALID;
    } catch (SQLException e) {
      throw new IOException(String.format("Error encountered while trying to obtain lease on trigger flow event for "
              + "table %s flow group: %s, flow name: %s flow execution id: %s and trigger timestamp: %s", tableName,
          flowGroup, flowName, flowExecutionId, triggerTimestamp, e));
    }
  }

  @Override
  public boolean updatePursuantTimestamp(String flowGroup, String flowName, String flowExecutionId,
      FlowActionType flowActionType, Timestamp triggerTimestamp)
      throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement updateStatement = connection.prepareStatement(
            String.format(UPDATE_PURSUANT_TIMESTAMP_STATEMENT, tableName))) {
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
      throw new IOException(String.format("Encountered exception while trying to update pursuant timestamp to null for flowGroup: %s flowName: %s"
          + "flowExecutionId: %s flowAction: %s triggerTimestamp: %s. Exception is %s", flowGroup, flowName, flowExecutionId,
          flowActionType, triggerTimestamp), e);
    }
  }
}
