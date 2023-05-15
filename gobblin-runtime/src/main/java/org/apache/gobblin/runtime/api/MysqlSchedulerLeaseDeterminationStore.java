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
 * TODO: multiActiveScheduler change here
 */
public class MysqlSchedulerLeaseDeterminationStore implements SchedulerLeaseDeterminationStore {
  public static final String CONFIG_PREFIX = "MysqlSchedulerLeaseDeterminationStore";

  protected final DataSource dataSource;
  private final String tableName;
  private final long epsilon;
  private final long linger;
  protected static final String CONDITIONALLY_INSERT_TO_OBTAIN_LEASE_STATEMENT = "INSERT INTO %s (flow_group, "
      + "flow_name, flow_execution_id, trigger_event_timestamp) VALUES (?, ?, ?, ?) WHERE NOT EXISTS ("
      + "SELECT * FROM %s WHERE flow_group=? AND flow_name=? AND flow_execution_id=? AND ABS(trigger_event_timestamp-?)"
      + " <= %s)";
  protected static final String GET_PURSUANT_TIMESTAMP_STATEMENT = "SELECT pursuant_timestamp FROM %s WHERE flow_group=? "
      + "AND flow_name=? AND flow_execution_id=? AND ABS(trigger_event_timestamp-?)";
  // TODO: Potentially use the following statement that obtains the lease with the insert, otherwise returns the original
  // value, but it's a bit hard to reason about this statement working.
  protected static final String ATTEMPT_OBTAINING_LEASE_ELSE_RETURN_EXISTING_ROW_STATEMENT = "SELECT * FROM "
      + "(SELECT ? AS flow_group, ? AS flow_name, ? AS flow_execution_id, ? AS trigger_event_timestamp) AS new_rows"
      + "WHERE EXISTS (SELECT * FROM %s WHERE %s.flow_group = new_rows.flow_group AND %s.flow_name = new_rows.flow_name)"
      + " AND %s.flow_execution_id = new_rows.flow_execution_id AND ABS(trigger_event_timestamp-?) <= %s)";
  private static final String CREATE_TABLE_STATEMENT = "CREATE TABLE IF NOT EXISTS %S (" + "flow_group varchar("
      + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, flow_name varchar("
      + ServiceConfigKeys.MAX_FLOW_GROUP_LENGTH + ") NOT NULL, " + "flow_execution_id varchar("
      + ServiceConfigKeys.MAX_FLOW_EXECUTION_ID_LENGTH + ") NOT NULL, "
      + "trigger_event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
      + "pursuant_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
      + "PRIMARY KEY (flow_group,flow_name,flow_execution_id,trigger_event_timestamp)";

  // Note: that union-ing the read value in the same query will require keeping track of the
  // host number to see who did the insert and checking that. Instead we can conditionally
  // write and read separately

  // OVERALL LOGIC
  // check if event within epsilon exists
  // QUERY 1: if one does exist return the row and check value of pursuant
  // if pursuant == null someone else completed
  // RETURN
  // QUERY 2: else if pursuant + linger <= current_timestamp
  // insert row yourself
  // QUERY 2: else pursuant + linger > current_timestamp
  // set reminder to check back after linger
  // QUERY 1: else insert row urself
  // RETURN

  // QUERY should check if row exists and return value if so
  // if row does not exist then do write
  // then based on what result we get and checking corresponding value do other stuff
    /*
    ```
      INSERT INTO mytable (column1, column2, column3)
      SELECT 'value1', 'value2', 'value3'
      FROM dual
      WHERE NOT EXISTS (
        SELECT *
        FROM mytable
        WHERE column1 = 'value1'
          AND column2 = 'value2'
          AND column3 = 'value3'
      );
      ```
     */
  public MysqlSchedulerLeaseDeterminationStore(Config config) throws IOException {
    if (config.hasPath(CONFIG_PREFIX)) {
      config = config.getConfig(CONFIG_PREFIX).withFallback(config);
    } else {
      throw new IOException("Please specify the config for MysqlDagActionStore");
    }

    this.tableName = ConfigUtils.getString(config, ConfigurationKeys.SCHEDULER_LEASE_DETERMINATION_STORE_DB_TABLE_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_LEASE_DETERMINATION_STORE_DB_TABLE);
    this.epsilon = ConfigUtils.getLong(config, ConfigurationKeys.SCHEDULER_TRIGGER_EVENT_EPSILON_VALUE_MILLIS,
        ConfigurationKeys.DEFAULT_SCHEDULER_TRIGGER_EVENT_EPSILON_MILLIS);
    this.linger = ConfigUtils.getLong(config, ConfigurationKeys.SCHEDULER_TRIGGER_EVENT_EPSILON_VALUE_MILLIS,
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
  public boolean attemptLeaseOfLaunchEvent(String flowGroup, String flowName, String flowExecutionId,
      Timestamp triggerTimestamp)
      throws IOException {
    try (Connection connection = this.dataSource.getConnection();
        PreparedStatement insertStatement = connection.prepareStatement(
            String.format(CONDITIONALLY_INSERT_TO_OBTAIN_LEASE_STATEMENT, tableName, tableName, epsilon))) {
      int i = 0;
      insertStatement.setString(++i, flowGroup);
      insertStatement.setString(++i, flowName);
      insertStatement.setString(++i, flowExecutionId);
      insertStatement.setTimestamp(++i, triggerTimestamp);
      insertStatement.setString(++i, flowGroup);
      insertStatement.setString(++i, flowName);
      insertStatement.setString(++i, flowExecutionId);
      insertStatement.setTimestamp(++i, triggerTimestamp);
      i = insertStatement.executeUpdate();

      if (i > 1) {
        throw new IOException(String.format("Expect at most 1 row in table for a given trigger event. %s rows "
            + "exist for the trigger flow event for table %s flow group: %s, flow name: %s flow execution id: %s "
            + "and trigger timestamp: %s.", i, tableName, flowGroup, flowName, flowExecutionId, triggerTimestamp));
      }
      connection.commit();
      // Return whether this query obtained lease on trigger event and completed insert
      return i == 1;
    } catch (SQLException e) {
      throw new IOException(String.format("Error encountered while trying to obtain lease on trigger flow event for "
          + "table %s flow group: %s, flow name: %s flow execution id: %s and trigger timestamp: %s", tableName,
          flowGroup, flowName, flowExecutionId, triggerTimestamp, e));
    }
  }

  @Override
  public Timestamp getPursuantTimestamp(String flowGroup, String flowName, String flowExecutionId,
      Timestamp triggerTimestamp)
      throws IOException {
    return null;
  }
}
