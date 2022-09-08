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

import org.apache.commons.dbcp.BasicDataSource;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.typesafe.config.Config;

import javax.inject.Singleton;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metastore.MysqlStateStore;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;


/**
 * An implementation of {@link UserQuotaManager} that stores quota usage in mysql.
 */
@Slf4j
@Singleton
public class MysqlUserQuotaManager extends AbstractUserQuotaManager {
  private final MysqlQuotaStore mysqlStore;

  @Inject
  public MysqlUserQuotaManager(Config config) throws IOException {
    super(config);
    this.mysqlStore = createQuotaStore(config);
  }

  // This implementation does not need to update quota usage when the service restarts or it's leadership status changes
  public void init(Collection<Dag<JobExecutionPlan>> dags) {
  }

  @Override
  int incrementJobCount(String user, CountType countType) throws IOException {
    try {
      return this.mysqlStore.increaseCount(user, countType);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  void decrementJobCount(String user, CountType countType) throws IOException {
    try {
      this.mysqlStore.decreaseCount(user, countType);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  int getCount(String name, CountType countType) throws IOException {
    return this.mysqlStore.getCount(name, countType);
  }

  /**
   * Creating an instance of MysqlQuotaStore.
   */
  protected MysqlQuotaStore createQuotaStore(Config config) throws IOException {
    String quotaStoreTableName = ConfigUtils.getString(config, ServiceConfigKeys.QUOTA_STORE_DB_TABLE_KEY,
        ServiceConfigKeys.DEFAULT_QUOTA_STORE_DB_TABLE);

    BasicDataSource basicDataSource = MysqlStateStore.newDataSource(config);

    return new MysqlQuotaStore(basicDataSource, quotaStoreTableName);
  }

  static class MysqlQuotaStore {
    protected final DataSource dataSource;
    final String tableName;
    private final String GET_USER_COUNT;
    private final String GET_REQUESTER_COUNT;
    private final String GET_FLOWGROUP_COUNT;
    private final String INCREASE_USER_COUNT_SQL;
    private final String INCREASE_REQUESTER_COUNT_SQL;
    private final String INCREASE_FLOW_COUNT_SQL;
    private final String DECREASE_USER_COUNT_SQL;
    private final String DECREASE_REQUESTER_COUNT_SQL;
    private final String DECREASE_FLOWGROUP_COUNT_SQL;
    private final String DELETE_USER_SQL;

    public MysqlQuotaStore(BasicDataSource dataSource, String tableName)
        throws IOException {
      this.dataSource = dataSource;
      this.tableName = tableName;

      GET_USER_COUNT = "SELECT user_count FROM " + tableName + " WHERE name = ? FOR UPDATE";
      GET_REQUESTER_COUNT = "SELECT requester_count FROM " + tableName + " WHERE name = ? FOR UPDATE";
      GET_FLOWGROUP_COUNT = "SELECT flowgroup_count FROM " + tableName + " WHERE name = ? FOR UPDATE";
      INCREASE_USER_COUNT_SQL = "INSERT INTO " + tableName + " (name, user_count) VALUES (?, 1) "
          + "ON DUPLICATE KEY UPDATE user_count=user_count+1";
      INCREASE_REQUESTER_COUNT_SQL = "INSERT INTO " + tableName + " (name, requester_count) VALUES (?, 1) "
          + "ON DUPLICATE KEY UPDATE requester_count=requester_count+1";
      INCREASE_FLOW_COUNT_SQL = "INSERT INTO " + tableName + " (name, flowgroup_count) VALUES (?, 1) "
          + "ON DUPLICATE KEY UPDATE flowgroup_count=flowgroup_count+1";
      DECREASE_USER_COUNT_SQL = "UPDATE " + tableName + " SET user_count=GREATEST(0, user_count-1) WHERE name = ?";
      DECREASE_REQUESTER_COUNT_SQL = "UPDATE " + tableName + " SET requester_count=GREATEST(0, requester_count-1) WHERE name = ?";
      DECREASE_FLOWGROUP_COUNT_SQL = "UPDATE " + tableName + " SET flowgroup_count=flowgroup_count-1 WHERE name = ?";
      DELETE_USER_SQL = "DELETE FROM " + tableName + " WHERE name = ? AND user_count<1 AND flowgroup_count<1";

      String createQuotaTable = "CREATE TABLE IF NOT EXISTS " + tableName + " (name VARCHAR(20) CHARACTER SET latin1 NOT NULL, "
          + "user_count INT NOT NULL DEFAULT 0, requester_count INT NOT NULL DEFAULT 0, flowgroup_count INT NOT NULL DEFAULT 0, "
          + "PRIMARY KEY (name), " + "UNIQUE INDEX ind (name))";
      try (Connection connection = dataSource.getConnection(); PreparedStatement createStatement = connection.prepareStatement(createQuotaTable)) {
        createStatement.executeUpdate();
      } catch (SQLException e) {
        throw new IOException("Failure creation table " + tableName, e);
      }
    }

    /**
     * returns count of countType for the name. if the row does not exist, returns zero.
     */
    @VisibleForTesting
    int getCount(String name, CountType countType) throws IOException {
      String selectStatement = countType == CountType.USER_COUNT ? GET_USER_COUNT : GET_FLOWGROUP_COUNT;
      try (Connection connection = dataSource.getConnection();
          PreparedStatement queryStatement = connection.prepareStatement(selectStatement)) {
        queryStatement.setString(1, name);
        try (ResultSet rs = queryStatement.executeQuery()) {
          if (rs.next()) {
            return rs.getInt(1);
          } else {
            return -1;
          }
        }
      } catch (Exception e) {
        throw new IOException("failure retrieving count from user/flowGroup " + name, e);
      }
    }

    public int increaseCount(String name, CountType countType) throws IOException, SQLException {
      Connection connection = dataSource.getConnection();
      connection.setAutoCommit(false);

      String selectStatement;
      String increaseStatement;

      switch(countType) {
        case USER_COUNT:
          selectStatement = GET_USER_COUNT;
          increaseStatement = INCREASE_USER_COUNT_SQL;
          break;
        case REQUESTER_COUNT:
          selectStatement = GET_REQUESTER_COUNT;
          increaseStatement = INCREASE_REQUESTER_COUNT_SQL;
          break;
        case FLOWGROUP_COUNT:
          selectStatement = GET_FLOWGROUP_COUNT;
          increaseStatement = INCREASE_FLOW_COUNT_SQL;
          break;
        default:
          throw new IOException("Invalid count type " + countType);
      }

      ResultSet rs = null;
      try (PreparedStatement statement1 = connection.prepareStatement(selectStatement);
          PreparedStatement statement2 = connection.prepareStatement(increaseStatement)) {
        statement1.setString(1, name);
        statement2.setString(1, name);
        rs = statement1.executeQuery();
        statement2.executeUpdate();
        connection.commit();
        if (rs != null && rs.next()) {
          return rs.getInt(1);
        } else {
          return 0;
        }
      } catch (SQLException e) {
        connection.rollback();
        throw new IOException("Failure increasing count for user/flowGroup " + name, e);
      } finally {
        if (rs != null) {
          rs.close();
        }
        connection.close();
      }
    }

    public void decreaseCount(String name, CountType countType) throws IOException, SQLException {
      Connection connection = dataSource.getConnection();
      connection.setAutoCommit(false);

      String selectStatement;
      String decreaseStatement;

      switch(countType) {
        case USER_COUNT:
          selectStatement = GET_USER_COUNT;
          decreaseStatement = DECREASE_USER_COUNT_SQL;
          break;
        case REQUESTER_COUNT:
          selectStatement = GET_REQUESTER_COUNT;
          decreaseStatement = DECREASE_REQUESTER_COUNT_SQL;
          break;
        case FLOWGROUP_COUNT:
          selectStatement = GET_FLOWGROUP_COUNT;
          decreaseStatement = DECREASE_FLOWGROUP_COUNT_SQL;
          break;
        default:
          throw new IOException("Invalid count type " + countType);
      }

      ResultSet rs = null;
      try (
          PreparedStatement statement1 = connection.prepareStatement(selectStatement);
          PreparedStatement statement2 = connection.prepareStatement(decreaseStatement);
          PreparedStatement statement3 = connection.prepareStatement(DELETE_USER_SQL)) {
        statement1.setString(1, name);
        statement2.setString(1, name);
        statement3.setString(1, name);
        rs = statement1.executeQuery();
        statement2.executeUpdate();
        statement3.executeUpdate();
        connection.commit();
        if (rs != null && rs.next() && rs.getInt(1) == 0) {
          log.warn("Decrement job count was called for " + name + " when the count was already zero/absent.");
        }
      } catch (SQLException e) {
        connection.rollback();
        throw new IOException("Failure decreasing count from user/flowGroup " + name, e);
      } finally {
        if (rs != null) {
          rs.close();
        }
        connection.close();
      }
    }
  }
}