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

package org.apache.gobblin.util;

import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;


/**
 * MySQL based implementations of stores require common functionality that can be stored in a utility class. The
 * functionality includes executing prepared statements on a data source object and executing SQL queries at fixed
 * intervals. The instantiater of the class should provide the data source used within this utility.
 */
public class MySQLStoreUtils {
  private final DataSource dataSource;
  private final Logger log;

  public MySQLStoreUtils(DataSource dataSource, Logger log) {
    this.dataSource = dataSource;
    this.log = log;
  }

  /** `j.u.Function` variant for an operation that may @throw IOException or SQLException: preserves method signature checked exceptions */
  @FunctionalInterface
  public interface CheckedFunction<T, R> {
    R apply(T t) throws IOException, SQLException;
  }

  /** Abstracts recurring pattern around resource management and exception re-mapping. */
  public <T> T withPreparedStatement(String sql, CheckedFunction<PreparedStatement, T> f, boolean shouldCommit)
      throws IOException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      T result = f.apply(statement);
      if (shouldCommit) {
        connection.commit();
      }
      statement.close();
      return result;
    } catch (SQLException e) {
      log.warn("Received SQL exception that can result from invalid connection. Checking if validation query is set {} "
          + "Exception is {}", ((HikariDataSource) dataSource).getConnectionTestQuery(), e);
      throw new IOException(e);
    }
  }

  /**
   * Repeats execution of a SQL command at a fixed interval while the service is running. The first execution of the
   * command is immediate.
   * @param sqlCommand SQL string
   * @param interval frequency with which command will run
   * @param timeUnit unit of time for interval
   */
  public void repeatSqlCommandExecutionAtInterval(String sqlCommand, long interval, TimeUnit timeUnit) {
    ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
    Runnable task = () -> {
      try {
        withPreparedStatement(sqlCommand,
            preparedStatement -> {
              int numRowsAffected = preparedStatement.executeUpdate();
              if (numRowsAffected != 0) {
                log.info("{} rows affected by SQL command: {}", numRowsAffected, sqlCommand);
              }
              return numRowsAffected;
            }, true);
      } catch (IOException e) {
        log.error("Failed to execute SQL command: {}", sqlCommand, e);
      }
    };
    executor.scheduleAtFixedRate(task, 0, interval, timeUnit);
  }
}
