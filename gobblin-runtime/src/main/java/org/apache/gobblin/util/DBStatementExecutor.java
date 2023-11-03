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
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;


/**
 * Many database stores require common functionality that can be stored in a utility class. The functionality
 * includes executing prepared statements on a data source object and SQL queries at fixed intervals.
 * The caller of the class MUST maintain ownership of the {@link DataSource} and close this instance when the
 * {@link DataSource} is about to be closed well. Both are to be done only once this instance will no longer be used.
 */
public class DBStatementExecutor implements Closeable {
  private final DataSource dataSource;
  private final Logger log;
  private final ArrayList<ScheduledThreadPoolExecutor> scheduledExecutors;

  public DBStatementExecutor(DataSource dataSource, Logger log) {
    this.dataSource = dataSource;
    this.log = log;
    this.scheduledExecutors = new ArrayList<>();
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
    this.scheduledExecutors.add(executor);
  }

  /**
   * Call before closing the data source object associated with this instance to also shut down any executors expecting
   * to be run on the data source.
   */
  @Override
  public void close() {
    for (ScheduledThreadPoolExecutor executor : this.scheduledExecutors) {
      executor.shutdownNow();
    }
  }
}
