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

  public void runSqlCommandWithInterval(String sqlCommand, long interval, TimeUnit timeUnit) {
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
