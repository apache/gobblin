package gobblin.writer.commands;

import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.converter.jdbc.JdbcEntryDatum;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class GenericJdbcBufferedInserter implements JdbcBufferedInserter {
  private static final Logger LOG = LoggerFactory.getLogger(GenericJdbcBufferedInserter.class);

  private static final String INSERT_STATEMENT_PREFIX_FORMAT = "INSERT INTO %s.%s (%s) VALUES (%s)";
  private static final Joiner JOINER_ON_COMMA = Joiner.on(',');

  private final Connection conn;
  private final int maxParamSize;
  private Retryer<Void> retryer;

  private int maxBatchSize;
  private int currBatchSize;
  private PreparedStatement pStmt;

  public GenericJdbcBufferedInserter(State state, Connection conn) {
    this.conn = conn;
    this.maxBatchSize = state.getPropAsInt(WRITER_JDBC_INSERT_BATCH_SIZE, DEFAULT_WRITER_JDBC_INSERT_BATCH_SIZE);
    this.maxParamSize = state.getPropAsInt(WRITER_JDBC_MAX_PARAM_SIZE, DEFAULT_WRITER_JDBC_MAX_PARAM_SIZE);

    //retry after 2, 4, 8, 16... sec, max 30 sec delay
    this.retryer = RetryerBuilder.<Void>newBuilder()
                                 .retryIfException()
                                 .withWaitStrategy(WaitStrategies.exponentialWait(1000, 30, TimeUnit.SECONDS))
                                 .withStopStrategy(StopStrategies.stopAfterAttempt(5))
                                 .build();
  }

  @Override
  public void insert(String databaseName, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    if (pStmt == null) {
      initializeBatch(databaseName, table, jdbcEntryData);
    }

    int i = 0;
    for (JdbcEntryDatum datum : jdbcEntryData) {
      pStmt.setObject(++i, datum.getVal());
    }
    pStmt.addBatch();
    currBatchSize++;

    if(currBatchSize >= maxBatchSize) {
      insertBatch();
    }
  }

  private void initializeBatch(String databaseName, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    List<String> columnNames = Lists.newArrayList();
    for (JdbcEntryDatum datum : jdbcEntryData) {
      columnNames.add(datum.getColumnName());
    }
    String insertPstmtStr = String.format(INSERT_STATEMENT_PREFIX_FORMAT,
                                          databaseName,
                                          table,
                                          JOINER_ON_COMMA.join(columnNames),
                                          JOINER_ON_COMMA.useForNull("?").join(new String[columnNames.size()]));

    LOG.info("Prepared insert statement: " + insertPstmtStr);
    pStmt = conn.prepareStatement(insertPstmtStr);

    int actualBatchSize = Math.min(maxBatchSize, maxParamSize / columnNames.size());
    if(maxBatchSize != actualBatchSize) {
      LOG.info("Changing batch size from " + maxBatchSize + " to " + actualBatchSize + " due to # of params limitation " + maxParamSize + " , # of columns: " + columnNames.size());
    }
    maxBatchSize = actualBatchSize;
  }

  private void insertBatch() throws SQLException {
    Callable<Void> insertCall = new Callable<Void>() { //Need a Callable interface to be wrapped by Retryer.
      @Override
      public Void call() throws Exception {
        pStmt.executeBatch();
        return null;
      }
    };

    try {
      retryer.wrap(insertCall).call();
    } catch (Exception e) {
      throw new RuntimeException("Failed to insert.", e);
    }
    resetBatch();
  }

  private void resetBatch() throws SQLException {
    pStmt.clearBatch();
    pStmt.clearParameters();
    currBatchSize = 0;
  }

  @Override
  public void flush() throws SQLException {
    if (currBatchSize > 0) {
      insertBatch();
    }
  }
}
