package gobblin.writer.commands;

import gobblin.configuration.ConfigurationKeys;
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

public class MySqlBufferedInserter implements JdbcBufferedInserter {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlBufferedInserter.class);

  private static final String INSERT_STATEMENT_PREFIX_FORMAT = "INSERT INTO %s (%s) VALUES ";
  private static final Joiner JOINER_ON_COMMA = Joiner.on(',');

  private List<JdbcEntryData> pendingInserts;
  private int usedBufferSize;
  private List<String> columnNames;
  private String insertStmtPrefix;
  private PreparedStatement insertPstmtForFixedBatch;
  private Retryer<Boolean> retryer;

  private int batchSize;
  private final int maxBufferSize;
  private final int maxParamSize;

  public MySqlBufferedInserter(State state) {
    this.batchSize = state.getPropAsInt(ConfigurationKeys.WRITER_JDBC_INSERT_BATCH_SIZE,
                                        ConfigurationKeys.DEFAULT_WRITER_JDBC_INSERT_BATCH_SIZE);
    this.maxBufferSize = state.getPropAsInt(ConfigurationKeys.WRITER_JDBC_INSERT_BUFFER_SIZE,
                                            ConfigurationKeys.DEFAULT_WRITER_JDBC_INSERT_BUFFER_SIZE);
    if(batchSize < 1) {
      throw new IllegalArgumentException(ConfigurationKeys.WRITER_JDBC_INSERT_BATCH_SIZE + " should be a positive number");
    }
    this.maxParamSize = state.getPropAsInt(ConfigurationKeys.WRITER_JDBC_MAX_PARAM_SIZE,
                                           ConfigurationKeys.DEFAULT_WRITER_JDBC_MAX_PARAM_SIZE);
  }

  @Override
  public void insert(Connection conn, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    if(columnNames == null) {
      initializeForBatch(conn, table, jdbcEntryData);
    }
    pendingInserts.add(jdbcEntryData);
    usedBufferSize += jdbcEntryData.byteSize();

    if(pendingInserts.size() == batchSize) {
      insertBatch(insertPstmtForFixedBatch); //reuse pre-computed Preparedstatement.
      return;
    }

    if(usedBufferSize >= maxBufferSize) {
      LOG.info("Try to reduce batch size for better performance. Inserting as it reached buffer limit. Buffer size: " + usedBufferSize + " , Threshold: " + maxBufferSize
                + " , # of entries in batch: " + pendingInserts.size());
      PreparedStatement pstmt = conn.prepareStatement(createPrepareStatementStr(insertStmtPrefix, pendingInserts.size()));
      insertBatch(pstmt);
    }
  }

  private void insertBatch(final PreparedStatement pstmt) throws SQLException {
    Callable<Boolean> insertCall = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        int i = 0;
        pstmt.clearParameters();
        for (JdbcEntryData pendingEntry : pendingInserts) {
          for(JdbcEntryDatum datum : pendingEntry) {
            pstmt.setObject(++i, datum.getVal());
          }
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug("Executing SQL " + pstmt);
        }
        return pstmt.execute();
      }
    };

    try {
      retryer.wrap(insertCall).call();
    } catch (Exception e) {
      throw new RuntimeException("Failed to insert.", e);
    }
    resetBatch();
  }

  private void initializeForBatch(Connection conn, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    columnNames = Lists.newArrayList();
    for (JdbcEntryDatum datum : jdbcEntryData) {
      columnNames.add(datum.getColumnName());
    }
    pendingInserts = Lists.newArrayList();

    insertStmtPrefix = String.format(INSERT_STATEMENT_PREFIX_FORMAT, table, JOINER_ON_COMMA.join(columnNames));
    int actualBatchSize = Math.min(batchSize, maxParamSize / columnNames.size());
    if(batchSize != actualBatchSize) {
      LOG.info("Changing batch size from " + batchSize + " to " + actualBatchSize + " due to # of params limitation " + maxParamSize + " , # of columns: " + columnNames.size());
    }
    batchSize = actualBatchSize;
    insertPstmtForFixedBatch = conn.prepareStatement(createPrepareStatementStr(insertStmtPrefix, batchSize));
    if(batchSize == 1) {
      LOG.info("Initialized for insert " + this);
    } else {
      LOG.info("Initialized for batch insert " + this);
    }

    //retry after 2, 4, 8, 16... sec, max 30 sec delay
    retryer = RetryerBuilder.<Boolean>newBuilder()
                            .retryIfException()
                            .withWaitStrategy(WaitStrategies.exponentialWait(1000, 30, TimeUnit.SECONDS))
                            .withStopStrategy(StopStrategies.stopAfterAttempt(5))
                            .build();
  }

  private void resetBatch() {
    usedBufferSize = 0;
    pendingInserts.clear();
  }

  private String createPrepareStatementStr(String insertStmtPrefix, int batchSize) {
    final String VALUE_FORMAT = "(%s)";

    StringBuilder sb = new StringBuilder(insertStmtPrefix);
    String values = String.format(VALUE_FORMAT, JOINER_ON_COMMA.useForNull("?").join(new String[columnNames.size()]));
    sb.append(values);
    for (int i = 1; i < batchSize; i++) {
      sb.append(',')
        .append(values);
    }
    return sb.append(';').toString();
  }

  @Override
  public void flush(Connection conn) throws SQLException {
    if(pendingInserts == null || pendingInserts.isEmpty()) {
      return;
    }
    PreparedStatement pstmt = conn.prepareStatement(createPrepareStatementStr(insertStmtPrefix, pendingInserts.size()));
    insertBatch(pstmt);
  }

  @Override
  public String toString() {
    return String
        .format(
            "MySqlBufferedWriter [pendingInserts=%s, usedBufferSize=%s, columnNames=%s, insertStmtPrefix=%s, retryer=%s, batchSize=%s, maxBufferSize=%s, maxParamSize=%s]",
            pendingInserts, usedBufferSize, columnNames, insertStmtPrefix, retryer, batchSize, maxBufferSize,
            maxParamSize);
  }
}
