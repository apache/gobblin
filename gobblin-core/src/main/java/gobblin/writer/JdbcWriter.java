package gobblin.writer;

import gobblin.util.ForkOperatorUtils;
import gobblin.util.jdbc.DataSourceBuilder;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class JdbcWriter implements DataWriter<JdbcEntryData> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroJdbcWriter.class);
  private static final String INSERT_STATEMENT_FORMAT = "INSERT INTO %s (%s) VALUES (%s)";

  private final Connection conn;
  private final State state;
  private final List<String> columnNames;
  private final PreparedStatement insertPstmt;
  private boolean failed;
  private long count;

  public JdbcWriter(JdbcWriterBuilder builder) {
    this.state = builder.destination.getProperties();
    this.state.appendToListProp(ConfigurationKeys.FORK_BRANCH_ID_KEY, Integer.toString(builder.branch));
    this.columnNames = Lists.newArrayList(builder.schema.getColumnNames());
    try {
      this.conn = createConnection();
      conn.setAutoCommit(false);
      String stagingTableKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE,
                                                                          builder.branches,
                                                                          builder.branch);
      String stagingTable = Objects.requireNonNull(state.getProp(stagingTableKey), "Staging table is missing with key " + stagingTableKey);
      this.insertPstmt = conn.prepareStatement(createPrepareStatementStr(stagingTable, columnNames));
      LOG.info("Created " + insertPstmt.getClass().getSimpleName() + " with " + insertPstmt);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private Connection createConnection() throws SQLException {
    DataSource dataSource = DataSourceBuilder.builder()
                                                            .url(state.getProp(ConfigurationKeys.JDBC_PUBLISHER_URL))
                                                            .driver(state.getProp(ConfigurationKeys.JDBC_PUBLISHER_DRIVER))
                                                            .userName(state.getProp(ConfigurationKeys.JDBC_PUBLISHER_USERNAME))
                                                            .passWord(state.getProp(ConfigurationKeys.JDBC_PUBLISHER_PASSWORD))
                                                            .maxActiveConnections(1)
                                                            .maxIdleConnections(1)
                                                            .state(state)
                                                            .build();

    return dataSource.getConnection();
  }

  /**
   * Convert Avro into INSERT statement and executes it.
   * {@inheritDoc}
   * @see gobblin.writer.DataWriter#write(java.lang.Object)
   */
  @Override
  public void write(JdbcEntryData record) throws IOException {
    LOG.info("Writing " + record);
    try {
      int i = 1;
      for (String columnName : columnNames) {
        insertPstmt.setObject(i++, record.getVal(columnName));
      }
      LOG.info("Executing " + insertPstmt);
      insertPstmt.executeUpdate();
      insertPstmt.clearParameters();
    } catch (Exception e) {
      failed = true;
      throw new RuntimeException(e);
    }
  }

  private String createPrepareStatementStr(String tableName, Collection<String> columnNames) {
    return String.format(INSERT_STATEMENT_FORMAT,
                         tableName,
                         Joiner.on(',').join(columnNames),
                         Joiner.on(',').useForNull("?").join(new String[columnNames.size()]));
  }

  @Override
  public void commit() throws IOException {
    try {
      conn.commit();
    } catch (Exception e) {
      failed = true;
      throw new RuntimeException(e);
    }
  }

  /**
   * Staging table is needed by publisher and won't be cleaned here.
   * {@inheritDoc}
   * @see gobblin.writer.DataWriter#cleanup()
   */
  @Override
  public void cleanup() throws IOException {
  }

  @Override
  public void close() throws IOException {
    try {
      try {
        if (failed && conn != null) {
          conn.rollback();
        }
      } finally {
        if(conn != null) {
          conn.close();
        }
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public long recordsWritten() {
    return count;
  }

  @Override
  public long bytesWritten() throws IOException {
    return -1L;
  }
}
