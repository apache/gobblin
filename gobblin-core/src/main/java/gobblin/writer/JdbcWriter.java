package gobblin.writer;

import gobblin.util.ForkOperatorUtils;
import gobblin.util.jdbc.DataSourceBuilder;
import gobblin.writer.commands.JdbcWriterCommands;
import gobblin.writer.commands.JdbcWriterCommandsFactory;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import javax.sql.DataSource;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class JdbcWriter implements DataWriter<JdbcEntryData> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroJdbcWriter.class);

  private final Connection conn;
  private final State state;
  private final JdbcWriterCommands commands;
  private final String table;
  private boolean failed;
  private long count;

  public JdbcWriter(JdbcWriterBuilder builder) {
    this.state = builder.destination.getProperties();
    this.state.appendToListProp(ConfigurationKeys.FORK_BRANCH_ID_KEY, Integer.toString(builder.branch));
    String stagingTableKey = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE,
        builder.branches,
        builder.branch);
    this.table = Objects.requireNonNull(state.getProp(stagingTableKey), "Staging table is missing with key " + stagingTableKey);
    this.commands = JdbcWriterCommandsFactory.newInstance(state);
    try {
      this.conn = createConnection();
      conn.setAutoCommit(false);
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
      commands.insert(conn, table, record);
      count++;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
