package gobblin.writer.commands;

import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.converter.jdbc.JdbcEntryDatum;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class MySqlWriterCommands implements JdbcWriterCommands {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlWriterCommands.class);

  private static final String CREATE_TABLE_SQL_FORMAT = "CREATE TABLE %s SELECT * FROM %s WHERE 1=2";
  private static final String SELECT_SQL_FORMAT = "SELECT COUNT(*) FROM %s";
  private static final String TRUNCATE_TABLE_FORMAT = "TRUNCATE TABLE %s";
  private static final String DROP_TABLE_SQL_FORMAT = "DROP TABLE %s";
  private static final String INFORMATION_SCHEMA_SELECT_SQL_PSTMT
                              = "SELECT column_name, column_type FROM information_schema.columns where table_name = ?";
  private static final String INSERT_STATEMENT_FORMAT = "INSERT INTO %s (%s) VALUES (%s)";
  private static final String COPY_INSERT_STATEMENT_FORMAT = "INSERT INTO %s SELECT * FROM %s";
  private static final String DELETE_STATEMENT_FORMAT = "DELETE FROM %s";

  private static final Joiner JOINER_ON_COMMA = Joiner.on(',');

  @Override
  public void createTableStructure(Connection conn, String fromStructure, String targetTableName) throws SQLException {
    String sql = String.format(CREATE_TABLE_SQL_FORMAT, targetTableName, fromStructure);
    execute(conn.prepareStatement(sql));
  }

  @Override
  public boolean isEmpty(Connection conn, String table) throws SQLException {
    String sql = String.format(SELECT_SQL_FORMAT, table);
    PreparedStatement pstmt = conn.prepareStatement(sql);
    ResultSet resultSet = pstmt.executeQuery();
    if(!resultSet.first()) {
      throw new RuntimeException("Should have received at least one row from SQL " + pstmt);
    }
    return 0 == resultSet.getInt(1);
  }

  @Override
  public void truncate(Connection conn, String table) throws SQLException {
    String sql = String.format(TRUNCATE_TABLE_FORMAT, table);
    execute(conn.prepareStatement(sql));
  }

  @Override
  public void deleteAll(Connection conn, String table) throws SQLException {
    String deleteSql = String.format(DELETE_STATEMENT_FORMAT, table);
    execute(conn.prepareStatement(deleteSql));
  }

  @Override
  public void drop(Connection conn, String table) throws SQLException {
    LOG.info("Dropping table " + table);
    String sql = String.format(DROP_TABLE_SQL_FORMAT, table);
    execute(conn.prepareStatement(sql));
  }

  /**
   * https://dev.mysql.com/doc/connector-j/en/connector-j-reference-type-conversions.html
   * {@inheritDoc}
   * @see gobblin.writer.commands.JdbcWriterCommands#retrieveDataColumns(java.sql.Connection, java.lang.String)
   */
  @Override
  public Map<String, JDBCType> retrieveDataColumns(Connection conn, String table) throws SQLException {
    Map<String, JDBCType> targetDataTypes = ImmutableMap.<String, JDBCType>builder()
        .put("DATE", JDBCType.DATE)
        .put("DATETIME", JDBCType.TIME)
        .put("TIME", JDBCType.TIME)
        .put("TIMESTAMP", JDBCType.TIMESTAMP)
        .build();

    ImmutableMap.Builder<String, JDBCType> dateColumnsBuilder = ImmutableMap.builder();
    try (PreparedStatement pstmt = conn.prepareStatement(INFORMATION_SCHEMA_SELECT_SQL_PSTMT)) {
      pstmt.setString(1, table);
      LOG.info("Retrieving column type information from SQL: " + pstmt);
      ResultSet rs = pstmt.executeQuery();
      if (!rs.first()) {
        throw new IllegalArgumentException("No result from information_schema.columns");
      }
      do {
        String type = rs.getString("column_type").toUpperCase();
        JDBCType convertedType = targetDataTypes.get(type);
        if (convertedType != null) {
          dateColumnsBuilder.put(rs.getString("column_name"), convertedType);
        }
      } while (rs.next());
    }
    return dateColumnsBuilder.build();
  }

  @Override
  public void insert(Connection conn, String table, JdbcEntryData jdbcEntryData) throws SQLException {
    List<String> columnNames = Lists.newArrayList();
    List<Object> vals = Lists.newArrayList();

    for (JdbcEntryDatum datum : jdbcEntryData) {
      columnNames.add(datum.getColumnName());
      vals.add(datum.getVal());
    }

    PreparedStatement pstmt = conn.prepareStatement(createPrepareStatementStr(table, columnNames));
    for(int i=0; i<vals.size(); i++) {
      pstmt.setObject(i+1, vals.get(i));
    }
    execute(pstmt);
  }

  private String createPrepareStatementStr(String tableName, List<String> columnNames) {
    return String.format(INSERT_STATEMENT_FORMAT,
                         tableName,
                         JOINER_ON_COMMA.join(columnNames),
                         JOINER_ON_COMMA.useForNull("?").join(new String[columnNames.size()]));
  }

  @Override
  public void flush(Connection conn) throws SQLException {
    LOG.info("flush noop"); //TODO add batch
  }

  @Override
  public void copyTable(Connection conn, String from, String to) throws SQLException {
    String sql = String.format(COPY_INSERT_STATEMENT_FORMAT, to, from);
    execute(conn.prepareStatement(sql));
  }

  private void execute(PreparedStatement statement) throws SQLException {
    LOG.info("Executing SQL " + statement);
    statement.execute();
  }
}
