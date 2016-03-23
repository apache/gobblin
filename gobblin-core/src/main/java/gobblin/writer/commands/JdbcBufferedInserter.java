package gobblin.writer.commands;

import gobblin.converter.jdbc.JdbcEntryData;

import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcBufferedInserter {

  public void insert(Connection conn, String table, JdbcEntryData jdbcEntryData) throws SQLException;

  public void flush(Connection conn) throws SQLException;
}
