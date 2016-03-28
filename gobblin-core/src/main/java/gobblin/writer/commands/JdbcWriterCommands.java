package gobblin.writer.commands;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.Map;

public interface JdbcWriterCommands extends JdbcBufferedInserter {

  public void createTableStructure(Connection conn, String fromStructure, String targetTableName) throws SQLException;

  public boolean isEmpty(Connection conn, String table) throws SQLException;

  public void truncate(Connection conn, String table) throws SQLException;

  public void deleteAll(Connection conn, String table) throws SQLException;

  public void drop(Connection conn, String table) throws SQLException;

  public Map<String, JDBCType> retrieveDateColumns(Connection conn, String table) throws SQLException;

  public void copyTable(Connection conn, String databaseName, String from, String to) throws SQLException;
}