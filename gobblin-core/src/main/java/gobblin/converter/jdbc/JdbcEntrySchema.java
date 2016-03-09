package gobblin.converter.jdbc;

import java.sql.JDBCType;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

public class JdbcEntrySchema implements Iterable<JdbcEntryMetaDatum> {
  private final Map<String, JdbcEntryMetaDatum> jdbcMetaData; //Pair of column name and JDBCType

  public JdbcEntrySchema(Iterable<JdbcEntryMetaDatum> jdbcMetaDatumEntries) {
    Objects.requireNonNull(jdbcMetaDatumEntries);
    ImmutableMap.Builder<String, JdbcEntryMetaDatum> builder = ImmutableMap.builder();
    for (JdbcEntryMetaDatum datum : jdbcMetaDatumEntries) {
      builder.put(datum.getColumnName(), datum);
    }
    jdbcMetaData = builder.build();
  }

  /**
   * @param columnName Column name case sensitive, as most of RDBMS does.
   * @return Returns JDBCType. If column name does not exist, returns null.
   */
  public JDBCType getJDBCType(String columnName) {
    JdbcEntryMetaDatum datum = jdbcMetaData.get(columnName);
    return datum == null ? null : datum.getJdbcType();
  }

  public Set<String> getColumnNames() {
    return jdbcMetaData.keySet();
  }

  @Override
  public String toString() {
    return String.format("JdbcEntrySchema [jdbcMetaData=%s]", jdbcMetaData);
  }

  @Override
  public Iterator<JdbcEntryMetaDatum> iterator() {
    return jdbcMetaData.values().iterator();
  }
}
