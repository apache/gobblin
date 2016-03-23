package gobblin.converter.jdbc;

import java.sql.JDBCType;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

public class JdbcEntrySchema implements Iterable<JdbcEntryMetaDatum> {
  private final Map<String, JdbcEntryMetaDatum> jdbcMetaData; //Pair of column name and JDBCType

  public JdbcEntrySchema(Iterable<JdbcEntryMetaDatum> jdbcMetaDatumEntries) {
    Objects.requireNonNull(jdbcMetaDatumEntries);
    ImmutableMap.Builder<String, JdbcEntryMetaDatum> builder = ImmutableSortedMap.naturalOrder();
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

  /**
   * Provides iterator sorted by column name
   * {@inheritDoc}
   * @see java.lang.Iterable#iterator()
   */
  @Override
  public Iterator<JdbcEntryMetaDatum> iterator() {
    return jdbcMetaData.values().iterator();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    JdbcEntrySchema other = (JdbcEntrySchema) obj;
    if (jdbcMetaData == null) {
      if (other.jdbcMetaData != null)
        return false;
    } else if (!jdbcMetaData.equals(other.jdbcMetaData))
      return false;
    return true;
  }
}
