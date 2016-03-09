package gobblin.converter.jdbc;

import java.sql.JDBCType;
import java.util.Objects;

public class JdbcEntryMetaDatum {
  private final String columnName;
  private final JDBCType jdbcType;

  public JdbcEntryMetaDatum(String columnName, JDBCType jdbcType) {
    this.columnName = Objects.requireNonNull(columnName);
    this.jdbcType = Objects.requireNonNull(jdbcType);
  }

  public String getColumnName() {
    return columnName;
  }

  public JDBCType getJdbcType() {
    return jdbcType;
  }

  /**
   * Note that it only uses column name as a key
   * {@inheritDoc}
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((columnName == null) ? 0 : columnName.hashCode());
    return result;
  }

  /**
   * Note that it only uses column name as a key
   * {@inheritDoc}
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    JdbcEntryMetaDatum other = (JdbcEntryMetaDatum) obj;
    if (columnName == null) {
      if (other.columnName != null)
        return false;
    } else if (!columnName.equals(other.columnName))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return String.format("JdbcEntrySchema [columnName=%s, jdbcType=%s]", columnName, jdbcType);
  }
}
