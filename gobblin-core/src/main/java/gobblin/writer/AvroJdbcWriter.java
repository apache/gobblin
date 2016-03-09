package gobblin.writer;

import gobblin.util.ForkOperatorUtils;
import gobblin.util.jdbc.DataSourceBuilder;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

public class AvroJdbcWriter implements DataWriter<GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroJdbcWriter.class);
  private static final String INSERT_STATEMENT_FORMAT = "INSERT INTO %s (%s) VALUES (%s)";
  private static final Set<Type> SUPPORTED_TYPES;
  static {
    SUPPORTED_TYPES = ImmutableSet.<Type>builder()
                                 .add(Type.BOOLEAN)
                                 .add(Type.INT)
                                 .add(Type.LONG)
                                 .add(Type.FLOAT)
                                 .add(Type.DOUBLE)
                                 .add(Type.STRING)
                                 .add(Type.RECORD)
                                 .add(Type.ENUM)
                                 .add(Type.UNION)
                                 .build();
  }

  private final Connection conn;
  private final State state;
  private final PreparedStatement insertPstmt;
  private final Map<String, Type> flattened;
  private final Set<String> dateColumns;
  private final String tableName;
  private boolean failed;
  private long count;

  public AvroJdbcWriter(AvroJdbcWriterBuilder builder) {
    this.state = builder.destination.getProperties();
    this.state.appendToListProp(ConfigurationKeys.FORK_BRANCH_ID_KEY, Integer.toString(builder.branch));
    this.flattened = flat(builder.schema);
    try {
      this.conn = createConnection();
      this.dateColumns = retrieveDateColumns();
      LOG.info("Date columns: " + dateColumns);
      this.tableName = getStagingTable(builder);

      conn.setAutoCommit(false);
      this.insertPstmt = conn.prepareStatement(createPrepareStatement(tableName, flattened));
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
   * https://dev.mysql.com/doc/connector-j/en/connector-j-reference-type-conversions.html
   *
   * @return
   * @throws SQLException
   */
  private ImmutableSet<String> retrieveDateColumns() throws SQLException {
    Set<String> targetDataTypes = ImmutableSet.<String>builder()
                                              .add("DATE")
                                              .add("DATETIME")
                                              .add("TIME")
                                              .add("TIMESTAMP")
                                              .build();

    final String SELECT_SQL_PSTMT = "SELECT column_name, column_type FROM information_schema.columns where table_name = ?";
    String table = Objects.requireNonNull(state.getProp(ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME));

    ImmutableSet.Builder<String> dateColumnsBuilder = ImmutableSet.builder();
    try (PreparedStatement pstmt = conn.prepareStatement(SELECT_SQL_PSTMT)) {
      pstmt.setString(1, table);
      ResultSet rs = pstmt.executeQuery();
      if(!rs.first()) {
        throw new IllegalArgumentException("No result from information_schema.columns");
      }
      do {
        String type = rs.getString("column_type").toUpperCase();
        if(targetDataTypes.contains(type)) {
          dateColumnsBuilder.add(rs.getString("column_name").toUpperCase());
        }
      } while (rs.next());
    }
    return dateColumnsBuilder.build();
  }

  /**
   * 1. If staging table has been passed from user, use it.
   * 2. If staging table has not been passed from user.
   * 2.1. Check permission if user has permission to create and drop table.
   * 2.1. use staging table prefix, branchs, branch ID to create staging table.
   * @return
   * @throws SQLException
   */
  private String getStagingTable(AvroJdbcWriterBuilder builder) throws SQLException {
    String key = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_TABLE,
        builder.branches,
        builder.branch);
    return Objects.requireNonNull(state.getProp(key));
  }

  /**
   * Convert Avro into INSERT statement and executes it.
   * {@inheritDoc}
   * @see gobblin.writer.DataWriter#write(java.lang.Object)
   */
  @Override
  public void write(GenericRecord record) throws IOException {
    LOG.info("Writing " + record);
    try {
      int i = 1;
      for (Map.Entry<String, Type> entry : flattened.entrySet()) {
        Object val = record.get(entry.getKey());

        if(val != null && dateColumns.contains(entry.getKey().toUpperCase())) {
          LOG.info("Date column detected " + entry.getKey() + " , " + entry.getValue());
          if(!Type.LONG.equals(entry.getValue())) {
            throw new IllegalArgumentException("Avro data type needs to be long for corresponding date,time,timestamp,datetime data type on JDBC side");
          }
          Long ms = Long.parseLong(val.toString());
          Timestamp ts = new Timestamp(ms);
          insertPstmt.setTimestamp(i++, ts);
          continue;
        }
        insertPstmt.setObject(i++, val == null ? null : val.toString());
      }
      LOG.info("Executing " + insertPstmt);
      insertPstmt.executeUpdate();
      insertPstmt.clearParameters();
    } catch (Exception e) {
      failed = true;
      throw new RuntimeException(e);
    }
  }

  /**
   * Avro schema can be a recursive structure and this method will flatten it and produces key val map represents schema.
   * @param schema
   * @return
   */
  private Map<String, Type> flat(Schema schema) {
    Map<String, Type> flattened = new LinkedHashMap<>();
    produceFlattenedHeler(schema, null, flattened);
    return flattened;
  }

  private void produceFlattenedHeler(Schema schema, Field field, Map<String, Type> flattened) {
    if (!SUPPORTED_TYPES.contains(schema.getType())) {
      throw new IllegalArgumentException(schema.getType() + " is not supported");
    }

    if (Type.RECORD.equals(schema.getType())) {
      for(Field f : schema.getFields()) { //Recurse
        produceFlattenedHeler(f.schema(), f, flattened);
      }
    } else {
      Type t = determineType(schema);
      if(field == null) {
        throw new IllegalArgumentException("Invalid Avro schema, no name has been assigned to " + schema);
      }
      Type existing = flattened.put(field.name(), t);
      if(existing != null) {
        //No duplicate name allowed when flattening (not considering name space we don't have any assumption between namespace and actual database field name)
        throw new IllegalArgumentException("Duplicate name detected in Avro schema.");
      }
    }
  }

  private Type determineType(Schema schema) { //TODO what happens String and int? Why anyone would do that? Should it make it fail?
    if (!Type.UNION.equals(schema.getType())) {
      return schema.getType();
    }
    List<Schema> schemas = schema.getTypes();
    if(schemas.size() > 2) {
      throw new IllegalArgumentException("More than two types are not supported " + schemas);
    }

    Type t = null;
    for (Schema s : schemas) {
      if (Type.NULL.equals(s.getType())) {
        continue;
      }
      if (t == null) {
        t = s.getType();
      } else {
        throw new IllegalArgumentException("Union type of " + schemas + " is not supported.");
      }
    }
    if (t != null) {
      return t;
    }
    throw new IllegalArgumentException("Cannot determine type of " + schema);
  }

  private static String createPrepareStatement(String tableName, Map<String, Type> flattened) {
    return String.format(INSERT_STATEMENT_FORMAT,
                         tableName,
                         Joiner.on(',').join(flattened.keySet()),
                         Joiner.on(',').useForNull("?").join(new String[flattened.size()]));
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
