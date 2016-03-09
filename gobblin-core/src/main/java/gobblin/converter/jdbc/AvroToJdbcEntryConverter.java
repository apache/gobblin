package gobblin.converter.jdbc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.util.jdbc.DataSourceBuilder;

/**
 * Converts Avro Schema into JdbcEntrySchema
 * Converts Avro GenericRecord into JdbcEntryData
 *
 * This converter is written based on Avro 1.8 specification http://avro.apache.org/docs/1.8.0/spec.html
 */
public class AvroToJdbcEntryConverter extends Converter<Schema, JdbcEntrySchema, GenericRecord, JdbcEntryData> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroToJdbcEntryConverter.class);
  private static final Set<Type> AVRO_SUPPORTED_TYPES;
  private static final Map<Type, JDBCType> AVRO_TYPE_JDBC_TYPE_MAPPING;
  private static final Set<JDBCType> JDBC_SUPPORTED_TYPES;
  static {
    AVRO_TYPE_JDBC_TYPE_MAPPING = ImmutableMap.<Type, JDBCType>builder()
                                              .put(Type.BOOLEAN, JDBCType.BOOLEAN)
                                              .put(Type.INT, JDBCType.INTEGER)
                                              .put(Type.LONG, JDBCType.BIGINT)
                                              .put(Type.FLOAT, JDBCType.FLOAT)
                                              .put(Type.DOUBLE, JDBCType.DOUBLE)
                                              .put(Type.STRING, JDBCType.VARCHAR)
                                              .put(Type.ENUM, JDBCType.VARCHAR)
                                              .build();

    AVRO_SUPPORTED_TYPES = ImmutableSet.<Type>builder()
                                       .addAll(AVRO_TYPE_JDBC_TYPE_MAPPING.keySet())
                                       .add(Type.UNION)
                                       .build();

    JDBC_SUPPORTED_TYPES = ImmutableSet.<JDBCType>builder()
                                       .addAll(AVRO_TYPE_JDBC_TYPE_MAPPING.values())
                                       .add(JDBCType.DATE)
                                       .add(JDBCType.TIME)
                                       .add(JDBCType.TIMESTAMP)
                                       .build();
  }

  /**
   * Converts Avro schema to JdbcEntrySchema.
   *
   * Few precondition to the Avro schema
   * 1. Avro schema should have one entry type record at first depth.
   * 2. Avro schema can recurse by having record inside record. As RDBMS structure is not recursive, this is not allowed.
   * 3. Supported Avro primitive types and conversion
   *  boolean --> java.lang.Boolean
   *  int --> java.lang.Integer
   *  long --> java.lang.Long or java.sql.Date , java.sql.Time , java.sql.Timestamp
   *  float --> java.lang.Float
   *  double --> java.lang.Double
   *  bytes --> byte[]
   *  string --> java.lang.String
   *  null: only allowed if it's within union (see complex types for more details)
   * 4. Supported Avro complex types
   *  Records: Only first level depth can have Records type. Basically converter will peel out Records type and start with 2nd level.
   *  Enum --> java.lang.String
   *  Unions --> Only allowed if it have one primitive type in it or null type with one primitive type where null will be ignored.
   *  Once Union is narrowed down to one primitive type, it will follow conversion of primitive type above.
   * {@inheritDoc}
   *
   * 5. In order to make conversion from Avro long type to java.sql.Date or java.sql.Time or java.sql.Timestamp,
   * converter will get table metadata from JDBC.
   * 6. As it needs JDBC connection from condition 5, it also assumes that it will use JDBC publisher where it will get connection information from.
   * 7. Conversion assumes that both schema, Avro and JDBC, uses same column name where name space in Avro is ignored.
   *    For case sensitivity, Avro is case sensitive where it differs in JDBC based on underlying database. As Avro is case sensitive, column name equality also take case sensitive in to account.
   *
   * @see gobblin.converter.Converter#convertSchema(java.lang.Object, gobblin.configuration.WorkUnitState)
   */
  @Override
  public JdbcEntrySchema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    LOG.info("Converting schema " + inputSchema);
    try (Connection conn = createConnection(workUnit)) {
      Map<String, Type> avroColumnType = flat(inputSchema);
      Map<String, JDBCType> dateColumnMapping = retrieveDateColumns(workUnit, conn);
      LOG.info("Date column mapping: " + dateColumnMapping);

      List<JdbcEntryMetaDatum> jdbcEntryMetaData = Lists.newArrayList();
      for(Map.Entry<String, Type> avroEntry : avroColumnType.entrySet()) {
        String colName = avroEntry.getKey();
        JDBCType jdbcType = dateColumnMapping.get(colName);
        if(jdbcType == null) {
          jdbcType = AVRO_TYPE_JDBC_TYPE_MAPPING.get(avroEntry.getValue());
        }
        Objects.requireNonNull(jdbcType, "Failed to convert " + avroEntry
                                         + " AVRO_TYPE_JDBC_TYPE_MAPPING: " + AVRO_TYPE_JDBC_TYPE_MAPPING
                                         + " , dateColumnMapping: " + dateColumnMapping);

        jdbcEntryMetaData.add(new JdbcEntryMetaDatum(colName, jdbcType));
      }
      JdbcEntrySchema converted = new JdbcEntrySchema(jdbcEntryMetaData);
      LOG.info("Converted schema into " + converted);
      return converted;
    } catch (SQLException e) {
      throw new SchemaConversionException(e);
    }
  }

  private Connection createConnection(State state) throws SQLException {
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
   * TODO: Make this pluggable
   * @return
   * @throws SQLException
   */
  private Map<String, JDBCType> retrieveDateColumns(State state, Connection conn) throws SQLException {
    Map<String, JDBCType> targetDataTypes = ImmutableMap.<String, JDBCType>builder()
                                              .put("DATE", JDBCType.DATE)
                                              .put("DATETIME", JDBCType.TIME)
                                              .put("TIME", JDBCType.TIME)
                                              .put("TIMESTAMP", JDBCType.TIMESTAMP)
                                              .build();

    final String SELECT_SQL_PSTMT = "SELECT column_name, column_type FROM information_schema.columns where table_name = ?";
    String table = Objects.requireNonNull(state.getProp(ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME));

    ImmutableMap.Builder<String, JDBCType> dateColumnsBuilder = ImmutableMap.builder();
    try (PreparedStatement pstmt = conn.prepareStatement(SELECT_SQL_PSTMT)) {
      pstmt.setString(1, table);
      LOG.info("Retrieving column type information from SQL: " + pstmt);
      ResultSet rs = pstmt.executeQuery();
      if(!rs.first()) {
        throw new IllegalArgumentException("No result from information_schema.columns");
      }
      do {
        String type = rs.getString("column_type").toUpperCase();
        JDBCType convertedType = targetDataTypes.get(type);
        if(convertedType != null) {
          dateColumnsBuilder.put(rs.getString("column_name"), convertedType);
        }
      } while (rs.next());
    }
    return dateColumnsBuilder.build();
  }

  private Map<String, Type> flat(Schema schema) throws SchemaConversionException {
    Map<String, Type> flattened = new LinkedHashMap<>();
    if (!Type.RECORD.equals(schema.getType())) {
      throw new SchemaConversionException(Type.RECORD + " is expected for the first level element in Avro schema " + schema);
    }

    for(Field f : schema.getFields()) {
      produceFlattenedHeler(f.schema(), f, flattened);
    }
    return flattened;
  }

  private void produceFlattenedHeler(Schema schema, Field field, Map<String, Type> flattened) throws SchemaConversionException {
    if (Type.RECORD.equals(schema.getType())) {
      throw new SchemaConversionException(Type.RECORD + " is only allowed for first level.");
    }

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

  private Type determineType(Schema schema) throws SchemaConversionException { //TODO what happens String and int? Why anyone would do that? Should it make it fail?
    if (!AVRO_SUPPORTED_TYPES.contains(schema.getType())) {
      throw new SchemaConversionException(schema.getType() + " is not supported");
    }

    if (!Type.UNION.equals(schema.getType())) {
      return schema.getType();
    }

    List<Schema> schemas = schema.getTypes();
    if(schemas.size() > 2) {
      throw new SchemaConversionException("More than two types are not supported " + schemas);
    }

    Type t = null;
    for (Schema s : schemas) {
      if (Type.NULL.equals(s.getType())) {
        continue;
      }
      if (t == null) {
        t = s.getType();
      } else {
        throw new SchemaConversionException("Union type of " + schemas + " is not supported.");
      }
    }
    if (t != null) {
      return t;
    }
    throw new SchemaConversionException("Cannot determine type of " + schema);
  }

  @Override
  public Iterable<JdbcEntryData> convertRecord(JdbcEntrySchema outputSchema, GenericRecord record,
      WorkUnitState workUnit) throws DataConversionException {
    LOG.info("Converting " + record);
    List<JdbcEntryDatum> jdbcEntryData = Lists.newArrayList();
    for (JdbcEntryMetaDatum entry : outputSchema) {
      final String colName = entry.getColumnName();
      final JDBCType jdbcType = entry.getJdbcType();
      final Object val = record.get(colName);

      if (val == null) {
        jdbcEntryData.add(new JdbcEntryDatum(colName, val));
      }

      if (!JDBC_SUPPORTED_TYPES.contains(jdbcType)) {
        throw new DataConversionException("Unsupported JDBC type detected " + jdbcType);
      }

      switch (jdbcType) {
        case VARCHAR:
          jdbcEntryData.add(new JdbcEntryDatum(colName, val.toString()));
          continue;
        case INTEGER:
          jdbcEntryData.add(new JdbcEntryDatum(colName, Integer.valueOf((int) val)));
          continue;
        case BOOLEAN:
          jdbcEntryData.add(new JdbcEntryDatum(colName, Boolean.valueOf((boolean) val)));
          continue;
        case BIGINT:
          jdbcEntryData.add(new JdbcEntryDatum(colName, Long.valueOf((long) val)));
          continue;
        case FLOAT:
          jdbcEntryData.add(new JdbcEntryDatum(colName, Float.valueOf((float) val)));
          continue;
        case DOUBLE:
          jdbcEntryData.add(new JdbcEntryDatum(colName, Double.valueOf((double) val)));
          continue;
        case DATE:
          jdbcEntryData.add(new JdbcEntryDatum(colName, new Date(TimeUnit.SECONDS.toMillis(Long.valueOf((long) val)))));
          continue;
        case TIME:
          jdbcEntryData.add(new JdbcEntryDatum(colName, new Time(TimeUnit.SECONDS.toMillis(Long.valueOf((long) val)))));
          continue;
        case TIMESTAMP:
          jdbcEntryData.add(new JdbcEntryDatum(colName, new Timestamp(TimeUnit.SECONDS.toMillis(Long.valueOf((long) val)))));
          continue;
        default:
          throw new DataConversionException(jdbcType + " is not supported");
      }
    }
    JdbcEntryData converted = new JdbcEntryData(jdbcEntryData);
    LOG.info("Converted data into " + converted);
    return new SingleRecordIterable<JdbcEntryData>(converted);
  }
}
