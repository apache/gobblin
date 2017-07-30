/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.source.jdbc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.exception.HighWatermarkException;
import gobblin.source.extractor.exception.RecordCountException;
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.utils.Utils;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.extractor.watermark.WatermarkType;
import gobblin.source.workunit.WorkUnit;


/**
 * Oracle extractor using JDBC protocol
 *
 * @author bjvanov, jinhyukchang, Lorand Bendig
 */
@Slf4j
public class OracleExtractor extends JdbcExtractor {

  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private static final String HOUR_FORMAT = "HH";

  private static final long SAMPLERECORDCOUNT = -1;
  private static final Pattern SAMPLE_CLAUSE_PATTERN =
      Pattern.compile(".*(rownum\\s*<\\s*=\\s*(\\d+)).*", Pattern.CASE_INSENSITIVE);

  private static final String EMPTY_CONDITION = "1=1";

  private static final String METADATA_SCHEMA_PSTMT_FORMAT =
      "SELECT " +
        "column_name, " +
        "UPPER(data_type), " +
        "NVL(data_length, 0) as length, " +
        "NVL(data_precision, 0) as precesion, " +
        "NVL(data_scale, 0) as scale, " +
        "CASE NVL(NULLABLE, 'Y') WHEN 'Y' THEN 1 ELSE 0 END as nullable, " +
        "' ' as format, " +
        "NVL(comments, ' ') as \"COMMENT\", " +
        "column_id " +
        "FROM " +
        "all_tab_columns " +
        "JOIN all_col_comments USING (owner, table_name, column_name) " +
        "WHERE UPPER(owner) = (?) " +
        "AND UPPER(table_name) = (?) " +
        "ORDER BY " +
        "column_id, column_name";

  private static Map<String, String> dataTypeMap = ImmutableMap.<String, String> builder()
      .put("char", "string")
      .put("varchar2", "string")
      .put("varchar", "string")
      .put("nchar", "string")
      .put("nvarchar2", "string")
      .put("nclob", "string")
      .put("clob", "string")
      .put("long", "string")
      .put("raw", "string")
      .put("long raw", "string")
      .put("rowid", "string")
      .put("urowid", "string")
      .put("xmltype", "string")
      .put("smallint", "int")
      .put("int", "int")
      .put("integer", "int")
      .put("bigint", "long")
      .put("binary_float", "float")
      .put("binary_double", "double")
      .put("float", "double")
      .put("number", "double")
      .put("numeric", "double")
      .put("dec", "double")
      .put("decimal", "double")
      .put("real", "double")
      .put("double precision", "double")
      .put("date", "date")
      .put("interval year", "date")
      .put("interval day", "timestamp")
      .put("datetime", "timestamp")
      .put("timestamp", "timestamp")
      .put("timestamp(0)", "timestamp")
      .put("timestamp(1)", "timestamp")
      .put("timestamp(2)", "timestamp")
      .put("timestamp(3)", "timestamp")
      .put("timestamp(4)", "timestamp")
      .put("timestamp(5)", "timestamp")
      .put("timestamp(6)", "timestamp")
      .put("timestamp(7)", "timestamp")
      .put("timestamp(8)", "timestamp")
      .put("timestamp(9)", "timestamp")
      .put("timestamp with time zone", "timestamp")
      .put("timezone with local timezone", "timestamp")
      .build();

  public OracleExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public List<Command> getSchemaMetadata(String schema, String entity) throws SchemaException {
    log.debug("Build query to get schema");
    Preconditions.checkNotNull(schema);
    Preconditions.checkNotNull(entity);
    List<Command> commands = new ArrayList<>();
    commands.add(getCommand(METADATA_SCHEMA_PSTMT_FORMAT, JdbcCommand.JdbcCommandType.QUERY));
    commands.add(getCommand(Arrays.asList(schema, entity), JdbcCommand.JdbcCommandType.QUERYPARAMS));
    return commands;
  }

  @Override
  public List<Command> getHighWatermarkMetadata(String schema, String entity, String watermarkColumn,
      List<Predicate> predicateList) throws HighWatermarkException {
    log.debug("Build query to get high watermark");
    List<Command> commands = new ArrayList<>();

    String columnProjection = "max(" + Utils.getCoalesceColumnNames(watermarkColumn) + ")";
    String watermarkFilter = StringUtils.defaultIfBlank(this.concatPredicates(predicateList), EMPTY_CONDITION);
    String query = this.getExtractSql();

    query = query.replace(this.getOutputColumnProjection(), columnProjection)
        .replace(ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL, watermarkFilter);

    commands.add(getCommand(query, JdbcCommand.JdbcCommandType.QUERY));
    return commands;
  }

  @Override
  public List<Command> getCountMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws RecordCountException {
    log.debug("Build query to get source record count");
    List<Command> commands = new ArrayList<>();

    String columnProjection = "count(1)";
    String watermarkFilter = StringUtils.defaultIfBlank(this.concatPredicates(predicateList), EMPTY_CONDITION);
    String query = this.getExtractSql();

    query = query.replace(this.getOutputColumnProjection(), columnProjection)
        .replace(ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL, watermarkFilter);
    query = addSampleQueryPart(query);
    query = castCountQuery(query);
    commands.add(getCommand(query, JdbcCommand.JdbcCommandType.QUERY));
    return commands;
  }

  @Override
  public List<Command> getDataMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws DataRecordException {
    log.debug("Build query to extract data");
    List<Command> commands = new ArrayList<>();
    int fetchSize = this.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_JDBC_RESULTSET_FETCH_SIZE,
        ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_JDBC_RESULTSET_FETCH_SIZE);
    log.info("Setting jdbc resultset fetch size as " + fetchSize);

    String watermarkFilter = StringUtils.defaultIfBlank(this.concatPredicates(predicateList), EMPTY_CONDITION);
    String query = this.getExtractSql();

    query = query.replace(ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL, watermarkFilter);
    query = addSampleQueryPart(query);

    commands.add(getCommand(query, JdbcCommand.JdbcCommandType.QUERY));
    commands.add(getCommand(fetchSize, JdbcCommand.JdbcCommandType.FETCHSIZE));
    return commands;
  }

  @Override
  public Map<String, String> getDataTypeMap() {
    return dataTypeMap;
  }

  @Override
  public long exractSampleRecordCountFromQuery(String query) {
    if (StringUtils.isBlank(query)) {
      return SAMPLERECORDCOUNT;
    }
    long recordcount = SAMPLERECORDCOUNT;
    Matcher matcher = SAMPLE_CLAUSE_PATTERN.matcher(query);
    if (matcher.matches()) {
      String limit = matcher.group(2);
      try {
        recordcount = Long.parseLong(limit);
      } catch (Exception e) {
        log.error("Ignoring incorrct limit value in input query:" + limit);
      }
    }
    return recordcount;
  }

  @Override
  public String removeSampleClauseFromQuery(String query) {
    if (StringUtils.isBlank(query)) {
      return null;
    }
    Matcher matcher = SAMPLE_CLAUSE_PATTERN.matcher(query);
    if (matcher.matches()) {
      query = query.replace(matcher.group(1), EMPTY_CONDITION);
    }
    return query;
  }

  @Override
  public String constructSampleClause() {
    long sampleRowCount = this.getSampleRecordCount();
    if (sampleRowCount >= 0) {
      return " rownum <= " + sampleRowCount;
    }
    return "";
  }

  @Override
  public Iterator<JsonElement> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList) throws IOException {
    return null;
  }

  @Override
  public String getConnectionUrl() {
    String host = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME);
    String port = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_PORT);
    String sid = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_SID).trim();
    String url = "jdbc:oracle:thin:@" + host.trim() + (StringUtils.isEmpty(port) ? "" : ":" + port) + ":" + sid;
    return url;
  }

  @Override
  public String getWatermarkSourceFormat(WatermarkType watermarkType) {
    String columnFormat = null;
    switch (watermarkType) {
      case TIMESTAMP:
        columnFormat = "YYYY-MM-dd HH:mm:ss";
        break;
      case DATE:
        columnFormat = "YYYY-MM-dd HH:mm:ss";
        break;
      case SIMPLE:
        break;
      default:
        log.error("Watermark type " + watermarkType.toString() + " not recognized");
    }
    return columnFormat;
  }

  @Override
  public String getHourPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting hour predicate for Oracle");
    String formattedValue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, HOUR_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedValue + "'";
  }

  @Override
  public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting date predicate for Oracle");
    return getTimestampPredicateCondition(column, value, valueFormat, operator);
  }

  /**
   * Oracle timestamp can go up to 9 digit precision. Existing behavior of Gobblin on extractor is to support
   * up to second and Oracle extractor will keep the same behavior.
   *
   * {@inheritDoc}
   * @see gobblin.source.extractor.extract.ProtocolSpecificLayer#getTimestampPredicateCondition(java.lang.String, long, java.lang.String, java.lang.String)
   */
  @Override
  public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting timestamp predicate for Oracle");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, TIMESTAMP_FORMAT);
    return "cast(" + Utils.getCoalesceColumnNames(column) + " as timestamp(0)) " //Support up to second.
        + operator + " " + "to_timestamp('" + formattedvalue + "', 'YYYY-MM-DD HH24:MI:SS')";
  }

  private String addSampleQueryPart(String query) {
    String sampleClause = constructSampleClause();
    if (sampleClause.isEmpty()) {
      return query;
    }
    String where = "where";
    query = query.replaceFirst(where, String.format("where %s and ", sampleClause));
    return query;
  }

  private String castCountQuery(String query) {
    if (this.getSampleRecordCount() >= 0) {
      return "select cast(count(1) as number) from (" + query.replace(" count(1) ", " * ") + ")temp";
    } else {
      return query.replace("count(1)", "cast(count(1) as number)");
    }
  }

}