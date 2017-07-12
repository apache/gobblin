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

import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.exception.HighWatermarkException;
import gobblin.source.extractor.utils.Utils;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.extractor.watermark.WatermarkType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.exception.RecordCountException;
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.extractor.extract.Command;
import gobblin.source.workunit.WorkUnit;
import lombok.extern.slf4j.Slf4j;


/**
 * SqlServer extractor using JDBC protocol
 *
 * @author nveeramr
 */
@Slf4j
public class SqlServerExtractor extends JdbcExtractor {
  private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private static final String DATE_FORMAT = "yyyy-MM-dd";
  private static final String HOUR_FORMAT = "HH";
  private static final long SAMPLERECORDCOUNT = -1;

  public SqlServerExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public String getLeftDelimitedIdentifier() {
    return this.enableDelimitedIdentifier ? "[" : "";
  }

  @Override
  public String getRightDelimitedIdentifier() {
    return this.enableDelimitedIdentifier ? "]" : "";
  }

  @Override
  public List<Command> getSchemaMetadata(String schema, String entity) throws SchemaException {
    log.debug("Build query to get schema");
    List<Command> commands = new ArrayList<>();
    List<String> queryParams = Arrays.asList(entity, schema);

    String metadataSql = "select " + " col.column_name, " + " col.data_type, "
        + " case when CHARACTER_OCTET_LENGTH is null then 0 else 0 end as length, "
        + " case when NUMERIC_PRECISION is null then 0 else NUMERIC_PRECISION end as precesion, "
        + " case when NUMERIC_SCALE is null then 0 else NUMERIC_SCALE end as scale, "
        + " case when is_nullable='NO' then 'false' else 'true' end as nullable, " + " '' as format, "
        + " '' as comment " + " from information_schema.COLUMNS col "
        + " WHERE upper(col.table_name)=upper(?) AND upper(col.table_schema)=upper(?) "
        + " order by col.ORDINAL_POSITION ";

    commands.add(JdbcExtractor.getCommand(metadataSql, JdbcCommand.JdbcCommandType.QUERY));
    commands.add(JdbcExtractor.getCommand(queryParams, JdbcCommand.JdbcCommandType.QUERYPARAMS));
    return commands;
  }

  @Override
  public List<Command> getHighWatermarkMetadata(String schema, String entity, String watermarkColumn,
      List<Predicate> predicateList) throws HighWatermarkException {
    log.debug("Build query to get high watermark");
    List<Command> commands = new ArrayList<>();

    String columnProjection = "max(" + Utils.getCoalesceColumnNames(watermarkColumn) + ")";
    String watermarkFilter = this.concatPredicates(predicateList);
    String query = this.getExtractSql();

    if (StringUtils.isBlank(watermarkFilter)) {
      watermarkFilter = "1=1";
    }
    query = query.replace(this.getOutputColumnProjection(), columnProjection)
        .replace(ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL, watermarkFilter);

    commands.add(JdbcExtractor.getCommand(query, JdbcCommand.JdbcCommandType.QUERY));
    return commands;
  }

  @Override
  public List<Command> getCountMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws RecordCountException {
    log.debug("Build query to get source record count");
    List<Command> commands = new ArrayList<>();

    String columnProjection = "COUNT(1)";
    String watermarkFilter = this.concatPredicates(predicateList);
    String query = this.getExtractSql();

    if (StringUtils.isBlank(watermarkFilter)) {
      watermarkFilter = "1=1";
    }
    query = query.replace(this.getOutputColumnProjection(), columnProjection)
        .replace(ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL, watermarkFilter);
    String sampleFilter = this.constructSampleClause();

    if (!StringUtils.isEmpty(sampleFilter)) {
      String col = sampleFilter + " 1 as col ";
      query = "SELECT COUNT(1) FROM (" + query.replace(" COUNT(1) ", col) + ")temp";
    }
    commands.add(JdbcExtractor.getCommand(query, JdbcCommand.JdbcCommandType.QUERY));
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

    String watermarkFilter = this.concatPredicates(predicateList);
    String query = this.getExtractSql();
    if (StringUtils.isBlank(watermarkFilter)) {
      watermarkFilter = "1=1";
    }

    query = query.replace(ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL, watermarkFilter);
    String sampleFilter = this.constructSampleClause();

    if (!StringUtils.isEmpty(sampleFilter)) {
      String columnProjection = this.getOutputColumnProjection();
      String newColumnProjection = sampleFilter + " " + columnProjection;
      query = query.replace(columnProjection, newColumnProjection);
    }

    commands.add(JdbcExtractor.getCommand(query, JdbcCommand.JdbcCommandType.QUERY));
    commands.add(JdbcExtractor.getCommand(fetchSize, JdbcCommand.JdbcCommandType.FETCHSIZE));
    return commands;
  }

  @Override
  public Map<String, String> getDataTypeMap() {
    Map<String, String> dataTypeMap = ImmutableMap.<String, String> builder().put("smallint", "int")
        .put("tinyint", "int").put("int", "int").put("bigint", "long").put("decimal", "double").put("numeric", "double")
        .put("float", "float").put("real", "double").put("money", "double").put("smallmoney", "double")
        .put("binary", "string").put("varbinary", "string").put("char", "string").put("varchar", "string")
        .put("nchar", "string").put("nvarchar", "string").put("text", "string").put("ntext", "string")
        .put("image", "string").put("hierarchyid", "string").put("uniqueidentifier", "string").put("date", "date")
        .put("datetime", "timestamp").put("datetime2", "timestamp").put("datetimeoffset", "timestamp")
        .put("smalldatetime", "timestamp").put("time", "time").put("bit", "boolean").build();
    return dataTypeMap;
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
    String url = "jdbc:sqlserver://" + host.trim() + ":" + port;
    return url;
  }

  @Override
  public long exractSampleRecordCountFromQuery(String query) {
    if (StringUtils.isBlank(query)) {
      return SAMPLERECORDCOUNT;
    }

    long recordcount = SAMPLERECORDCOUNT;
    String inputQuery = query.toLowerCase();

    int limitStartIndex = inputQuery.indexOf(" top ");
    int limitEndIndex = getLimitEndIndex(inputQuery, limitStartIndex);
    if (limitStartIndex > 0) {
      String limitValue = query.substring(limitStartIndex + 5, limitEndIndex);
      try {
        recordcount = Long.parseLong(limitValue);
      } catch (Exception e) {
        log.error("Ignoring incorrct limit value in input query:" + limitValue);
      }
    }
    return recordcount;
  }

  @Override
  public String removeSampleClauseFromQuery(String query) {
    if (StringUtils.isBlank(query)) {
      return null;
    }

    String outputQuery = query;
    String inputQuery = query.toLowerCase();
    int limitStartIndex = inputQuery.indexOf(" top ");
    int limitEndIndex = getLimitEndIndex(inputQuery, limitStartIndex);
    if (limitStartIndex > 0) {
      outputQuery = query.substring(0, limitStartIndex) + " " + query.substring(limitEndIndex);
    }
    return outputQuery;
  }

  private static int getLimitEndIndex(String inputQuery, int limitStartIndex) {
    int limitEndIndex = -1;
    if (limitStartIndex > 0) {
      limitEndIndex = limitStartIndex + 5;
      String remainingQuery = inputQuery.substring(limitEndIndex);
      boolean numFound = false;

      int pos = 0;
      while (pos < remainingQuery.length()) {
        char ch = remainingQuery.charAt(pos);
        if (ch == ' ' && !numFound) {
          pos++;
          continue;
        } else if (numFound && (!Character.isDigit(ch))) {
          break;
        } else {
          numFound = true;
        }
        pos++;
      }
      limitEndIndex = limitEndIndex + pos;
    }
    return limitEndIndex;
  }

  @Override
  public String constructSampleClause() {
    long sampleRowCount = this.getSampleRecordCount();
    if (sampleRowCount >= 0) {
      return " top " + sampleRowCount;
    }
    return "";
  }

  @Override
  public String getWatermarkSourceFormat(WatermarkType watermarkType) {
    String columnFormat = null;
    switch (watermarkType) {
      case TIMESTAMP:
        columnFormat = "yyyy-MM-dd HH:mm:ss";
        break;
      case DATE:
        columnFormat = "yyyy-MM-dd";
        break;
      default:
        log.error("Watermark type " + watermarkType.toString() + " not recognized");
    }
    return columnFormat;
  }

  @Override
  public String getHourPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting hour predicate for Sqlserver");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, HOUR_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting date predicate for Sqlserver");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, DATE_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting timestamp predicate for Sqlserver");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, TIMESTAMP_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }
}
