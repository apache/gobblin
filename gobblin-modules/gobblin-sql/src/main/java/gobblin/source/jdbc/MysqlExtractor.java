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

import org.apache.commons.lang3.StringUtils;

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

import lombok.extern.slf4j.Slf4j;


/**
 * MySql extractor using JDBC protocol
 *
 * @author nveeramr
 */
@Slf4j
public class MysqlExtractor extends JdbcExtractor {
  private static final String MYSQL_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private static final String MYSQL_DATE_FORMAT = "yyyy-MM-dd";
  private static final String MYSQL_HOUR_FORMAT = "HH";
  private static final long SAMPLERECORDCOUNT = -1;

  public MysqlExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public String getHourPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting hour predicate for Mysql");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, MYSQL_HOUR_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting date predicate for Mysql");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, MYSQL_DATE_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting timestamp predicate for Mysql");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, MYSQL_TIMESTAMP_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public List<Command> getSchemaMetadata(String schema, String entity) throws SchemaException {
    log.debug("Build query to get schema");
    List<Command> commands = new ArrayList<>();
    boolean promoteUnsignedInt = this.workUnitState.getPropAsBoolean(
        ConfigurationKeys.SOURCE_QUERYBASED_PROMOTE_UNSIGNED_INT_TO_BIGINT,
        ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_PROMOTE_UNSIGNED_INT_TO_BIGINT);

    String promoteUnsignedIntQueryParam = promoteUnsignedInt ? "% unsigned" : "dummy";

    List<String> queryParams = Arrays.asList(promoteUnsignedIntQueryParam, entity, schema);

    String metadataSql = "select " + " col.column_name, "
        + " case when col.column_type like (?) and col.data_type = 'int' then 'bigint' else col.data_type end"
        + " as data_type,"
        + " case when CHARACTER_OCTET_LENGTH is null then 0 else 0 end as length, "
        + " case when NUMERIC_PRECISION is null then 0 else NUMERIC_PRECISION end as precesion, "
        + " case when NUMERIC_SCALE is null then 0 else NUMERIC_SCALE end as scale, "
        + " case when is_nullable='NO' then 'false' else 'true' end as nullable, " + " '' as format, "
        + " case when col.column_comment is null then '' else col.column_comment end as comment "
        + " from information_schema.COLUMNS col "
        + " WHERE upper(col.table_name)=upper(?) AND upper(col.table_schema)=upper(?) "
        + " order by col.ORDINAL_POSITION ";

    commands.add(getCommand(metadataSql, JdbcCommand.JdbcCommandType.QUERY));
    commands.add(getCommand(queryParams, JdbcCommand.JdbcCommandType.QUERYPARAMS));
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

    commands.add(getCommand(query, JdbcCommand.JdbcCommandType.QUERY));
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
    query = query + sampleFilter;

    if (!StringUtils.isEmpty(sampleFilter)) {
      query = "SELECT COUNT(1) FROM (" + query.replace(" COUNT(1) ", " 1 ") + ")temp";
    }
    commands.add(getCommand(query, JdbcCommand.JdbcCommandType.QUERY));
    return commands;
  }

  @Override
  public List<Command> getDataMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws DataRecordException {
    log.debug("Build query to extract data");
    List<Command> commands = new ArrayList<>();
    int fetchsize = Integer.MIN_VALUE;

    String watermarkFilter = this.concatPredicates(predicateList);
    String query = this.getExtractSql();
    if (StringUtils.isBlank(watermarkFilter)) {
      watermarkFilter = "1=1";
    }

    query = query.replace(ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL, watermarkFilter);
    String sampleFilter = this.constructSampleClause();
    query = query + sampleFilter;

    commands.add(getCommand(query, JdbcCommand.JdbcCommandType.QUERY));
    commands.add(getCommand(fetchsize, JdbcCommand.JdbcCommandType.FETCHSIZE));
    return commands;
  }

  @Override
  public String getConnectionUrl() {
    String host = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME);
    String port = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_PORT);
    String database = this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_SCHEMA);
    String url = "jdbc:mysql://" + host.trim() + ":" + port + "/" + database.trim();

    if (Boolean.valueOf(this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_IS_COMPRESSION_ENABLED))) {
      return url + "?useCompression=true";
    }
    return url;
  }

  /** {@inheritdoc} */
  @Override
  protected boolean convertBitToBoolean() {
    return false;
  }

  @Override
  public Map<String, String> getDataTypeMap() {
    Map<String, String> dataTypeMap = ImmutableMap.<String, String> builder().put("tinyint", "int")
        .put("smallint", "int").put("mediumint", "int").put("int", "int").put("bigint", "long").put("float", "float")
        .put("double", "double").put("decimal", "double").put("numeric", "double").put("date", "date")
        .put("timestamp", "timestamp").put("datetime", "timestamp").put("time", "time").put("char", "string")
        .put("varchar", "string").put("varbinary", "string").put("text", "string").put("tinytext", "string")
        .put("mediumtext", "string").put("longtext", "string").put("blob", "string").put("tinyblob", "string")
        .put("mediumblob", "string").put("longblob", "string").put("enum", "string").build();
    return dataTypeMap;
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
  public long exractSampleRecordCountFromQuery(String query) {
    if (StringUtils.isBlank(query)) {
      return SAMPLERECORDCOUNT;
    }

    long recordcount = SAMPLERECORDCOUNT;

    String limit = null;
    String inputQuery = query.toLowerCase();
    int limitIndex = inputQuery.indexOf(" limit ");
    if (limitIndex > 0) {
      limit = query.substring(limitIndex + 7).trim();
    }

    if (StringUtils.isNotBlank(limit)) {
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
    String limitString = "";
    String inputQuery = query.toLowerCase();
    int limitIndex = inputQuery.indexOf(" limit");
    if (limitIndex > 0) {
      limitString = query.substring(limitIndex);
    }
    return query.replace(limitString, "");
  }

  @Override
  public String constructSampleClause() {
    long sampleRowCount = this.getSampleRecordCount();
    if (sampleRowCount >= 0) {
      return " limit " + sampleRowCount;
    }
    return "";
  }

  @Override
  public String getLeftDelimitedIdentifier() {
    return this.enableDelimitedIdentifier ? "`" : "";
  }

  @Override
  public String getRightDelimitedIdentifier() {
    return this.enableDelimitedIdentifier ? "`" : "";
  }

  @Override
  public Iterator<JsonElement> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList) throws IOException {
    return null;
  }
}
