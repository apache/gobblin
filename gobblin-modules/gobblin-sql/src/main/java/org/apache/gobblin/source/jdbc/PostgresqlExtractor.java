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
package org.apache.gobblin.source.jdbc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.exception.HighWatermarkException;
import org.apache.gobblin.source.extractor.exception.RecordCountException;
import org.apache.gobblin.source.extractor.exception.SchemaException;
import org.apache.gobblin.source.extractor.extract.Command;
import org.apache.gobblin.source.extractor.utils.Utils;
import org.apache.gobblin.source.extractor.watermark.Predicate;
import org.apache.gobblin.source.extractor.watermark.WatermarkType;
import org.apache.gobblin.source.workunit.WorkUnit;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class PostgresqlExtractor extends JdbcExtractor {
  private static final String CONNECTION_DATABASE = "source.conn.database";
  private static final String POSTGRES_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private static final String POSTGRES_DATE_FORMAT = "yyyy-MM-dd";
  private static final String POSTGRES_HOUR_FORMAT = "HH";
  private static final long SAMPLERECORDCOUNT = -1;

  public PostgresqlExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public String getHourPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting hour predicate for Postgres");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, POSTGRES_HOUR_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting date predicate for Postgres");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, POSTGRES_DATE_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting timestamp predicate for Postgres");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, POSTGRES_TIMESTAMP_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public List<Command> getSchemaMetadata(String schema, String entity)
      throws SchemaException {
    log.debug("Build query to get schema");
    List<Command> commands = new ArrayList<>();
    List<String> queryParams = Arrays.asList(entity, schema);

    String metadataSql = "select col.column_name, col.data_type, "
        + "case when CHARACTER_OCTET_LENGTH is null then 0 else 0 end as length, "
        + "case when NUMERIC_PRECISION is null then 0 else NUMERIC_PRECISION end as precesion, "
        + "case when NUMERIC_SCALE is null then 0 else NUMERIC_SCALE end as scale, "
        + "case when is_nullable='NO' then 'false' else 'true' end as nullable, '' as format, " + "'' as comment "
        + "from information_schema.COLUMNS col "
        + "WHERE upper(col.table_name)=upper(?) AND upper(col.table_schema)=upper(?) "
        + "order by col.ORDINAL_POSITION";

    commands.add(getCommand(metadataSql, JdbcCommand.JdbcCommandType.QUERY));
    commands.add(getCommand(queryParams, JdbcCommand.JdbcCommandType.QUERYPARAMS));
    return commands;
  }

  @Override
  public List<Command> getHighWatermarkMetadata(String schema, String entity, String watermarkColumn,
      List<Predicate> predicateList)
      throws HighWatermarkException {
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
    int fetchsize = this.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_JDBC_RESULTSET_FETCH_SIZE,
        ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_JDBC_RESULTSET_FETCH_SIZE);
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
    String database = this.workUnitState.getProp(CONNECTION_DATABASE);

    return "jdbc:postgresql://" + host.trim() + ":" + port + "/" + database.trim();
  }

  /** {@inheritdoc} */
  @Override
  protected boolean convertBitToBoolean() {
    return false;
  }

  @Override
  public Map<String, String> getDataTypeMap() {
    Map<String, String> dataTypeMap =
        ImmutableMap.<String, String>builder().put("tinyint", "int").put("smallint", "int").put("mediumint", "int")
            .put("integer", "int").put("int", "int").put("bigint", "long").put("float", "float").put("double", "double")
            .put("double precision", "double").put("decimal", "double").put("numeric", "double").put("date", "date").put("timestamp", "timestamp")
            .put("timestamp without time zone", "timestamp").put("timestamp with time zone", "timestamp")
            .put("datetime", "timestamp").put("time", "time").put("char", "string").put("varchar", "string")
            .put("varbinary", "string").put("text", "string").put("tinytext", "string").put("mediumtext", "string")
            .put("character varying", "string").put("longtext", "string").put("blob", "string").put("tinyblob", "string").put("mediumblob", "string")
            .put("longblob", "string").put("enum", "string").build();
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
  public long extractSampleRecordCountFromQuery(String query) {
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
    if (inputQuery.contains(" where ")) {
      String newQuery = query.replace(limitString, " AND 1=1");
      if (newQuery.toLowerCase().contains(" where and 1=1")) {
        return query.replace(limitString, " 1=1");
      }
      return newQuery;
    }
    return query.replace(limitString, " where 1=1");
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
      List<Predicate> predicateList)
      throws IOException {
    return null;
  }
}
