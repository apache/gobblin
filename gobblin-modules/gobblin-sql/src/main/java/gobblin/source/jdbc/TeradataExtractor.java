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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.exception.HighWatermarkException;
import gobblin.source.extractor.exception.RecordCountException;
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.extract.CommandOutput;
import gobblin.source.extractor.schema.Schema;
import gobblin.source.extractor.utils.Utils;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.extractor.watermark.WatermarkType;
import gobblin.source.workunit.WorkUnit;

/**
 * Teradata extractor using JDBC protocol
 *
 * @author ypopov
 */
@Slf4j
public class TeradataExtractor extends JdbcExtractor {
  private static final String TERADATA_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private static final String TERADATA_DATE_FORMAT = "yyyy-MM-dd";
  private static final String TERADATA_HOUR_FORMAT = "HH";
  private static final long SAMPLE_RECORD_COUNT = -1;
  private static final String ELEMENT_DATA_TYPE = "string";

  private static final String TERADATA_SAMPLE_CLAUSE = " sample ";

  private static final Gson gson = new Gson();

  public TeradataExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public List<Command> getSchemaMetadata(String schema, String entity) throws SchemaException {
    log.debug("Build query to get schema");

    List<Command> commands = new ArrayList<Command>();

    String inputQuery = this.workUnit.getProp(ConfigurationKeys.SOURCE_QUERYBASED_QUERY);
    String metadataSql, predicate = "1=0";
    if(isNullOrEmpty(inputQuery)) {
      metadataSql = "select * from " + schema + "." + entity;
    } else {
      metadataSql = this.removeSampleClauseFromQuery(inputQuery);
    }

    metadataSql = SqlQueryUtils.addPredicate(metadataSql, predicate);
    commands.add(JdbcExtractor.getCommand(metadataSql, JdbcCommand.JdbcCommandType.QUERY));
    return commands;
  }

  @Override
  public List<Command> getHighWatermarkMetadata(String schema, String entity, String watermarkColumn,
      List<Predicate> predicateList) throws HighWatermarkException {
    log.debug("Build query to get high watermark");
    List<Command> commands = new ArrayList<Command>();

    String columnProjection = "max(" + Utils.getCoalesceColumnNames(watermarkColumn) + ")";
    String watermarkFilter = this.concatPredicates(predicateList);
    String query = this.getExtractSql();

    if (isNullOrEmpty(watermarkFilter)) {
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
    List<Command> commands = new ArrayList<Command>();

    String columnProjection = "CAST(COUNT(1) AS BIGINT)";
    String watermarkFilter = this.concatPredicates(predicateList);
    String query = this.getExtractSql();

    if (isNullOrEmpty(watermarkFilter)) {
      watermarkFilter = "1=1";
    }
    query = query.replace(this.getOutputColumnProjection(), columnProjection)
        .replace(ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL, watermarkFilter);
    String sampleFilter = this.constructSampleClause();
    query = query + sampleFilter;

    if (!isNullOrEmpty(sampleFilter)) {
      query = "SELECT " + columnProjection + " FROM (" + query.replace(columnProjection, "1 as t") + ") temp";
    }
    commands.add(JdbcExtractor.getCommand(query, JdbcCommand.JdbcCommandType.QUERY));
    return commands;
  }

  @Override
  public List<Command> getDataMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws DataRecordException {
    log.debug("Build query to extract data");
    List<Command> commands = new ArrayList<Command>();
    int fetchSize = this.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_JDBC_RESULTSET_FETCH_SIZE,
        ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_JDBC_RESULTSET_FETCH_SIZE);

    String watermarkFilter = this.concatPredicates(predicateList);
    String query = this.getExtractSql();
    if (isNullOrEmpty(watermarkFilter)) {
      watermarkFilter = "1=1";
    }

    query = query.replace(ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL, watermarkFilter);
    String sampleFilter = this.constructSampleClause();
    query = query + sampleFilter;

    commands.add(JdbcExtractor.getCommand(query, JdbcCommand.JdbcCommandType.QUERY));
    commands.add(JdbcExtractor.getCommand(fetchSize, JdbcCommand.JdbcCommandType.FETCHSIZE));
    return commands;
  }

  @Override
  public Map<String, String> getDataTypeMap() {
    Map<String, String> dataTypeMap =
        ImmutableMap.<String, String>builder()
            .put("byteint", "int")
            .put("smallint", "int")
            .put("integer", "int")
            .put("bigint", "long")
            .put("float", "float")
            .put("decimal", "double")
            .put("char", "string")
            .put("varchar", "string")
            .put("byte", "bytes")
            .put("varbyte", "bytes")
            .put("date", "date")
            .put("time", "time")
            .put("timestamp", "timestamp")
            .put("clob", "string")
            .put("blob", "string")
            .put("structured udt", "array")
            .put("double precision", "float")
            .put("numeric", "double")
            .put("real", "float")
            .put("character", "string")
            .put("char varying", "string")
            .put("character varying", "string")
            .put("long varchar", "string")
            .put("interval", "string")
            .build();
    return dataTypeMap;
  }

  @Override
  public Iterator<JsonElement> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList) throws IOException {
    return null;
  }

  @Override
  public String getConnectionUrl() {
    String urlPrefix = "jdbc:teradata://";
    String host = this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME);
    checkArgument(!isNullOrEmpty(host), "Connectionn host cannot be null or empty at %s", ConfigurationKeys.SOURCE_CONN_HOST_NAME);

    String port = this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_PORT,"1025");
    String database = this.workUnit.getProp(ConfigurationKeys.SOURCE_QUERYBASED_SCHEMA);
    String defaultUrl = urlPrefix + host.trim() + "/TYPE=FASTEXPORT,DATABASE=" + database.trim() + ",DBS_PORT=" + port.trim() ;
    // use custom url from source.conn.host if Teradata jdbc url available
    return host.contains(urlPrefix) ? host.trim() : defaultUrl;
  }

  @Override
  public long exractSampleRecordCountFromQuery(String query) {
    if (isNullOrEmpty(query)) {
      return SAMPLE_RECORD_COUNT;
    }

    long recordcount = SAMPLE_RECORD_COUNT;

    String limit = null;
    String inputQuery = query.toLowerCase();
    int limitIndex = inputQuery.indexOf(TERADATA_SAMPLE_CLAUSE);
    if (limitIndex > 0) {
      limit = query.substring(limitIndex + TERADATA_SAMPLE_CLAUSE.length()).trim();
    }

    if (!isNullOrEmpty(limit)) {
      try {
        recordcount = Long.parseLong(limit);
      } catch (Exception e) {
        log.error("Ignoring incorrect limit value in input query: {}", limit);
      }
    }
    return recordcount;
  }

  @Override
  public String removeSampleClauseFromQuery(String query) {
    if (isNullOrEmpty(query)) {
      return null;
    }
    String limitString = "";
    String inputQuery = query.toLowerCase();
    int limitIndex = inputQuery.indexOf(TERADATA_SAMPLE_CLAUSE);
    if (limitIndex > 0) {
      limitString = query.substring(limitIndex);
    }
    return query.replace(limitString, "");
  }

  @Override
  public String constructSampleClause() {
    long sampleRowCount = this.getSampleRecordCount();
    if (sampleRowCount >= 0) {
      return TERADATA_SAMPLE_CLAUSE + sampleRowCount;
    }
    return "";
  }

  @Override
  public String getWatermarkSourceFormat(WatermarkType watermarkType) {
    String columnFormat = null;
    switch (watermarkType) {
      case TIMESTAMP:
        columnFormat = TERADATA_TIMESTAMP_FORMAT;
        break;
      case DATE:
        columnFormat = TERADATA_DATE_FORMAT;
        break;
      case HOUR:
        columnFormat = TERADATA_HOUR_FORMAT;
        break;
      case SIMPLE:
        break;
      default:
        log.error("Watermark type {} not recognized", watermarkType.toString());
    }
    return columnFormat;
  }

  @Override
  public String getHourPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting hour predicate for Teradata");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, TERADATA_HOUR_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting date predicate for Teradata");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, TERADATA_DATE_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting timestamp predicate for Teradata");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, TERADATA_TIMESTAMP_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public JsonArray getSchema(CommandOutput<?, ?> response) throws SchemaException, IOException {
    log.debug("Extract schema from resultset");
    ResultSet resultset = null;
    Iterator<ResultSet> itr = (Iterator<ResultSet>) response.getResults().values().iterator();
    if (itr.hasNext()) {
      resultset = itr.next();
    } else {
      throw new SchemaException("Failed to get schema from Teradata - empty schema resultset");
    }

    JsonArray fieldJsonArray = new JsonArray();
    try {
        Schema schema = new Schema();
        ResultSetMetaData rsmd = resultset.getMetaData();

        String columnName, columnTypeName;
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
          columnName = rsmd.getColumnName(i);
          columnTypeName = rsmd.getColumnTypeName(i);

          schema.setColumnName(columnName);

          List<String> mapSymbols = null;
          JsonObject newDataType = this.convertDataType(columnName, columnTypeName, ELEMENT_DATA_TYPE, mapSymbols);

          schema.setDataType(newDataType);
          schema.setLength(rsmd.getColumnDisplaySize(i));
          schema.setPrecision(rsmd.getPrecision(i));
          schema.setScale(rsmd.getScale(i));
          schema.setNullable(rsmd.isNullable(i) == ResultSetMetaData.columnNullable);
          schema.setComment(rsmd.getColumnLabel(i));

          String jsonStr = gson.toJson(schema);
          JsonObject obj = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
          fieldJsonArray.add(obj);
        }

    } catch (Exception e) {
      throw new SchemaException("Failed to get schema from Teradaa; error - " + e.getMessage(), e);
    }

    return fieldJsonArray;
  }

}
