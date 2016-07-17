/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.jdbc;

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
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.sql.ResultSet;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.exception.RecordCountException;
import gobblin.source.extractor.schema.Schema;
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.extract.CommandOutput;
import gobblin.source.workunit.WorkUnit;
import lombok.extern.slf4j.Slf4j;


/**
 * Oracle extractor using JDBC protocol
 *
 * @author bjvanov
 */
@Slf4j
public class OracleExtractor extends JdbcExtractor {
  private static final String TIMESTAMP_FORMAT = "dd-MMM-yy hh.mm.ss.SSSSSSSSS a";
  private static final String DATE_FORMAT = "dd-MMM-yy";
  private static final String HOUR_FORMAT = "hh";
  private static final long SAMPLERECORDCOUNT = -1;
  private static final Pattern SAMPLE_CLAUSE_PATTERN = Pattern
    .compile("rownum <=? \\d+ and|where rownum <=? \\d+$|and rownum <=? \\d+");


  public OracleExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public List<Command> getSchemaMetadata(String schema, String entity) throws SchemaException {
    log.debug("Build query to get schema");
    List<Command> commands = new ArrayList<>();
    List<String> queryParams = Arrays.asList(entity, schema);

    String metadataSql = "select " + " col.column_name, " + " col.data_type, "
        + " case when CHAR_LENGTH is null then 0 else 0 end as length, "
        + " case when DATA_PRECISION is null then 0 else DATA_PRECISION end as precesion, "
        + " case when DATA_SCALE is null then 0 else DATA_SCALE end as scale, "
        + " case when NULLABLE='N' then 'false' else 'true' end as nullable, " + " 'null' as \"format\", "
        + " 'null' as \"comment\" " + " from ALL_TAB_COLS col "
        + " WHERE upper(col.table_name)=upper(?) AND upper(col.owner)=upper(?) "
        + " order by col.column_name, col.data_type";

    commands.add(JdbcExtractor.getCommand(metadataSql, JdbcCommand.JdbcCommandType.QUERY));
    commands.add(JdbcExtractor.getCommand(queryParams, JdbcCommand.JdbcCommandType.QUERYPARAMS));
    return commands;
  }

  @Override
  public JsonArray getSchema(CommandOutput<?, ?> response) throws SchemaException, IOException {
    this.log.debug("Extract schema from resultset");
    ResultSet resultset = null;
    Iterator<ResultSet> itr = (Iterator<ResultSet>) response.getResults().values().iterator();
    if (itr.hasNext()) {
      resultset = itr.next();
    } else {
      throw new SchemaException("Failed to get schema from Oracle - Resultset has no records");
    }

    JsonArray fieldJsonArray = new JsonArray();
    try {
      while (resultset.next()) {
        Schema schema = new Schema();
        String columnName = resultset.getString(1);
        schema.setColumnName(columnName);

        String dataType = resultset.getString(2);
        String elementDataType = "string";
        List<String> mapSymbols = null;
        JsonObject newDataType = this.convertDataType(columnName, dataType, elementDataType, mapSymbols);

        schema.setDataType(newDataType);
        schema.setLength(resultset.getLong(3));
        schema.setPrecision(resultset.getInt(4));
        schema.setScale(resultset.getInt(5));
        schema.setNullable(SqlQueryUtils.castToBoolean(resultset.getString(6)));
        schema.setFormat(resultset.getString(7));
        schema.setComment(resultset.getString(8));
        schema.setDefaultValue(null);
        schema.setUnique(false);

        String jsonStr = this.getGson().toJson(schema);
        JsonObject obj = this.getGson().fromJson(jsonStr, JsonObject.class).getAsJsonObject();
        fieldJsonArray.add(obj);
      }
    } catch (Exception e) {
      throw new SchemaException("Failed to get schema from Oracle; error - " + e.getMessage(), e);
    }

    return fieldJsonArray;
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

  // TODO: How to handle variable precision datatypes e.g. NUMBER(6)
  @Override
  public Map<String, String> getDataTypeMap() {
    Map<String, String> dataTypeMap = ImmutableMap.<String, String> builder().put("char", "string")
        .put("varchar2", "string").put("varchar", "string").put("nchar", "string").put("nvarchar2", "string")
        .put("nclob", "string").put("clob", "string").put("long", "string").put("number", "double")
        .put("binary_float", "float").put("binary_double", "double").put("date", "date").put("datetime", "timestamp")
        .put("timestamp", "timestamp").put("timestamp with time zone", "timestamp").put("numeric", "double")
        .put("timezone with local timezone", "timestamp").put("raw", "string").put("long raw", "string")
        .put("rowid", "string").put("urowid", "string").put("xmltype", "string").put("float", "float")
        .put("dec", "double").put("decimal", "double").put("integer", "int").put("int", "int").put("bigint", "long")
        .put("smallint", "int").put("real", "double").put("double precision", "double").put("float", "double")
        .put("interval year", "date").put("interval day", "timestamp").put("timestamp(0)", "timestamp")
        .put("timestamp(1)", "timestamp").put("timestamp(2)", "timestamp").put("timestamp(3)", "timestamp")
        .put("timestamp(4)", "timestamp").put("timestamp(5)", "timestamp").put("timestamp(6)", "timestamp")
        .put("timestamp(7)", "timestamp").put("timestamp(8)", "timestamp").put("timestamp(9)", "timestamp").build();
    return dataTypeMap;
  }

  @Override
  public Iterator<JsonElement> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList) throws IOException {
    return null;
  }

  @Override
  public String getConnectionUrl() {
    String host = this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME);
    String port = this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_PORT);
    String sid = this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_SID).trim();
    String url = "jdbc:oracle:thin:@" + host.trim() + (StringUtils.isEmpty(port) ? "" : ":" + port) + ":" + sid;
    return url;
  }

  @Override
  public long exractSampleRecordCountFromQuery(String query) {
    if (StringUtils.isBlank(query)) {
      return SAMPLERECORDCOUNT;
    }

    long recordcount = SAMPLERECORDCOUNT;
    String inputQuery = query.toLowerCase();

    int limitStartIndex = -1;
    int limitEndIndex = -1;
    Matcher matcher = SAMPLE_CLAUSE_PATTERN.matcher(inputQuery);
    if (matcher.find()) {
      String recordCountString = matcher.group().replaceAll("[\\D]", "");
      try {
        recordcount = Long.parseLong(recordCountString);
      } catch (Exception e) {
        log.error("Ignoring incorrect limit value in input query:" + recordCountString);
      }
    }
    return recordcount;

    // boolean multiPredicate = inputQuery.indexOf(" and ") != -1 ? true : false;
    // boolean leadingLimit = false;
    // if (!multiPredicate) {
    //   limitStartIndex = inputQuery.indexOf(" where rownum <= ");
    // } else {
    //   limitStartIndex = inputQuery.indexOf(" and rownum <= ");
    //   if (limitStartIndex == -1 && inputQuery.indexOf(" where rownum <= ") > -1) {
    //     limitStartIndex = inputQuery.indexOf(" where rownum <= ");
    //     leadingLimit = true;
    //   } 
    // }

    // int limitEndIndex = getLimitEndIndex(inputQuery, limitStartIndex);
    // if (limitStartIndex > 0) {
    //   String limitValue = query.substring(query.indexOf("<=") + 3, limitEndIndex);
    //   try {
    //     recordcount = Long.parseLong(limitValue);
    //   } catch (Exception e) {
    //     log.error("Ignoring incorrct limit value in input query:" + limitValue);
    //   }
    // }
    // return recordcount;
  }

  @Override
  public String removeSampleClauseFromQuery(String query) {
    if (StringUtils.isBlank(query)) {
      return query;
    }

    String outputQuery = query;
    String inputQuery = query.toLowerCase();

    int limitStartIndex = -1;
    int limitEndIndex = -1;
    Matcher matcher = SAMPLE_CLAUSE_PATTERN.matcher(inputQuery);
    if (matcher.find()) {
      limitStartIndex = matcher.start();
      limitEndIndex = matcher.end();
      outputQuery = query.substring(0, limitStartIndex) + " " + query.substring(limitEndIndex);
    }
    return outputQuery.trim().replaceAll(" +", " ");

    // boolean multiPredicate = inputQuery.indexOf(" and ") != -1 ? true : false;
    // boolean leadingLimit = false;
    // if (!multiPredicate) {
    //   limitStartIndex = inputQuery.indexOf(" where rownum <= ");
    // } else {
    //   limitStartIndex = inputQuery.indexOf(" and rownum <= ");
    //   if (limitStartIndex == -1 && inputQuery.indexOf(" where rownum <= ") > -1) {
    //     limitStartIndex = inputQuery.indexOf(" where rownum <= ");
    //     leadingLimit = true;
    //   } 
    // }
    
    
    // int limitEndIndex = getLimitEndIndex(inputQuery, limitStartIndex);
    
    // if ((limitStartIndex > 0 && !multiPredicate) || (limitStartIndex > 0 && multiPredicate && !leadingLimit)) {
    //   outputQuery = query.substring(0, limitStartIndex) + " " + query.substring(limitEndIndex);
    // } else if (limitStartIndex > 0 && multiPredicate && leadingLimit) {
    //   outputQuery = query.substring(0, limitStartIndex + 6) + " " + query.substring(limitEndIndex + 4);
    // }
    // return outputQuery.trim().replaceAll(" +", " ");
  }

  // private static int getLimitEndIndex(String inputQuery, int limitStartIndex) {
  //   int limitEndIndex = -1;
  //   if (limitStartIndex > 0) {
  //     limitEndIndex = inputQuery.charAt(limitStartIndex + 1) == 'w' ? limitStartIndex + 17 : limitStartIndex + 15;
  //     String remainingQuery = inputQuery.substring(limitEndIndex);
  //     boolean numFound = false;

  //     int pos = 0;
  //     while (pos < remainingQuery.length()) {
  //       char ch = remainingQuery.charAt(pos);
  //       if (ch == ' ' && !numFound) {
  //         pos++;
  //         continue;
  //       } else if (numFound && (!Character.isDigit(ch))) {
  //         break;
  //       } else {
  //         numFound = true;
  //       }
  //       pos++;
  //     }
  //     limitEndIndex = limitEndIndex + pos;
  //   }
  //   return limitEndIndex;
  // }

    @Override
  public String constructSampleClause() {
    long sampleRowCount = this.getSampleRecordCount();
    if (sampleRowCount >= 0) {
      return " rownum <= " + sampleRowCount;
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
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, HOUR_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting date predicate for Oracle");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, DATE_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  @Override
  public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.debug("Getting timestamp predicate for Oracle");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, TIMESTAMP_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }
}
