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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
 * OracleExtractor extracts data from Oracle database.
 * It inherits reuses many codes from JdbcExtractor. JdbcExtractor retrieves data via getRecordSet,
 * where watermark is put into where clause in SELECT statement. As SQL statements and metadata store is differ among RDMSs,
 * Oracle extractor provides specific query statement for extracting metadata (in getSchemaMetadata),
 * extracting data (in getDataMetadata), count (in getCountMetadata), and provides data type mapping for JdbcExtractor.
 */
public class OracleExtractor extends JdbcExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(OracleExtractor.class);

  public static final String ORCL_SID = "source.oracle.sid";
  private static final String ORCL_HOUR_FORMAT = "HH";
  private static final String ORCL_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";

  private static final String SAMPLE_QUERY_KEYWORD = " rownum ";
  private static final String URL_FORMAT = "jdbc:oracle:thin:@%s:%d:%s"; //jdbc:oracle:thin:@[HOST][:PORT]:SID
  private static final String METADATA_SCHEMA_PSTMT_FORMAT =
      "SELECT " +
        "column_name, " +
        "UPPER(data_type), " +
        "data_length as length, " +
        "NVL(data_precision, 0) as precesion, " +
        "NVL(data_scale, 0) as scale, " +
        "CASE NVL(NULLABLE, 'Y') WHEN 'Y' THEN 'true' ELSE 'false' END as nullable, " +
        "' ' as format, " +
        "NVL(comments, ' ') as \"COMMENT\", " +
        "column_id " +
        "FROM " +
        "all_tab_columns " +
        "JOIN all_col_comments USING (owner, table_name, column_name) " +
        "WHERE UPPER(owner) = ? " +
        "AND UPPER(table_name) = ? " +
        "ORDER BY " +
        "column_id, column_name";

  private static final Map<String, String> DATA_TYPE_MAP;
  static {
    DATA_TYPE_MAP = ImmutableMap.<String, String>builder()
        .put("VARCHAR2", "string")
        .put("NVARCHAR2", "string")
        .put("NUMBER", "double")
        .put("LONG", "long")
        .put("DATE", "date")
        .put("CHAR", "string")
        .put("NCHAR", "string")
        .put("CLOB", "string")
        .put("NCLOB", "string")
        .put("TIMESTAMP", "timestamp")
        .put("TIMESTAMP(0)", "timestamp")
        .put("TIMESTAMP(1)", "timestamp")
        .put("TIMESTAMP(2)", "timestamp")
        .put("TIMESTAMP(3)", "timestamp")
        .put("TIMESTAMP(4)", "timestamp")
        .put("TIMESTAMP(5)", "timestamp")
        .put("TIMESTAMP(6)", "timestamp")
        .put("TIMESTAMP(7)", "timestamp")
        .put("TIMESTAMP(8)", "timestamp")
        .put("TIMESTAMP(9)", "timestamp")
        .build();
  }

  public OracleExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public List<Command> getSchemaMetadata(String schema, String entity) throws SchemaException {
    LOG.debug("Build query to get schema");

    List<Command> commands = Lists.newArrayList();
    String pstmt = String.format(METADATA_SCHEMA_PSTMT_FORMAT,
                                 Preconditions.checkNotNull(schema),
                                 Preconditions.checkNotNull(entity));
    commands.add(JdbcExtractor.getCommand(pstmt, JdbcCommand.JdbcCommandType.QUERY));
    commands.add(JdbcExtractor.getCommand(Arrays.asList(schema, entity), JdbcCommand.JdbcCommandType.QUERYPARAMS));
    return commands;
  }

  @Override
  public List<Command> getHighWatermarkMetadata(String schema, String entity, String watermarkColumn,
      List<Predicate> predicateList)
      throws HighWatermarkException {
    LOG.debug("Build query to get high watermark");
    List<Command> commands = Lists.newArrayList();

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
    List<Command> commands = Lists.newArrayList();

    String columnProjection = "COUNT(1)";
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
  public List<Command> getDataMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws DataRecordException {
    LOG.debug("Build query to extract data");
    List<Command> commands = Lists.newArrayList();

    String watermarkFilter = this.concatPredicates(predicateList);
    String query = this.getExtractSql();
    if (StringUtils.isBlank(watermarkFilter)) {
      watermarkFilter = "1=1";
    }

    query = query.replace(ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL, watermarkFilter);
    commands.add(JdbcExtractor.getCommand(query, JdbcCommand.JdbcCommandType.QUERY));

    int fetchSize = workUnit.getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_FETCH_SIZE, ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_JDBC_RESULTSET_FETCH_SIZE);
    commands.add(JdbcExtractor.getCommand(fetchSize, JdbcCommand.JdbcCommandType.FETCHSIZE));
    return commands;
  }

  @Override
  public Map<String, String> getDataTypeMap() {
    return DATA_TYPE_MAP;
  }

  @Override
  public Iterator<JsonElement> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList) throws IOException {
    //No need specific retrieval as getRecordSet in JdbcExtractor is sufficient.
    throw new UnsupportedOperationException("getRecordSetFromSourceApi is not supported.");
  }

  @Override
  public String getConnectionUrl() {
    return String.format(URL_FORMAT, this.workUnit.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME).trim(),
                                     this.workUnit.getPropAsInt(ConfigurationKeys.SOURCE_CONN_PORT),
                                     this.workUnit.getProp(ORCL_SID).trim());
  }

  @Override
  public long exractSampleRecordCountFromQuery(String query) {
    if (StringUtils.isEmpty(query)) {
      return -1;
    }

    String inputQuery = query.toLowerCase();
    int idx = inputQuery.indexOf(SAMPLE_QUERY_KEYWORD);
    if (idx < 0) {
      return -1;
    }

    StringBuilder sb = new StringBuilder();
    idx += SAMPLE_QUERY_KEYWORD.length();
    for(; idx < inputQuery.length(); idx++) {
      char c = inputQuery.charAt(idx);
      if(c >= '0' && c <= '9') {
        sb.append(c);
      }
    }

    String sampleNumber = sb.toString();
    if(StringUtils.isEmpty(sampleNumber)) {
      LOG.warn("was not able to extract sample number on the query: " + query);
      return -1;
    }
    return Long.parseLong(sampleNumber);

  }

  /**
   * Purpose of this method is to remove sample clause like limit (from MySQL) or top (from SQL server)
   * so that it can parse the query properly. As sample clause in Oracle is in where clause, it is not needed
   * to remove it as it won't break any parsing process. (Removing condition from where clause is not simple and
   * it's better to avoid when possible)
   *
   * {@inheritDoc}
   * @see gobblin.source.extractor.extract.jdbc.JdbcSpecificLayer#removeSampleClauseFromQuery(java.lang.String)
   */
  @Override
  public String removeSampleClauseFromQuery(String query) {
    return query;
  }

  /**
   * Purpose of this method is to add back sample clause. As it has not been removed from the first place
   * ( @see removeSampleClauseFromQuery ), it does not need to add back. (Adding back condition to where clause is not simple and
   * it's better to avoid when possible)
   * {@inheritDoc}
   * @see gobblin.source.extractor.extract.jdbc.JdbcSpecificLayer#constructSampleClause()
   */
  @Override
  public String constructSampleClause() {
    return "";
  }

  @Override
  public String getWatermarkSourceFormat(WatermarkType watermarkType) {
    String columnFormat = null;
    switch (watermarkType) {
      case TIMESTAMP:
        columnFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS"; //Duplicate as DATE to make Findbugs happy
        break;
      case DATE:
        columnFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS";
        break;
      default:
        LOG.warn("Watermark type " + watermarkType.toString() + " is not supported");
    }
    return columnFormat;
  }

  @Override
  public String getHourPredicateCondition(String column, long value, String valueFormat, String operator) {
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, ORCL_HOUR_FORMAT);
    return Utils.getCoalesceColumnNames(column) + " " + operator + " '" + formattedvalue + "'";
  }

  /**
   * Reuse getTimestampPredicateCondition
   * {@inheritDoc}
   * @see gobblin.source.extractor.extract.ProtocolSpecificLayer#getDatePredicateCondition(java.lang.String, long, java.lang.String, java.lang.String)
   */
  @Override
  public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
    return getTimestampPredicateCondition(column, value, valueFormat, operator);
  }

  /**
   * Oracle timestamp can go up to 9 digit precision. Existing behavior of Gobblin on extractor is to support
   * up to second and Oracle extractor will keep the same behavior.
   *
   * (In order to support 9 digit precision, many changes are required as SimpleDateFormat, widely used in Gobblin,
   * only supports up to millisecond.)
   *
   * {@inheritDoc}
   * @see gobblin.source.extractor.extract.ProtocolSpecificLayer#getTimestampPredicateCondition(java.lang.String, long, java.lang.String, java.lang.String)
   */
  @Override
  public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, ORCL_TIMESTAMP_FORMAT);
    return "CAST(" + Utils.getCoalesceColumnNames(column) + "  AS TIMESTAMP(0)) " //Support up to second.
        + operator + " "
        + "TO_TIMESTAMP('" + formattedvalue + "', 'YYYY-MM-DD HH24:MI:SS')";
  }
}
