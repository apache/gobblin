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
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.password.PasswordManager;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.exception.HighWatermarkException;
import gobblin.source.extractor.exception.RecordCountException;
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.extract.CommandOutput;
import gobblin.source.extractor.extract.QueryBasedExtractor;
import gobblin.source.extractor.extract.SourceSpecificLayer;
import gobblin.source.jdbc.JdbcCommand.JdbcCommandType;
import gobblin.source.extractor.resultset.RecordSetList;
import gobblin.source.extractor.schema.ColumnAttributes;
import gobblin.source.extractor.schema.ColumnNameCase;
import gobblin.source.extractor.schema.Schema;
import gobblin.source.extractor.utils.Utils;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.extractor.watermark.WatermarkType;
import gobblin.source.workunit.WorkUnit;


/**
 * Extract data using JDBC protocol
 *
 * @author nveeramr
 */
public abstract class JdbcExtractor extends QueryBasedExtractor<JsonArray, JsonElement>
    implements SourceSpecificLayer<JsonArray, JsonElement>, JdbcSpecificLayer {
  private static final Gson gson = new Gson();
  private List<String> headerRecord;
  private boolean firstPull = true;
  private CommandOutput<?, ?> dataResponse = null;
  protected String extractSql;
  protected long sampleRecordCount;
  protected JdbcProvider jdbcSource;
  protected Connection dataConnection;
  protected int timeOut;
  private List<ColumnAttributes> columnAliasMap = new ArrayList<>();
  private Map<String, Schema> metadataColumnMap = new HashMap<>();
  private List<String> metadataColumnList = new ArrayList<>();
  private String inputColumnProjection;
  private String outputColumnProjection;
  private long totalRecordCount = 0;
  private boolean nextRecord = true;
  private int unknownColumnCounter = 1;
  protected boolean enableDelimitedIdentifier = false;

  private Logger log = LoggerFactory.getLogger(JdbcExtractor.class);

  /**
   * Metadata column mapping to lookup columns specified in input query
   *
   * @return metadata(schema) column mapping
   */
  public Map<String, Schema> getMetadataColumnMap() {
    return this.metadataColumnMap;
  }

  /**
   * @param metadataColumnMap metadata column mapping
   */
  public void setMetadataColumnMap(Map<String, Schema> metadataColumnMap) {
    this.metadataColumnMap = metadataColumnMap;
  }

  /**
   * Metadata column list
   *
   * @return metadata(schema) column list
   */
  public List<String> getMetadataColumnList() {
    return this.metadataColumnList;
  }

  /**
   * @param metadataColumnList metadata column list
   */
  public void setMetadataColumnList(List<String> metadataColumnList) {
    this.metadataColumnList = metadataColumnList;
  }

  /**
   * Sample Records specified in input query
   *
   * @return sample record count
   */
  public long getSampleRecordCount() {
    return this.sampleRecordCount;
  }

  /**
   * @param sampleRecordCount sample record count
   */
  public void setSampleRecordCount(long sampleRecordCount) {
    this.sampleRecordCount = sampleRecordCount;
  }

  /**
   * query to extract data from data source
   *
   * @return query
   */
  public String getExtractSql() {
    return this.extractSql;
  }

  /**
   * @param extractSql extract query
   */
  public void setExtractSql(String extractSql) {
    this.extractSql = extractSql;
  }

  /**
   * output column projection with aliases specified in input sql
   *
   * @return column projection
   */
  public String getOutputColumnProjection() {
    return this.outputColumnProjection;
  }

  /**
   * @param outputColumnProjection output column projection
   */
  public void setOutputColumnProjection(String outputColumnProjection) {
    this.outputColumnProjection = outputColumnProjection;
  }

  /**
   * input column projection with source columns specified in input sql
   *
   * @return column projection
   */
  public String getInputColumnProjection() {
    return this.inputColumnProjection;
  }

  /**
   * @param inputColumnProjection input column projection
   */
  public void setInputColumnProjection(String inputColumnProjection) {
    this.inputColumnProjection = inputColumnProjection;
  }

  /**
   * source column and alias mapping
   *
   * @return map of column name and alias name
   */
  public List<ColumnAttributes> getColumnAliasMap() {
    return this.columnAliasMap;
  }

  /**
   * add column and alias mapping
   *
   * @param columnAliasMap column alias mapping
   */
  public void addToColumnAliasMap(ColumnAttributes columnAliasMap) {
    this.columnAliasMap.add(columnAliasMap);
  }

  /**
   * check whether is first pull or not
   *
   * @return true, for the first run and it will be set to false after the
   *         first run
   */
  public boolean isFirstPull() {
    return this.firstPull;
  }

  /**
   * @param firstPull
   */
  public void setFirstPull(boolean firstPull) {
    this.firstPull = firstPull;
  }

  /**
   * Header record to convert csv to json
   *
   * @return header record with list of columns
   */
  protected List<String> getHeaderRecord() {
    return this.headerRecord;
  }

  /**
   * @param headerRecord list of column names
   */
  protected void setHeaderRecord(List<String> headerRecord) {
    this.headerRecord = headerRecord;
  }

  /**
   * @return connection timeout
   */
  public int getTimeOut() {
    return this.timeOut;
  }

  /**
   * @return true, if records available. Otherwise, false
   */
  public boolean hasNextRecord() {
    return this.nextRecord;
  }

  /**
   * @param nextRecord next Record
   */
  public void setNextRecord(boolean nextRecord) {
    this.nextRecord = nextRecord;
  }

  /**
   * @param timeOut connection timeout
   */
  @Override
  public void setTimeOut(int timeOut) {
    this.timeOut = timeOut;
  }

  /**
   * @return private static final Gson factory
   */
  public Gson getGson() {
    return this.gson;
  }

  public JdbcExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public void extractMetadata(String schema, String entity, WorkUnit workUnit) throws SchemaException, IOException {
    this.log.info("Extract metadata using JDBC");
    String inputQuery = workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_QUERY);
    if (hasJoinOperation(inputQuery)) {
      throw new RuntimeException("Query across multiple tables not supported");
    }

    String watermarkColumn = workUnitState.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
    this.enableDelimitedIdentifier = workUnitState.getPropAsBoolean(
        ConfigurationKeys.ENABLE_DELIMITED_IDENTIFIER, ConfigurationKeys.DEFAULT_ENABLE_DELIMITED_IDENTIFIER);
    JsonObject defaultWatermark = this.getDefaultWatermark();
    String derivedWatermarkColumnName = defaultWatermark.get("columnName").getAsString();
    this.setSampleRecordCount(this.exractSampleRecordCountFromQuery(inputQuery));
    inputQuery = this.removeSampleClauseFromQuery(inputQuery);
    JsonArray targetSchema = new JsonArray();
    List<String> headerColumns = new ArrayList<>();

    try {
      List<Command> cmds = this.getSchemaMetadata(schema, entity);

      CommandOutput<?, ?> response = this.executePreparedSql(cmds);

      JsonArray array = this.getSchema(response);

      this.buildMetadataColumnMap(array);
      this.parseInputQuery(inputQuery);
      List<String> sourceColumns = this.getMetadataColumnList();

      for (ColumnAttributes colMap : this.columnAliasMap) {
        String alias = colMap.getAliasName();
        String columnName = colMap.getColumnName();
        String sourceColumnName = colMap.getSourceColumnName();
        if (this.isMetadataColumn(columnName, sourceColumns)) {
          String targetColumnName = this.getTargetColumnName(columnName, alias);
          Schema obj = this.getUpdatedSchemaObject(columnName, alias, targetColumnName);
          String jsonStr = gson.toJson(obj);
          JsonObject jsonObject = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
          targetSchema.add(jsonObject);
          headerColumns.add(targetColumnName);
          sourceColumnName = getLeftDelimitedIdentifier() + sourceColumnName + getRightDelimitedIdentifier();
          this.columnList.add(sourceColumnName);
        }
      }

      if (this.hasMultipleWatermarkColumns(watermarkColumn)) {
        derivedWatermarkColumnName = getLeftDelimitedIdentifier() + derivedWatermarkColumnName + getRightDelimitedIdentifier();
        this.columnList.add(derivedWatermarkColumnName);
        headerColumns.add(derivedWatermarkColumnName);
        targetSchema.add(defaultWatermark);
        this.workUnitState.setProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY, derivedWatermarkColumnName);
      }

      String outputColProjection = Joiner.on(",").useForNull("null").join(this.columnList);
      outputColProjection = outputColProjection.replace(derivedWatermarkColumnName,
          Utils.getCoalesceColumnNames(watermarkColumn) + " AS " + derivedWatermarkColumnName);
      this.setOutputColumnProjection(outputColProjection);
      String extractQuery = this.getExtractQuery(schema, entity, inputQuery);

      this.setHeaderRecord(headerColumns);
      this.setOutputSchema(targetSchema);
      this.setExtractSql(extractQuery);
      // this.workUnit.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY,
      // this.escapeCharsInColumnName(this.workUnit.getProp(ConfigurationKeys.SOURCE_ENTITY),
      // ConfigurationKeys.ESCAPE_CHARS_IN_COLUMN_NAME, "_"));
      this.log.info("Schema:" + targetSchema);
      this.log.info("Extract query: " + this.getExtractSql());
    } catch (RuntimeException | IOException | SchemaException e) {
      throw new SchemaException("Failed to get metadata using JDBC; error - " + e.getMessage(), e);
    }
  }

  /**
   * Build/Format input query in the required format
   *
   * @param schema
   * @param entity
   * @param inputQuery
   * @return formatted extract query
   */
  private String getExtractQuery(String schema, String entity, String inputQuery) {
    String inputColProjection = this.getInputColumnProjection();
    String outputColProjection = this.getOutputColumnProjection();
    String query = inputQuery;
    if (query == null) {
      // if input query is null, build the query from metadata
      query = "SELECT " + outputColProjection + " FROM " + schema + "." + entity;
    } else {
      // replace input column projection with output column projection
      if (StringUtils.isNotBlank(inputColProjection)) {
        query = query.replace(inputColProjection, outputColProjection);
      }
    }

    query = addOptionalWatermarkPredicate(query);
    return query;
  }

  /**
   * @param query
   * @return query with watermark predicate symbol
   */
  protected String addOptionalWatermarkPredicate(String query) {
    String watermarkPredicateSymbol = ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_WATERMARK_PREDICATE_SYMBOL;
    if (!query.contains(watermarkPredicateSymbol)) {
      query = SqlQueryUtils.addPredicate(query, watermarkPredicateSymbol);
    }
    return query;
  }

  /**
   * Update schema of source column Update column name with target column
   * name/alias Update watermark, nullable and primary key flags
   *
   * @param sourceColumnName
   * @param targetColumnName
   * @return schema object of a column
   */
  private Schema getUpdatedSchemaObject(String sourceColumnName, String alias, String targetColumnName) {
    // Check for source column and alias
    Schema obj = this.getMetadataColumnMap().get(sourceColumnName.toLowerCase());
    if (obj == null && alias != null) {
      obj = this.getMetadataColumnMap().get(alias.toLowerCase());
    }

    if (obj == null) {
      obj = getCustomColumnSchema(targetColumnName);
    } else {
      String watermarkColumn = this.workUnitState.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
      String primarykeyColumn = this.workUnitState.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY);
      boolean isMultiColumnWatermark = this.hasMultipleWatermarkColumns(watermarkColumn);

      obj.setColumnName(targetColumnName);
      boolean isWatermarkColumn = this.isWatermarkColumn(watermarkColumn, sourceColumnName);
      if (isWatermarkColumn) {
        this.updateDeltaFieldConfig(sourceColumnName, targetColumnName);
      } else if (alias != null) {
        // Check for alias
        isWatermarkColumn = this.isWatermarkColumn(watermarkColumn, alias);
        this.updateDeltaFieldConfig(alias, targetColumnName);
      }

      // If there is only one watermark column, then consider it as a
      // watermark. Otherwise add a default watermark column in the end
      if (!isMultiColumnWatermark) {
        obj.setWaterMark(isWatermarkColumn);
      }

      // override all columns to nullable except primary key and watermark
      // columns
      if ((isWatermarkColumn && !isMultiColumnWatermark)
          || this.getPrimarykeyIndex(primarykeyColumn, sourceColumnName) > 0) {
        obj.setNullable(false);
      } else {
        obj.setNullable(true);
      }

      // set primary key index for all the primary key fields
      int primarykeyIndex = this.getPrimarykeyIndex(primarykeyColumn, sourceColumnName);
      if (primarykeyIndex > 0 && (!sourceColumnName.equalsIgnoreCase(targetColumnName))) {
        this.updatePrimaryKeyConfig(sourceColumnName, targetColumnName);
      }

      obj.setPrimaryKey(primarykeyIndex);
    }
    return obj;
  }

  /**
   * Get target column name if column is not found in metadata, then name it
   * as unknown column If alias is not found, target column is nothing but
   * source column
   *
   * @param sourceColumnName
   * @param alias
   * @return targetColumnName
   */
  private String getTargetColumnName(String sourceColumnName, String alias) {
    String targetColumnName = alias;
    Schema obj = this.getMetadataColumnMap().get(sourceColumnName.toLowerCase());
    if (obj == null) {
      targetColumnName = (targetColumnName == null ? "unknown" + this.unknownColumnCounter : targetColumnName);
      this.unknownColumnCounter++;
    } else {
      targetColumnName = (StringUtils.isNotBlank(targetColumnName) ? targetColumnName : sourceColumnName);
    }
    targetColumnName = this.toCase(targetColumnName);
    return Utils.escapeSpecialCharacters(targetColumnName, ConfigurationKeys.ESCAPE_CHARS_IN_COLUMN_NAME, "_");
  }

  /**
   * Build metadata column map with column name and column schema object.
   * Build metadata column list with list columns in metadata
   *
   * @param array Schema of all columns
   */
  private void buildMetadataColumnMap(JsonArray array) {
    if (array != null) {
      for (JsonElement columnElement : array) {
        Schema schemaObj = gson.fromJson(columnElement, Schema.class);
        String columnName = schemaObj.getColumnName();
        this.metadataColumnMap.put(columnName.toLowerCase(), schemaObj);
        this.metadataColumnList.add(columnName.toLowerCase());
      }
    }
  }

  /**
   * Update water mark column property if there is an alias defined in query
   *
   * @param srcColumnName source column name
   * @param tgtColumnName target column name
   */
  private void updateDeltaFieldConfig(String srcColumnName, String tgtColumnName) {
    if (this.workUnitState.contains(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY)) {
      String watermarkCol = this.workUnitState.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY);
      this.workUnitState.setProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY,
          watermarkCol.replaceAll(srcColumnName, tgtColumnName));
    }
  }

  /**
   * Update primary key column property if there is an alias defined in query
   *
   * @param srcColumnName source column name
   * @param tgtColumnName target column name
   */
  private void updatePrimaryKeyConfig(String srcColumnName, String tgtColumnName) {
    if (this.workUnitState.contains(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY)) {
      String primarykey = this.workUnitState.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY);
      this.workUnitState.setProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY,
          primarykey.replaceAll(srcColumnName, tgtColumnName));
    }
  }

  /**
   * If input query is null or '*' in the select list, consider all columns.
   *
   * @return true, to select all colums. else, false.
   */
  private boolean isSelectAllColumns() {
    String columnProjection = this.getInputColumnProjection();
    if (columnProjection == null || columnProjection.trim().equals("*") || columnProjection.contains(".*")) {
      return true;
    }
    return false;
  }

  /**
   * Parse query provided in pull file Set input column projection - column
   * projection in the input query Set columnAlias map - column and its alias
   * mentioned in input query
   *
   * @param query input query
   */
  private void parseInputQuery(String query) {
    List<String> projectedColumns = new ArrayList<>();
    if (StringUtils.isNotBlank(query)) {
      String queryLowerCase = query.toLowerCase();
      int startIndex = queryLowerCase.indexOf("select ") + 7;
      int endIndex = queryLowerCase.indexOf(" from ");
      if (startIndex >= 0 && endIndex >= 0) {
        String columnProjection = query.substring(startIndex, endIndex);
        this.setInputColumnProjection(columnProjection);
        // parse the select list
        StringBuffer sb = new StringBuffer();
        int bracketCount = 0;
        for (int i = 0; i < columnProjection.length(); i++) {
          char c = columnProjection.charAt(i);
          if (c == '(') {
            bracketCount++;
          }

          if (c == ')') {
            bracketCount--;
          }

          if (bracketCount != 0) {
            sb.append(c);
          } else {
            if (c != ',') {
              sb.append(c);
            } else {
              projectedColumns.add(sb.toString());
              sb = new StringBuffer();
            }
          }
        }
        projectedColumns.add(sb.toString());
      }
    }

    if (this.isSelectAllColumns()) {
      List<String> columnList = this.getMetadataColumnList();
      for (String columnName : columnList) {
        ColumnAttributes col = new ColumnAttributes();
        col.setColumnName(columnName);
        col.setAliasName(columnName);
        col.setSourceColumnName(columnName);
        this.addToColumnAliasMap(col);
      }
    } else {
      for (String projectedColumn : projectedColumns) {
        String column = projectedColumn.trim();
        String alias = null;
        String sourceColumn = column;
        int spaceOccurences = StringUtils.countMatches(column.trim(), " ");
        if (spaceOccurences > 0) {
          // separate column and alias if they are separated by "as"
          // or space
          int lastSpaceIndex = column.toLowerCase().lastIndexOf(" as ");
          sourceColumn = column.substring(0, lastSpaceIndex);
          alias = column.substring(lastSpaceIndex + 4);
        }

        // extract column name if projection has table name in it
        String columnName = sourceColumn;
        if (sourceColumn.contains(".")) {
          columnName = sourceColumn.substring(sourceColumn.indexOf(".") + 1);
        }

        ColumnAttributes col = new ColumnAttributes();
        col.setColumnName(columnName);
        col.setAliasName(alias);
        col.setSourceColumnName(sourceColumn);
        this.addToColumnAliasMap(col);
      }
    }
  }

  /**
   * Execute query using JDBC simple Statement Set fetch size
   *
   * @param cmds commands - query, fetch size
   * @return JDBC ResultSet
   * @throws Exception
   */
  private CommandOutput<?, ?> executeSql(List<Command> cmds) {
    String query = null;
    int fetchSize = 0;

    for (Command cmd : cmds) {
      if (cmd instanceof JdbcCommand) {
        JdbcCommandType type = (JdbcCommandType) cmd.getCommandType();
        switch (type) {
          case QUERY:
            query = cmd.getParams().get(0);
            break;
          case FETCHSIZE:
            fetchSize = Integer.parseInt(cmd.getParams().get(0));
            break;
          default:
            this.log.error("Command " + type.toString() + " not recognized");
            break;
        }
      }
    }

    this.log.info("Executing query:" + query);
    ResultSet resultSet = null;
    try {
      this.jdbcSource = createJdbcSource();
      this.dataConnection = this.jdbcSource.getConnection();
      Statement statement = this.dataConnection.createStatement();

      if (fetchSize != 0 && this.getExpectedRecordCount() > 2000) {
        statement.setFetchSize(fetchSize);
      }
      final boolean status = statement.execute(query);
      if (status == false) {
        this.log.error("Failed to execute sql:" + query);
      }
      resultSet = statement.getResultSet();
    } catch (Exception e) {
      this.log.error("Failed to execute sql:" + query + " ;error-" + e.getMessage(), e);
    }

    CommandOutput<JdbcCommand, ResultSet> output = new JdbcCommandOutput();
    output.put((JdbcCommand) cmds.get(0), resultSet);
    return output;
  }

  /**
   * Execute query using JDBC PreparedStatement to pass query parameters Set
   * fetch size
   *
   * @param cmds commands - query, fetch size, query parameters
   * @return JDBC ResultSet
   * @throws Exception
   */
  private CommandOutput<?, ?> executePreparedSql(List<Command> cmds) {
    String query = null;
    List<String> queryParameters = null;
    int fetchSize = 0;

    for (Command cmd : cmds) {
      if (cmd instanceof JdbcCommand) {
        JdbcCommandType type = (JdbcCommandType) cmd.getCommandType();
        switch (type) {
          case QUERY:
            query = cmd.getParams().get(0);
            break;
          case QUERYPARAMS:
            queryParameters = cmd.getParams();
            break;
          case FETCHSIZE:
            fetchSize = Integer.parseInt(cmd.getParams().get(0));
            break;
          default:
            this.log.error("Command " + type.toString() + " not recognized");
            break;
        }
      }
    }

    this.log.info("Executing query:" + query);
    ResultSet resultSet = null;
    try {
      this.jdbcSource = createJdbcSource();
      this.dataConnection = this.jdbcSource.getConnection();

      PreparedStatement statement =
          this.dataConnection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

      int parameterPosition = 1;
      if (queryParameters != null && queryParameters.size() > 0) {
        for (String parameter : queryParameters) {
          statement.setString(parameterPosition, parameter);
          parameterPosition++;
        }
      }
      if (fetchSize != 0) {
        statement.setFetchSize(fetchSize);
      }
      final boolean status = statement.execute();
      if (status == false) {
        this.log.error("Failed to execute sql:" + query);
      }
      resultSet = statement.getResultSet();

    } catch (Exception e) {
      this.log.error("Failed to execute sql:" + query + " ;error-" + e.getMessage(), e);
    }

    CommandOutput<JdbcCommand, ResultSet> output = new JdbcCommandOutput();
    output.put((JdbcCommand) cmds.get(0), resultSet);
    return output;
  }

  /**
   * Create JDBC source to get connection
   *
   * @return JDBCSource
   */
  protected JdbcProvider createJdbcSource() {
    String driver = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_DRIVER);
    String userName = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME);
    String password = PasswordManager.getInstance(this.workUnitState)
        .readPassword(this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD));
    String connectionUrl = this.getConnectionUrl();

    String proxyHost = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL);
    int proxyPort = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT) != null
        ? this.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT) : -1;

    if (this.jdbcSource == null || this.jdbcSource.isClosed()) {
      this.jdbcSource = new JdbcProvider(driver, connectionUrl, userName, password, 1, this.getTimeOut(), "DEFAULT",
          proxyHost, proxyPort);
      return this.jdbcSource;
    } else {
      return this.jdbcSource;
    }
  }

  @Override
  public long getMaxWatermark(String schema, String entity, String watermarkColumn, List<Predicate> predicateList,
      String watermarkSourceFormat) throws HighWatermarkException {
    this.log.info("Get high watermark using JDBC");
    long calculatedHighWatermark = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;

    try {
      List<Command> cmds = this.getHighWatermarkMetadata(schema, entity, watermarkColumn, predicateList);
      CommandOutput<?, ?> response = this.executeSql(cmds);
      calculatedHighWatermark = this.getHighWatermark(response, watermarkColumn, watermarkSourceFormat);
      return calculatedHighWatermark;
    } catch (Exception e) {
      throw new HighWatermarkException("Failed to get high watermark using JDBC; error - " + e.getMessage(), e);
    }
  }

  @Override
  public long getSourceCount(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws RecordCountException {
    this.log.info("Get source record count using JDBC");
    long count = 0;
    try {
      List<Command> cmds = this.getCountMetadata(schema, entity, workUnit, predicateList);
      CommandOutput<?, ?> response = this.executeSql(cmds);
      count = this.getCount(response);
      this.log.info("Source record count:" + count);
      return count;
    } catch (Exception e) {
      throw new RecordCountException("Failed to get source record count using JDBC; error - " + e.getMessage(), e);
    }
  }

  @Override
  public Iterator<JsonElement> getRecordSet(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList) throws DataRecordException, IOException {
    Iterator<JsonElement> rs = null;
    List<Command> cmds;
    try {
      if (isFirstPull()) {
        this.log.info("Get data recordset using JDBC");
        cmds = this.getDataMetadata(schema, entity, workUnit, predicateList);
        this.dataResponse = this.executePreparedSql(cmds);
        this.setFirstPull(false);
      }

      rs = this.getData(this.dataResponse);
      return rs;
    } catch (Exception e) {
      throw new DataRecordException("Failed to get record set using JDBC; error - " + e.getMessage(), e);
    }
  }

  @Override
  public JsonArray getSchema(CommandOutput<?, ?> response) throws SchemaException, IOException {
    this.log.debug("Extract schema from resultset");
    ResultSet resultset = null;
    Iterator<ResultSet> itr = (Iterator<ResultSet>) response.getResults().values().iterator();
    if (itr.hasNext()) {
      resultset = itr.next();
    } else {
      throw new SchemaException("Failed to get schema from database - Resultset has no records");
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
        schema.setNullable(resultset.getBoolean(6));
        schema.setFormat(resultset.getString(7));
        schema.setComment(resultset.getString(8));
        schema.setDefaultValue(null);
        schema.setUnique(false);

        String jsonStr = gson.toJson(schema);
        JsonObject obj = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
        fieldJsonArray.add(obj);
      }
    } catch (Exception e) {
      throw new SchemaException("Failed to get schema from database; error - " + e.getMessage(), e);
    }

    return fieldJsonArray;
  }

  @Override
  public long getHighWatermark(CommandOutput<?, ?> response, String watermarkColumn, String watermarkColumnFormat)
      throws HighWatermarkException {
    this.log.debug("Extract high watermark from resultset");
    ResultSet resultset = null;
    Iterator<ResultSet> itr = (Iterator<ResultSet>) response.getResults().values().iterator();
    if (itr.hasNext()) {
      resultset = itr.next();
    } else {
      throw new HighWatermarkException("Failed to get high watermark from database - Resultset has no records");
    }

    Long HighWatermark;
    try {
      String watermark;
      if (resultset.next()) {
        watermark = resultset.getString(1);
      } else {
        watermark = null;
      }

      if (watermark == null) {
        return ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
      }

      if (watermarkColumnFormat != null) {
        SimpleDateFormat inFormat = new SimpleDateFormat(watermarkColumnFormat);
        Date date = null;
        try {
          date = inFormat.parse(watermark);
        } catch (ParseException e) {
          this.log.error("ParseException: " + e.getMessage(), e);
        }
        SimpleDateFormat outFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        HighWatermark = Long.parseLong(outFormat.format(date));
      } else {
        HighWatermark = Long.parseLong(watermark);
      }
    } catch (Exception e) {
      throw new HighWatermarkException("Failed to get high watermark from database; error - " + e.getMessage(), e);
    }

    return HighWatermark;
  }

  @Override
  public long getCount(CommandOutput<?, ?> response) throws RecordCountException {
    this.log.debug("Extract source record count from resultset");
    ResultSet resultset = null;
    long count = 0;
    Iterator<ResultSet> itr = (Iterator<ResultSet>) response.getResults().values().iterator();
    if (itr.hasNext()) {
      resultset = itr.next();

      try {
        if (resultset.next()) {
          count = resultset.getLong(1);
        }
      } catch (Exception e) {
        throw new RecordCountException("Failed to get source record count from database; error - " + e.getMessage(), e);
      }
    } else {
      throw new RuntimeException("Failed to get source record count from database - Resultset has no records");
    }

    return count;
  }

  @Override
  public Iterator<JsonElement> getData(CommandOutput<?, ?> response) throws DataRecordException, IOException {
    this.log.debug("Extract data records from resultset");

    RecordSetList<JsonElement> recordSet = this.getNewRecordSetList();

    if (response == null || !this.hasNextRecord()) {
      return recordSet.iterator();
    }

    ResultSet resultset = null;
    Iterator<ResultSet> itr = (Iterator<ResultSet>) response.getResults().values().iterator();
    if (itr.hasNext()) {
      resultset = itr.next();
    } else {
      throw new DataRecordException("Failed to get source record count from database - Resultset has no records");
    }

    try {
      final ResultSetMetaData resultsetMetadata = resultset.getMetaData();

      int batchSize = this.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_QUERYBASED_FETCH_SIZE, 0);
      batchSize = (batchSize == 0 ? ConfigurationKeys.DEFAULT_SOURCE_FETCH_SIZE : batchSize);

      int recordCount = 0;
      while (resultset.next()) {

        final int numColumns = resultsetMetadata.getColumnCount();
        JsonObject jsonObject = new JsonObject();

        for (int i = 1; i < numColumns + 1; i++) {
          final String columnName = this.getHeaderRecord().get(i - 1);
          jsonObject.addProperty(columnName, parseColumnAsString(resultset, resultsetMetadata, i));

        }

        recordSet.add(jsonObject);

        recordCount++;
        this.totalRecordCount++;

        // Insert records in record set until it reaches the batch size
        if (recordCount >= batchSize) {
          this.log.info("Total number of records processed so far: " + this.totalRecordCount);
          return recordSet.iterator();
        }
      }
      this.setNextRecord(false);
      this.log.info("Total number of records processed so far: " + this.totalRecordCount);
      return recordSet.iterator();
    } catch (Exception e) {
      throw new DataRecordException("Failed to get records from database; error - " + e.getMessage(), e);
    }
  }

  /*
   * For Blob data, need to get the bytes and use base64 encoding to encode the byte[]
   * When reading from the String, need to use base64 decoder
   *     String tmp = ... ( get the String value )
   *     byte[] foo = Base64.decodeBase64(tmp);
   */
  private String readBlobAsString(Blob logBlob) throws SQLException {
    if (logBlob == null) {
      return StringUtils.EMPTY;
    }

    byte[] ba = logBlob.getBytes(1L, (int) (logBlob.length()));

    if (ba == null) {
      return StringUtils.EMPTY;
    }
    String baString = Base64.encodeBase64String(ba);
    return baString;
  }

  /**
   * HACK: there is a bug in the MysqlExtractor where tinyint columns are always treated as ints.
   * There are MySQL jdbc driver setting (tinyInt1isBit=true and transformedBitIsBoolean=false) that
   * can cause tinyint(1) columns to be treated as BIT/BOOLEAN columns. The default behavior is to
   * treat tinyint(1) as BIT.
   *
   * Currently, {@link MysqlExtractor#getDataTypeMap()} uses the information_schema to check types.
   * That does not do the above conversion. {@link #parseColumnAsString(ResultSet, ResultSetMetaData, int)}
   * which does the above type mapping.
   *
   * On the other hand, SqlServerExtractor treats BIT columns as Booleans. So we can be in a bind
   * where sometimes BIT has to be converted to an int (for backwards compatibility in MySQL) and
   * sometimes to a Boolean (for SqlServer).
   *
   * This function adds configurable behavior depending on the Extractor type.
   **/
  protected boolean convertBitToBoolean() {
    return true;
  }

  private String parseColumnAsString(final ResultSet resultset, final ResultSetMetaData resultsetMetadata, int i)
      throws SQLException {

    if (isBlob(resultsetMetadata.getColumnType(i))) {
      return readBlobAsString(resultset.getBlob(i));
    }
    if ((resultsetMetadata.getColumnType(i) == Types.BIT
         || resultsetMetadata.getColumnType(i) == Types.BOOLEAN)
        && convertBitToBoolean()) {
      return Boolean.toString(resultset.getBoolean(i));
    }
    return resultset.getString(i);
  }

  private static boolean isBlob(int columnType) {
    return columnType == Types.LONGVARBINARY || columnType == Types.BINARY;
  }

  protected static Command getCommand(String query, JdbcCommandType commandType) {
    return new JdbcCommand().build(Arrays.asList(query), commandType);
  }

  protected static Command getCommand(int fetchSize, JdbcCommandType commandType) {
    return new JdbcCommand().build(Arrays.asList(Integer.toString(fetchSize)), commandType);
  }

  protected static Command getCommand(List<String> params, JdbcCommandType commandType) {
    return new JdbcCommand().build(params, commandType);
  }

  /**
   * Concatenate all predicates with "and" clause
   *
   * @param predicateList list of predicate(filter) conditions
   * @return predicate
   */
  protected String concatPredicates(List<Predicate> predicateList) {
    List<String> conditions = new ArrayList<>();
    for (Predicate predicate : predicateList) {
      conditions.add(predicate.getCondition());
    }
    return Joiner.on(" and ").skipNulls().join(conditions);
  }

  /**
   * Schema of default watermark column-required if there are multiple watermarks
   *
   * @return column schema
   */
  private JsonObject getDefaultWatermark() {
    Schema schema = new Schema();
    String dataType;
    String columnName = "derivedwatermarkcolumn";

    schema.setColumnName(columnName);

    WatermarkType wmType = WatermarkType.valueOf(
        this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_WATERMARK_TYPE, "TIMESTAMP").toUpperCase());
    switch (wmType) {
      case TIMESTAMP:
        dataType = "timestamp";
        break;
      case DATE:
        dataType = "date";
        break;
      default:
        dataType = "int";
        break;
    }

    String elementDataType = "string";
    List<String> mapSymbols = null;
    JsonObject newDataType = this.convertDataType(columnName, dataType, elementDataType, mapSymbols);
    schema.setDataType(newDataType);
    schema.setWaterMark(true);
    schema.setPrimaryKey(0);
    schema.setLength(0);
    schema.setPrecision(0);
    schema.setScale(0);
    schema.setNullable(false);
    schema.setFormat(null);
    schema.setComment("Default watermark column");
    schema.setDefaultValue(null);
    schema.setUnique(false);

    String jsonStr = gson.toJson(schema);
    JsonObject obj = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
    return obj;
  }

  /**
   * Schema of a custom column - required if column not found in metadata
   *
   * @return column schema
   */
  private Schema getCustomColumnSchema(String columnName) {
    Schema schema = new Schema();
    String dataType = "string";
    schema.setColumnName(columnName);
    String elementDataType = "string";
    List<String> mapSymbols = null;
    JsonObject newDataType = this.convertDataType(columnName, dataType, elementDataType, mapSymbols);
    schema.setDataType(newDataType);
    schema.setWaterMark(false);
    schema.setPrimaryKey(0);
    schema.setLength(0);
    schema.setPrecision(0);
    schema.setScale(0);
    schema.setNullable(true);
    schema.setFormat(null);
    schema.setComment("Custom column");
    schema.setDefaultValue(null);
    schema.setUnique(false);
    return schema;
  }

  /**
   * Check if the SELECT query has join operation
   */
  public static boolean hasJoinOperation(String selectQuery) {
    if (selectQuery == null || selectQuery.length() == 0) {
      return false;
    }

    SqlParser sqlParser = SqlParser.create(selectQuery);
    try {
      SqlSelect sqlSelect = (SqlSelect)sqlParser.parseQuery();
      SqlNode node = sqlSelect.getFrom();
      return node.getKind() == SqlKind.JOIN;
    } catch (SqlParseException e) {
      return false;
    }
  }

  /**
   * New record set for iterator
   *
   * @return RecordSetList
   */
  private static RecordSetList<JsonElement> getNewRecordSetList() {
    return new RecordSetList<>();
  }

  /**
   * Change the column name case to upper, lower or nochange; Default nochange
   *
   * @return column name with the required case
   */
  private String toCase(String targetColumnName) {
    String columnName = targetColumnName;
    ColumnNameCase caseType = ColumnNameCase.valueOf(this.workUnitState
        .getProp(ConfigurationKeys.SOURCE_COLUMN_NAME_CASE, ConfigurationKeys.DEFAULT_COLUMN_NAME_CASE).toUpperCase());
    switch (caseType) {
      case TOUPPER:
        columnName = targetColumnName.toUpperCase();
        break;
      case TOLOWER:
        columnName = targetColumnName.toLowerCase();
        break;
      default:
        columnName = targetColumnName;
        break;
    }
    return columnName;
  }

  /**
   * Default DelimitedIdentifier is 'double quotes',
   * but that would make the column name case sensitive in some of the systems, e.g. Oracle.
   * Queries may fail if
   * (1) enableDelimitedIdentifier is true, and
   * (2) Queried system is case sensitive when using double quotes as delimited identifier, and
   * (3) Intended column name does not match the column name in the schema including case.
   *
   * @return leftDelimitedIdentifier
   */

  public String getLeftDelimitedIdentifier() {
    return this.enableDelimitedIdentifier ? "\"" : "";
  }

  public String getRightDelimitedIdentifier() {
    return this.enableDelimitedIdentifier ? "\"" : "";
  }

  @Override
  public void closeConnection() throws Exception {
    if (this.dataConnection != null) {
      try {
        this.dataConnection.close();
      } catch (SQLException e) {
        this.log.error("Failed to close connection ;error-" + e.getMessage(), e);
      }
    }

    this.jdbcSource.close();
  }
}
