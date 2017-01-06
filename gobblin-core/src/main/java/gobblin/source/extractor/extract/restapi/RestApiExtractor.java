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

package gobblin.source.extractor.extract.restapi;

import com.google.common.collect.ImmutableList;

import gobblin.source.extractor.exception.RestApiConnectionException;
import gobblin.source.extractor.exception.RestApiProcessingException;
import gobblin.source.extractor.utils.Utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.common.base.Splitter;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.exception.HighWatermarkException;
import gobblin.source.extractor.exception.RecordCountException;
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.extractor.extract.QueryBasedExtractor;
import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.extract.CommandOutput;
import gobblin.source.extractor.extract.SourceSpecificLayer;
import gobblin.source.extractor.schema.Schema;
import gobblin.source.workunit.WorkUnit;
import lombok.extern.slf4j.Slf4j;


/**
 * An implementation of rest api extractor for the sources that are using rest api
 *
 * @param <D> type of data record
 * @param <S> type of schema
 */
@Slf4j
public abstract class RestApiExtractor extends QueryBasedExtractor<JsonArray, JsonElement>
    implements SourceSpecificLayer<JsonArray, JsonElement>, RestApiSpecificLayer {
  private static final Gson GSON = new Gson();
  protected String instanceUrl;
  protected String updatedQuery;

  protected final RestApiConnector connector;

  public RestApiExtractor(WorkUnitState state) {
    super(state);
    this.connector = getConnector(state);
  }

  protected abstract RestApiConnector getConnector(WorkUnitState state);

  @Override
  public void extractMetadata(String schema, String entity, WorkUnit workUnit) throws SchemaException {
    log.info("Extract Metadata using Rest Api");
    JsonArray columnArray = new JsonArray();
    String inputQuery = workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_QUERY);
    List<String> columnListInQuery = null;
    JsonArray array = null;
    if (!Strings.isNullOrEmpty(inputQuery)) {
      columnListInQuery = Utils.getColumnListFromQuery(inputQuery);
    }

    String excludedColumns = workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_EXCLUDED_COLUMNS);
    List<String> columnListExcluded = ImmutableList.<String> of();

    if (Strings.isNullOrEmpty(inputQuery) && !Strings.isNullOrEmpty(excludedColumns)) {
      Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
      columnListExcluded = splitter.splitToList(excludedColumns.toLowerCase());
    }

    try {
      boolean success = this.connector.connect();
      if (!success) {
        throw new SchemaException("Failed to connect.");
      }
      log.debug("Connected successfully.");
      List<Command> cmds = this.getSchemaMetadata(schema, entity);
      CommandOutput<?, ?> response = this.connector.getResponse(cmds);
      array = this.getSchema(response);

      for (JsonElement columnElement : array) {
        Schema obj = GSON.fromJson(columnElement, Schema.class);
        String columnName = obj.getColumnName();

        obj.setWaterMark(this.isWatermarkColumn(workUnitState.getProp("extract.delta.fields"), columnName));

        if (this.isWatermarkColumn(workUnitState.getProp("extract.delta.fields"), columnName)) {
          obj.setNullable(false);
        } else if (this.getPrimarykeyIndex(workUnitState.getProp("extract.primary.key.fields"), columnName) == 0) {
          // set all columns as nullable except primary key and watermark columns
          obj.setNullable(true);
        }

        obj.setPrimaryKey(this.getPrimarykeyIndex(workUnitState.getProp("extract.primary.key.fields"), columnName));

        String jsonStr = GSON.toJson(obj);
        JsonObject jsonObject = GSON.fromJson(jsonStr, JsonObject.class).getAsJsonObject();

        // If input query is null or provided '*' in the query select all columns.
        // Else, consider only the columns mentioned in the column list
        if (inputQuery == null || columnListInQuery == null
            || (columnListInQuery.size() == 1 && columnListInQuery.get(0).equals("*"))
            || (columnListInQuery.size() >= 1 && this.isMetadataColumn(columnName, columnListInQuery))) {
          if (!columnListExcluded.contains(columnName.trim().toLowerCase())) {
            this.columnList.add(columnName);
            columnArray.add(jsonObject);
          }
        }
      }

      if (inputQuery == null && this.columnList.size() != 0) {
        // if input query is null, build the query from metadata
        this.updatedQuery = "SELECT " + Joiner.on(",").join(this.columnList) + " FROM " + entity;
      } else {
        // if input query is not null, build the query with intersection of columns from input query and columns from Metadata
        if (inputQuery != null) {
          String queryLowerCase = inputQuery.toLowerCase();
          int columnsStartIndex = queryLowerCase.indexOf("select ") + 7;
          int columnsEndIndex = queryLowerCase.indexOf(" from ");
          if (columnsStartIndex > 0 && columnsEndIndex > 0) {
            String givenColumnList = inputQuery.substring(columnsStartIndex, columnsEndIndex);
            this.updatedQuery = inputQuery.replace(givenColumnList, Joiner.on(",").join(this.columnList));
          } else {
            this.updatedQuery = inputQuery;
          }
        }
      }

      log.info("Updated input query: " + this.updatedQuery);
      log.debug("Schema:" + columnArray);
      this.setOutputSchema(columnArray);
    } catch (RuntimeException | RestApiConnectionException | RestApiProcessingException | IOException
        | SchemaException e) {
      throw new SchemaException("Failed to get schema using rest api; error - " + e.getMessage(), e);
    }
  }

  @Override
  public long getMaxWatermark(String schema, String entity, String watermarkColumn, List<Predicate> predicateList,
      String watermarkSourceFormat) throws HighWatermarkException {
    log.info("Get high watermark using Rest Api");
    long CalculatedHighWatermark = -1;
    try {
      boolean success = this.connector.connect();
      if (!success) {
        throw new HighWatermarkException("Failed to connect.");
      }
      log.debug("Connected successfully.");
      List<Command> cmds = this.getHighWatermarkMetadata(schema, entity, watermarkColumn, predicateList);
      CommandOutput<?, ?> response = this.connector.getResponse(cmds);
      CalculatedHighWatermark = this.getHighWatermark(response, watermarkColumn, watermarkSourceFormat);
      log.info("High watermark:" + CalculatedHighWatermark);
      return CalculatedHighWatermark;
    } catch (Exception e) {
      throw new HighWatermarkException("Failed to get high watermark using rest api; error - " + e.getMessage(), e);
    }
  }

  @Override
  public long getSourceCount(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws RecordCountException {
    log.info("Get source record count using Rest Api");
    long count = 0;
    try {
      boolean success = this.connector.connect();
      if (!success) {
        throw new RecordCountException("Failed to connect.");
      }
      log.debug("Connected successfully.");
      List<Command> cmds = this.getCountMetadata(schema, entity, workUnit, predicateList);
      CommandOutput<?, ?> response = this.connector.getResponse(cmds);
      count = getCount(response);
      log.info("Source record count:" + count);
      return count;
    } catch (Exception e) {
      throw new RecordCountException("Failed to get record count using rest api; error - " + e.getMessage(), e);
    }
  }

  @Override
  public Iterator<JsonElement> getRecordSet(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList) throws DataRecordException {
    log.debug("Get data records using Rest Api");
    Iterator<JsonElement> rs = null;
    List<Command> cmds;
    try {
      boolean success = true;
      if (this.connector.isConnectionClosed()) {
        success = this.connector.connect();
      }

      if (!success) {
        throw new DataRecordException("Failed to connect.");
      }
      log.debug("Connected successfully.");
      if (this.getPullStatus() == false) {
        return null;
      }
      if (this.getNextUrl() == null) {
        cmds = this.getDataMetadata(schema, entity, workUnit, predicateList);
      } else {
        cmds = RestApiConnector.constructGetCommand(this.getNextUrl());
      }
      CommandOutput<?, ?> response = this.connector.getResponse(cmds);
      rs = this.getData(response);
      return rs;
    } catch (Exception e) {
      throw new DataRecordException("Failed to get records using rest api; error - " + e.getMessage(), e);
    }
  }

  @Override
  public void setTimeOut(int timeOut) {
    this.connector.setAuthTokenTimeout(timeOut);
  }

}
