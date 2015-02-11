/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.restapi;

import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.exception.RestApiConnectionException;
import gobblin.source.extractor.exception.RestApiProcessingException;
import gobblin.source.extractor.utils.Utils;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.extractor.exception.HighWatermarkException;
import gobblin.source.extractor.exception.RecordCountException;
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.extractor.extract.QueryBasedExtractor;
import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.extract.CommandOutput;
import gobblin.source.extractor.extract.SourceSpecificLayer;
import gobblin.source.extractor.extract.restapi.RestApiCommand.RestApiCommandType;
import gobblin.source.extractor.schema.Schema;
import gobblin.source.workunit.WorkUnit;


/**
 * An implementation of rest api extractor for the sources that are using rest api
 *
 * @param <D> type of data record
 * @param <S> type of schema
 */
public abstract class RestApiExtractor extends QueryBasedExtractor<JsonArray, JsonElement> implements SourceSpecificLayer<JsonArray, JsonElement>, RestApiSpecificLayer {
  private static final Gson gson = new Gson();
  private HttpClient httpClient = null;
  private boolean autoEstablishAuthToken = false;
  private long authTokenTimeout;
  private String accessToken;
  private long createdAt;
  protected String instanceUrl;
  protected String updatedQuery;
  protected Logger log = LoggerFactory.getLogger(RestApiExtractor.class);

  public RestApiExtractor(WorkUnitState state) {
    super(state);
  }

  private void setAuthTokenTimeout(long authTokenTimeout) {
    this.authTokenTimeout = authTokenTimeout;
  }

  /**
   * get http client
   * @return default httpclient
   */
  protected HttpClient getHttpClient() {
    if (httpClient == null) {
      httpClient = new DefaultHttpClient();

      if (super.workUnitState.contains(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL) && !super.workUnitState
          .getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL).isEmpty()) {
        log.info("Connecting via proxy: " + super.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL));

        HttpHost proxy = new HttpHost(super.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL),
            super.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT));
        httpClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
      }
    }
    return httpClient;
  }

  @Override
  public void extractMetadata(String schema, String entity, WorkUnit workUnit)
      throws SchemaException {
    this.log.info("Extract Metadata using Rest Api");
    JsonArray columnArray = new JsonArray();
    String inputQuery = workUnit.getProp(ConfigurationKeys.SOURCE_QUERYBASED_QUERY);
    List<String> columnListInQuery = null;
    JsonArray array = null;
    if (!Strings.isNullOrEmpty(inputQuery)) {
      columnListInQuery = Utils.getColumnListFromQuery(inputQuery);
    }

    try {
      boolean success = this.getConnection();
      if (!success) {
        throw new SchemaException("Failed to connect.");
      } else {
        this.log.debug("Connected successfully.");
        List<Command> cmds = this.getSchemaMetadata(schema, entity);
        CommandOutput<?, ?> response = this.getResponse(cmds);
        array = this.getSchema(response);

        for (JsonElement columnElement : array) {
          Schema obj = gson.fromJson(columnElement, Schema.class);
          String columnName = obj.getColumnName();

          obj.setWaterMark(this.isWatermarkColumn(workUnit.getProp("extract.delta.fields"), columnName));

          if (this.isWatermarkColumn(workUnit.getProp("extract.delta.fields"), columnName)) {
            obj.setNullable(false);
          } else if (this.getPrimarykeyIndex(workUnit.getProp("extract.primary.key.fields"), columnName) == 0) {
            // set all columns as nullable except primary key and watermark columns
            obj.setNullable(true);
          }

          obj.setPrimaryKey(this.getPrimarykeyIndex(workUnit.getProp("extract.primary.key.fields"), columnName));

          String jsonStr = gson.toJson(obj);
          JsonObject jsonObject = gson.fromJson(jsonStr, JsonObject.class).getAsJsonObject();

          // If input query is null or provided '*' in the query select all columns.
          // Else, consider only the columns mentioned in the column list
          if (inputQuery == null || columnListInQuery == null || (columnListInQuery.size() == 1 && columnListInQuery
              .get(0).equals("*")) || (columnListInQuery.size() >= 1 && this
              .isMetadataColumn(columnName, columnListInQuery))) {
            this.columnList.add(columnName);
            columnArray.add(jsonObject);
          }
        }

        if (inputQuery == null && this.columnList.size() != 0) {
          // if input query is null, build the query from metadata
          this.updatedQuery = "SELECT " + Joiner.on(",").join(columnList) + " FROM " + entity;
        } else {
          // if input query is not null, build the query with intersection of columns from input query and columns from Metadata
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

        this.log.info("Updated input query: " + this.updatedQuery);
        this.log.debug("Schema:" + columnArray);
        this.setOutputSchema(columnArray);
      }
    } catch (Exception e) {
      throw new SchemaException("Failed to get schema using rest api; error - " + e.getMessage(), e);
    }
  }

  @Override
  public long getMaxWatermark(String schema, String entity, String watermarkColumn, List<Predicate> predicateList,
      String watermarkSourceFormat)
      throws HighWatermarkException {
    this.log.info("Get high watermark using Rest Api");
    long CalculatedHighWatermark = -1;
    try {
      boolean success = this.getConnection();
      if (!success) {
        throw new HighWatermarkException("Failed to connect.");
      } else {
        this.log.debug("Connected successfully.");
        List<Command> cmds = this.getHighWatermarkMetadata(schema, entity, watermarkColumn, predicateList);
        CommandOutput<?, ?> response = this.getResponse(cmds);
        CalculatedHighWatermark = this.getHighWatermark(response, watermarkColumn, watermarkSourceFormat);
      }
      this.log.info("High watermark:" + CalculatedHighWatermark);
      return CalculatedHighWatermark;
    } catch (Exception e) {
      throw new HighWatermarkException("Failed to get high watermark using rest api; error - " + e.getMessage(), e);
    }
  }

  @Override
  public long getSourceCount(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws RecordCountException {
    this.log.info("Get source record count using Rest Api");
    long count = 0;
    try {
      boolean success = this.getConnection();
      if (!success) {
        throw new RecordCountException("Failed to connect.");
      } else {
        this.log.debug("Connected successfully.");
        List<Command> cmds = this.getCountMetadata(schema, entity, workUnit, predicateList);
        CommandOutput<?, ?> response = this.getResponse(cmds);
        count = this.getCount(response);
        this.log.info("Source record count:" + count);
      }
      return count;
    } catch (Exception e) {
      throw new RecordCountException("Failed to get record count using rest api; error - " + e.getMessage(), e);
    }
  }

  @Override
  public Iterator<JsonElement> getRecordSet(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList)
      throws DataRecordException {
    this.log.debug("Get data records using Rest Api");
    Iterator<JsonElement> rs = null;
    List<Command> cmds;
    try {
      boolean success = true;
      if (isConnectionClosed()) {
        success = this.getConnection();
      }

      if (!success) {
        throw new DataRecordException("Failed to connect.");
      } else {
        this.log.debug("Connected successfully.");
        if (this.getPullStatus() == false) {
          return null;
        } else {
          if (this.getNextUrl() == null) {
            cmds = this.getDataMetadata(schema, entity, workUnit, predicateList);
          } else {
            cmds = RestApiExtractor.constructGetCommand(this.getNextUrl());
          }
          CommandOutput<?, ?> response = this.getResponse(cmds);
          rs = this.getData(response);
        }
      }
      return rs;
    } catch (Exception e) {
      throw new DataRecordException("Failed to get records using rest api; error - " + e.getMessage(), e);
    }
  }

  @Override
  public void setTimeOut(int timeOut) {
    this.setAuthTokenTimeout(timeOut);
  }

  @Override
  public Map<String, String> getDataTypeMap() {
    return this.getDataTypeMap();
  }

  /**
   * Connect to rest api
   * @return true if it is success else false
   */
  private boolean getConnection()
      throws RestApiConnectionException {
    this.log.debug("Connecting to the source using Rest Api");
    return this.connect();
  }

  /**
   * Check if connection is closed
   * @return true if the connection is closed else false
   */
  private boolean isConnectionClosed()
      throws Exception {
    if (this.httpClient == null) {
      return true;
    }
    return false;
  }

  /**
   * get http connection
   * @return true if the connection is success else false
   */
  private boolean connect()
      throws RestApiConnectionException {
    if (autoEstablishAuthToken) {
      if (authTokenTimeout <= 0) {
        return false;
      } else if ((System.currentTimeMillis() - createdAt) > authTokenTimeout) {
        return false;
      }
    }

    HttpEntity httpEntity = null;
    try {
      httpEntity = this.getAuthentication();

      if (httpEntity != null) {
        JsonElement json = gson.fromJson(EntityUtils.toString(httpEntity), JsonObject.class);
        if (json == null) {
          throw new RestApiConnectionException(
              "Failed on authentication with the following HTTP response received:\n" + EntityUtils
                  .toString(httpEntity));
        }

        JsonObject jsonRet = json.getAsJsonObject();
        if (!this.hasId(jsonRet)) {
          throw new RestApiConnectionException(
              "Failed on authentication with the following HTTP response received:\n" + json);
        }

        this.instanceUrl = jsonRet.get("instance_url").getAsString();
        this.accessToken = jsonRet.get("access_token").getAsString();
        this.createdAt = System.currentTimeMillis();
      }
    } catch (Exception e) {
      throw new RestApiConnectionException("Failed to get rest api connection; error - " + e.getMessage(), e);
    } finally {
      if (httpEntity != null) {
        try {
          EntityUtils.consume(httpEntity);
        } catch (Exception e) {
          throw new RestApiConnectionException("Failed to consume httpEntity; error - " + e.getMessage(), e);
        }
      }
    }

    return true;
  }

  private boolean hasId(JsonObject json) {
    if (json.has("id") || json.has("Id") || json.has("ID") || json.has("iD")) {
      return true;
    }
    return false;
  }

  /**
   * get http response in json format using url
   * @return json string with the response
   */
  private CommandOutput<?, ?> getResponse(List<Command> cmds)
      throws RestApiProcessingException {
    String url = cmds.get(0).getParams().get(0);

    this.log.info("URL: " + url);
    String jsonStr = null;
    HttpRequestBase httpRequest = new HttpGet(url);
    addHeaders(httpRequest);
    HttpEntity httpEntity = null;
    HttpResponse httpResponse = null;
    try {
      httpResponse = this.httpClient.execute(httpRequest);
      StatusLine status = httpResponse.getStatusLine();
      httpEntity = httpResponse.getEntity();

      if (httpEntity != null) {
        jsonStr = EntityUtils.toString(httpEntity);
      }

      if (status.getStatusCode() >= 400) {
        this.log.info("Unable to get response using: " + url);
        JsonElement jsonRet = gson.fromJson(jsonStr, JsonArray.class);
        throw new RestApiProcessingException(this.getFirstErrorMessage("Failed to retrieve response from", jsonRet));
      }
    } catch (Exception e) {
      throw new RestApiProcessingException("Failed to process rest api request; error - " + e.getMessage(), e);
    } finally {
      try {
        if (httpEntity != null) {
          EntityUtils.consume(httpEntity);
        }
        // httpResponse.close();
      } catch (Exception e) {
        throw new RestApiProcessingException("Failed to consume httpEntity; error - " + e.getMessage(), e);
      }
    }
    CommandOutput<RestApiCommand, String> output = new RestApiCommandOutput();
    output.put((RestApiCommand) cmds.get(0), jsonStr);
    return output;
  }

  private void addHeaders(HttpRequestBase httpRequest) {
    if (this.accessToken != null) {
      httpRequest.addHeader("Authorization", "OAuth " + this.accessToken);
    }
    httpRequest.addHeader("Content-Type", "application/json");
    //httpRequest.addHeader("Accept-Encoding", "zip");
    //httpRequest.addHeader("Content-Encoding", "gzip");
    //httpRequest.addHeader("Connection", "Keep-Alive");
    //httpRequest.addHeader("Keep-Alive", "timeout=60000");
  }

  /**
   * get error message while executing http url
   * @return error message
   */
  private String getFirstErrorMessage(String defaultMessage, JsonElement json) {
    if (json == null) {
      return defaultMessage;
    }

    JsonObject jsonObject = null;

    if (!json.isJsonArray()) {
      jsonObject = json.getAsJsonObject();
    } else {
      JsonArray jsonArray = json.getAsJsonArray();
      for (int i = 0; i < jsonArray.size(); i++) {
        JsonElement element = jsonArray.get(i);
        jsonObject = element.getAsJsonObject();
        break;
      }
    }

    if (jsonObject != null) {
      if (jsonObject.has("error_description")) {
        defaultMessage = defaultMessage + jsonObject.get("error_description").getAsString();
      } else if (jsonObject.has("message")) {
        defaultMessage = defaultMessage + jsonObject.get("message").getAsString();
      }
    }

    return defaultMessage;
  }

  public static List<Command> constructGetCommand(String restQuery) {
    return Arrays.asList(new RestApiCommand().build(Arrays.asList(restQuery), RestApiCommandType.GET));
  }
}
