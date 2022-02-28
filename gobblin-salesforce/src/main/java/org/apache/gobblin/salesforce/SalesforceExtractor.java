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

package org.apache.gobblin.salesforce;

import com.google.common.collect.Iterators;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchInfoList;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectorConfig;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.exception.HighWatermarkException;
import org.apache.gobblin.source.extractor.exception.RecordCountException;
import org.apache.gobblin.source.extractor.exception.RestApiClientException;
import org.apache.gobblin.source.extractor.exception.RestApiConnectionException;
import org.apache.gobblin.source.extractor.exception.SchemaException;
import org.apache.gobblin.source.extractor.extract.Command;
import org.apache.gobblin.source.extractor.extract.CommandOutput;
import org.apache.gobblin.source.jdbc.SqlQueryUtils;
import org.apache.gobblin.source.extractor.extract.restapi.RestApiCommand;
import org.apache.gobblin.source.extractor.extract.restapi.RestApiCommand.RestApiCommandType;
import org.apache.gobblin.source.extractor.extract.restapi.RestApiConnector;
import org.apache.gobblin.source.extractor.extract.restapi.RestApiExtractor;
import org.apache.gobblin.source.extractor.schema.Schema;
import org.apache.gobblin.source.extractor.utils.Utils;
import org.apache.gobblin.source.extractor.watermark.Predicate;
import org.apache.gobblin.source.extractor.watermark.WatermarkType;
import org.apache.gobblin.source.workunit.WorkUnit;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static org.apache.gobblin.salesforce.SalesforceConfigurationKeys.*;

/**
 * An implementation of salesforce extractor for extracting data from SFDC
 */
@Slf4j
public class SalesforceExtractor extends RestApiExtractor {

  private static final String SOQL_RESOURCE = "/queryAll";
  public static final String SALESFORCE_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'.000Z'";
  private static final String SALESFORCE_DATE_FORMAT = "yyyy-MM-dd";
  private static final String SALESFORCE_HOUR_FORMAT = "HH";
  private static final String SALESFORCE_SOAP_SERVICE = "/services/Soap/u";
  private static final Gson GSON = new Gson();
  private static final int MAX_RETRY_INTERVAL_SECS = 600;

  private boolean pullStatus = true;
  private String nextUrl;

  private BulkConnection bulkConnection = null;
  private JobInfo bulkJob = new JobInfo();
  private List<BatchIdAndResultId> bulkResultIdList;
  private boolean bulkJobFinished = true;
  private boolean newBulkResultSet = true;

  private final int pkChunkingSize;
  private final SalesforceConnector sfConnector;

  private final int retryLimit;
  private final long retryInterval;
  private final long retryExceedQuotaInterval;

  private final boolean bulkApiUseQueryAll;
  private SfConfig conf;


  public SalesforceExtractor(WorkUnitState state) {
    super(state);
    conf = new SfConfig(state.getProperties());

    this.sfConnector = (SalesforceConnector) this.connector;
    this.pkChunkingSize = conf.pkChunkingSize;

    this.retryInterval = conf.retryInterval;
    this.retryExceedQuotaInterval = conf.retryExceedQuotaInterval;
    this.bulkApiUseQueryAll = conf.bulkApiUseQueryAll;
    this.retryLimit = conf.fetchRetryLimit;
  }

  @Override
  protected RestApiConnector getConnector(WorkUnitState state) {
    return new SalesforceConnector(state);
  }

  /**
   * true is further pull required else false
   */
  private void setPullStatus(boolean pullStatus) {
    this.pullStatus = pullStatus;
  }

  /**
   * url for the next pull from salesforce
   */
  private void setNextUrl(String nextUrl) {
    this.nextUrl = nextUrl;
  }

  private boolean isBulkJobFinished() {
    return this.bulkJobFinished;
  }

  private void setBulkJobFinished(boolean bulkJobFinished) {
    this.bulkJobFinished = bulkJobFinished;
  }

  private boolean isNewBulkResultSet() {
    return this.newBulkResultSet;
  }

  private void setNewBulkResultSet(boolean newBulkResultSet) {
    this.newBulkResultSet = newBulkResultSet;
  }

  @Override
  public HttpEntity getAuthentication() throws RestApiConnectionException {
    log.debug("Authenticating salesforce");
    return this.connector.getAuthentication();
  }

  @Override
  public List<Command> getSchemaMetadata(String schema, String entity) throws SchemaException {
    log.debug("Build url to retrieve schema");
    return constructGetCommand(this.sfConnector.getFullUri("/sobjects/" + entity.trim() + "/describe"));
  }

  @Override
  public JsonArray getSchema(CommandOutput<?, ?> response) throws SchemaException {
    log.info("Get schema from salesforce");

    String output;
    Iterator<String> itr = (Iterator<String>) response.getResults().values().iterator();
    if (itr.hasNext()) {
      output = itr.next();
    } else {
      throw new SchemaException("Failed to get schema from salesforce; REST response has no output");
    }

    JsonArray fieldJsonArray = new JsonArray();
    JsonElement element = GSON.fromJson(output, JsonObject.class);
    JsonObject jsonObject = element.getAsJsonObject();

    try {
      JsonArray array = jsonObject.getAsJsonArray("fields");
      for (JsonElement columnElement : array) {
        JsonObject field = columnElement.getAsJsonObject();
        Schema schema = new Schema();
        schema.setColumnName(field.get("name").getAsString());

        String dataType = field.get("type").getAsString();
        String elementDataType = "string";
        List<String> mapSymbols = null;
        JsonObject newDataType =
            this.convertDataType(field.get("name").getAsString(), dataType, elementDataType, mapSymbols);
        log.debug("ColumnName:" + field.get("name").getAsString() + ";   old datatype:" + dataType + ";   new datatype:"
            + newDataType);

        schema.setDataType(newDataType);
        schema.setLength(field.get("length").getAsLong());
        schema.setPrecision(field.get("precision").getAsInt());
        schema.setScale(field.get("scale").getAsInt());
        schema.setNullable(field.get("nillable").getAsBoolean());
        schema.setFormat(null);
        schema.setComment((field.get("label").isJsonNull() ? null : field.get("label").getAsString()));
        schema
            .setDefaultValue((field.get("defaultValue").isJsonNull() ? null : field.get("defaultValue").getAsString()));
        schema.setUnique(field.get("unique").getAsBoolean());

        String jsonStr = GSON.toJson(schema);
        JsonObject obj = GSON.fromJson(jsonStr, JsonObject.class).getAsJsonObject();
        fieldJsonArray.add(obj);
      }
    } catch (Exception e) {
      throw new SchemaException("Failed to get schema from salesforce; error - " + e.getMessage(), e);
    }
    return fieldJsonArray;
  }

  @Override
  public List<Command> getHighWatermarkMetadata(String schema, String entity, String watermarkColumn,
      List<Predicate> predicateList) throws HighWatermarkException {
    log.debug("Build url to retrieve high watermark");
    String query = "SELECT " + watermarkColumn + " FROM " + entity;
    String defaultPredicate = " " + watermarkColumn + " != null";
    String defaultSortOrder = " ORDER BY " + watermarkColumn + " desc LIMIT 1";

    String existingPredicate = "";
    if (this.updatedQuery != null) {
      String queryLowerCase = this.updatedQuery.toLowerCase();
      int startIndex = queryLowerCase.indexOf(" where ");
      if (startIndex > 0) {
        existingPredicate = this.updatedQuery.substring(startIndex);
      }
    }
    query = query + existingPredicate;

    String limitString = getLimitFromInputQuery(query);
    query = query.replace(limitString, "");

    Iterator<Predicate> i = predicateList.listIterator();
    while (i.hasNext()) {
      Predicate predicate = i.next();
      query = SqlQueryUtils.addPredicate(query, predicate.getCondition());
    }
    query = SqlQueryUtils.addPredicate(query, defaultPredicate);
    query = query + defaultSortOrder;
    log.info("getHighWatermarkMetadata - QUERY: " + query);

    try {
      return constructGetCommand(this.sfConnector.getFullUri(getSoqlUrl(query)));
    } catch (Exception e) {
      throw new HighWatermarkException("Failed to get salesforce url for high watermark; error - " + e.getMessage(), e);
    }
  }

  @Override
  public long getHighWatermark(CommandOutput<?, ?> response, String watermarkColumn, String format)
      throws HighWatermarkException {
    log.info("Get high watermark from salesforce");

    String output;
    Iterator<String> itr = (Iterator<String>) response.getResults().values().iterator();
    if (itr.hasNext()) {
      output = itr.next();
    } else {
      throw new HighWatermarkException("Failed to get high watermark from salesforce; REST response has no output");
    }

    JsonElement element = GSON.fromJson(output, JsonObject.class);
    long highTs;
    try {
      JsonObject jsonObject = element.getAsJsonObject();
      JsonArray jsonArray = jsonObject.getAsJsonArray("records");
      if (jsonArray.size() == 0) {
        return -1;
      }

      String value = jsonObject.getAsJsonArray("records").get(0).getAsJsonObject().get(watermarkColumn).getAsString();
      if (format != null) {
        SimpleDateFormat inFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
          date = inFormat.parse(value);
        } catch (ParseException e) {
          log.error("ParseException: " + e.getMessage(), e);
        }
        SimpleDateFormat outFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        highTs = Long.parseLong(outFormat.format(date));
      } else {
        highTs = Long.parseLong(value);
      }

    } catch (Exception e) {
      throw new HighWatermarkException("Failed to get high watermark from salesforce; error - " + e.getMessage(), e);
    }
    return highTs;
  }

  @Override
  public List<Command> getCountMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws RecordCountException {
    log.debug("Build url to retrieve source record count");
    String existingPredicate = "";
    if (this.updatedQuery != null) {
      String queryLowerCase = this.updatedQuery.toLowerCase();
      int startIndex = queryLowerCase.indexOf(" where ");
      if (startIndex > 0) {
        existingPredicate = this.updatedQuery.substring(startIndex);
      }
    }

    String query = "SELECT COUNT() FROM " + entity + existingPredicate;
    String limitString = getLimitFromInputQuery(query);
    query = query.replace(limitString, "");

    try {
      if (isNullPredicate(predicateList)) {
        log.info("QUERY with null predicate: " + query);
        return constructGetCommand(this.sfConnector.getFullUri(getSoqlUrl(query)));
      }
      Iterator<Predicate> i = predicateList.listIterator();
      while (i.hasNext()) {
        Predicate predicate = i.next();
        query = SqlQueryUtils.addPredicate(query, predicate.getCondition());
      }

      query = query + getLimitFromInputQuery(this.updatedQuery);
      log.info("getCountMetadata - QUERY: " + query);
      return constructGetCommand(this.sfConnector.getFullUri(getSoqlUrl(query)));
    } catch (Exception e) {
      throw new RecordCountException("Failed to get salesforce url for record count; error - " + e.getMessage(), e);
    }
  }

  @Override
  public long getCount(CommandOutput<?, ?> response) throws RecordCountException {
    log.info("Get source record count from salesforce");

    String output;
    Iterator<String> itr = (Iterator<String>) response.getResults().values().iterator();
    if (itr.hasNext()) {
      output = itr.next();
    } else {
      throw new RecordCountException("Failed to get count from salesforce; REST response has no output");
    }

    JsonElement element = GSON.fromJson(output, JsonObject.class);
    long count;
    try {
      JsonObject jsonObject = element.getAsJsonObject();
      count = jsonObject.get("totalSize").getAsLong();
    } catch (Exception e) {
      throw new RecordCountException("Failed to get record count from salesforce; error - " + e.getMessage(), e);
    }
    return count;
  }

  @Override
  public List<Command> getDataMetadata(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws DataRecordException {
    log.debug("Build url to retrieve data records");
    String query = this.updatedQuery;
    String url = null;
    try {
      if (this.getNextUrl() != null && this.pullStatus) {
        url = this.getNextUrl();
      } else {
        if (isNullPredicate(predicateList)) {
          log.info("getDataMetaData null predicate - QUERY:" + query);
          return constructGetCommand(this.sfConnector.getFullUri(getSoqlUrl(query)));
        }

        String limitString = getLimitFromInputQuery(query);
        query = query.replace(limitString, "");

        Iterator<Predicate> i = predicateList.listIterator();
        while (i.hasNext()) {
          Predicate predicate = i.next();
          query = SqlQueryUtils.addPredicate(query, predicate.getCondition());
        }

        if (Boolean.valueOf(this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_IS_SPECIFIC_API_ACTIVE))) {
          query = SqlQueryUtils.addPredicate(query, "IsDeleted = true");
        }

        query = query + limitString;
        log.info("getDataMetadata - QUERY: " + query);
        url = this.sfConnector.getFullUri(getSoqlUrl(query));
      }
      return constructGetCommand(url);
    } catch (Exception e) {
      throw new DataRecordException("Failed to get salesforce url for data records; error - " + e.getMessage(), e);
    }
  }

  private static String getLimitFromInputQuery(String query) {
    String inputQuery = query.toLowerCase();
    int limitIndex = inputQuery.indexOf(" limit");
    if (limitIndex > 0) {
      return query.substring(limitIndex);
    }
    return "";
  }

  @Override
  public Iterator<JsonElement> getData(CommandOutput<?, ?> response) throws DataRecordException {
    log.debug("Get data records from response");

    String output;
    Iterator<String> itr = (Iterator<String>) response.getResults().values().iterator();
    if (itr.hasNext()) {
      output = itr.next();
    } else {
      throw new DataRecordException("Failed to get data from salesforce; REST response has no output");
    }

    List<JsonElement> rs = Lists.newArrayList();
    JsonElement element = GSON.fromJson(output, JsonObject.class);
    JsonArray partRecords;
    try {
      JsonObject jsonObject = element.getAsJsonObject();

      partRecords = jsonObject.getAsJsonArray("records");
      if (jsonObject.get("done").getAsBoolean()) {
        setPullStatus(false);
      } else {
        setNextUrl(this.sfConnector.getFullUri(
            jsonObject.get("nextRecordsUrl").getAsString().replaceAll(this.sfConnector.getServicesDataEnvPath(), "")));
      }

      JsonArray array = Utils.removeElementFromJsonArray(partRecords, "attributes");
      Iterator<JsonElement> li = array.iterator();
      while (li.hasNext()) {
        JsonElement recordElement = li.next();
        rs.add(recordElement);
      }
      return rs.iterator();
    } catch (Exception e) {
      throw new DataRecordException("Failed to get records from salesforce; error - " + e.getMessage(), e);
    }
  }

  @Override
  public boolean getPullStatus() {
    return this.pullStatus;
  }

  @Override
  public String getNextUrl() {
    return this.nextUrl;
  }

  public static String getSoqlUrl(String soqlQuery) throws RestApiClientException {
    String path = SOQL_RESOURCE + "/";
    NameValuePair pair = new BasicNameValuePair("q", soqlQuery);
    List<NameValuePair> qparams = new ArrayList<>();
    qparams.add(pair);
    return buildUrl(path, qparams);
  }

  private static String buildUrl(String path, List<NameValuePair> qparams) throws RestApiClientException {
    URIBuilder builder = new URIBuilder();
    builder.setPath(path);
    ListIterator<NameValuePair> i = qparams.listIterator();
    while (i.hasNext()) {
      NameValuePair keyValue = i.next();
      builder.setParameter(keyValue.getName(), keyValue.getValue());
    }
    URI uri;
    try {
      uri = builder.build();
    } catch (Exception e) {
      throw new RestApiClientException("Failed to build url; error - " + e.getMessage(), e);
    }
    return new HttpGet(uri).getURI().toString();
  }

  private static boolean isNullPredicate(List<Predicate> predicateList) {
    if (predicateList == null || predicateList.size() == 0) {
      return true;
    }
    return false;
  }

  @Override
  public String getWatermarkSourceFormat(WatermarkType watermarkType) {
    switch (watermarkType) {
      case TIMESTAMP:
        return "yyyy-MM-dd'T'HH:mm:ss";
      case DATE:
        return "yyyy-MM-dd";
      default:
        return null;
    }
  }

  @Override
  public String getHourPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.info("Getting hour predicate from salesforce");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, SALESFORCE_HOUR_FORMAT);
    return column + " " + operator + " " + formattedvalue;
  }

  @Override
  public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
    log.info("Getting date predicate from salesforce");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, SALESFORCE_DATE_FORMAT);
    return column + " " + operator + " " + formattedvalue;
  }

  @Override
  public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.info("Getting timestamp predicate from salesforce");
    String formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, SALESFORCE_TIMESTAMP_FORMAT);
    return column + " " + operator + " " + formattedvalue;
  }

  @Override
  public Map<String, String> getDataTypeMap() {
    Map<String, String> dataTypeMap = ImmutableMap.<String, String>builder().put("url", "string")
        .put("textarea", "string").put("reference", "string").put("phone", "string").put("masterrecord", "string")
        .put("location", "string").put("id", "string").put("encryptedstring", "string").put("email", "string")
        .put("DataCategoryGroupReference", "string").put("calculated", "string").put("anyType", "string")
        .put("address", "string").put("blob", "string").put("date", "date").put("datetime", "timestamp")
        .put("time", "time").put("object", "string").put("string", "string").put("int", "int").put("long", "long")
        .put("double", "double").put("percent", "double").put("currency", "double").put("decimal", "double")
        .put("boolean", "boolean").put("picklist", "string").put("multipicklist", "string").put("combobox", "string")
        .put("list", "string").put("set", "string").put("map", "string").put("enum", "string").build();
    return dataTypeMap;
  }

  private Boolean isPkChunkingFetchDone = false;

  private Iterator<JsonElement> fetchRecordSetPkChunking(WorkUnit workUnit) {
    if (isPkChunkingFetchDone) {
      return null; // must return null to represent no more data.
    }
    log.info("----Get records for pk-chunking----" + workUnit.getProp(PK_CHUNKING_JOB_ID));
    isPkChunkingFetchDone = true; // set to true, never come here twice.
    bulkApiLogin();
    String jobId = workUnit.getProp(PK_CHUNKING_JOB_ID);
    String batchIdResultIdPairString = workUnit.getProp(PK_CHUNKING_BATCH_RESULT_ID_PAIRS);
    List<FileIdVO> fileIdList = this.parseBatchIdResultIdString(jobId, batchIdResultIdPairString);
    return new ResultChainingIterator(bulkConnection, fileIdList, retryLimit, retryInterval, retryExceedQuotaInterval);
  }

  private List<FileIdVO> parseBatchIdResultIdString(String jobId, String batchIdResultIdString) {
    return Arrays.stream(batchIdResultIdString.split(","))
        .map( x -> x.split(":")).map(x -> new FileIdVO(jobId, x[0], x[1]))
        .collect(Collectors.toList());
  }

  private Boolean isBulkFetchDone = false;

  private Iterator<JsonElement> fetchRecordSet(
      String schema,
      String entity,
      WorkUnit workUnit,
      List<Predicate> predicateList
) {
    if (isBulkFetchDone) {
      return null; // need to return null to indicate no more data.
    }
    isBulkFetchDone = true;
    log.info("----Get records for bulk batch job----");
    try {
      // set finish status to false before starting the bulk job
      this.setBulkJobFinished(false);
      this.bulkResultIdList = getQueryResultIds(entity, predicateList);
      log.info("Number of bulk api resultSet Ids:" + this.bulkResultIdList.size());
      List<FileIdVO> fileIdVoList = this.bulkResultIdList.stream()
          .map(x -> new FileIdVO(this.bulkJob.getId(), x.batchId, x.resultId))
          .collect(Collectors.toList());
      ResultChainingIterator chainingIter = new ResultChainingIterator(
          bulkConnection, fileIdVoList, retryLimit, retryInterval, retryExceedQuotaInterval);
      chainingIter.add(getSoftDeletedRecords(schema, entity, workUnit, predicateList));
      return chainingIter;
    } catch (Exception e) {
      throw new RuntimeException("Failed to get records using bulk api; error - " + e.getMessage(), e);
    }
  }

  @Override
  public Iterator<JsonElement> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList) {
    log.debug("Getting salesforce data using bulk api");
    if (workUnit.contains(PK_CHUNKING_JOB_ID)) {
      return fetchRecordSetPkChunking(workUnit);
    } else {
      return fetchRecordSet(schema, entity, workUnit, predicateList);
    }
  }

  /**
   * Get soft deleted records using Rest Api
   * @return iterator with deleted records
   */
  private Iterator<JsonElement> getSoftDeletedRecords(String schema, String entity, WorkUnit workUnit, List<Predicate> predicateList)
      throws DataRecordException {
    boolean disableSoftDeletePull = this.workUnit.getPropAsBoolean(SOURCE_QUERYBASED_SALESFORCE_IS_SOFT_DELETES_PULL_DISABLED);
    if (this.columnList.contains("IsDeleted") && !disableSoftDeletePull) {
      return new QueryResultIterator(this, schema, entity, workUnit, predicateList);
    } else {
      log.info("Ignoring soft delete records");
      return null;
    }
  }

  private void bulkApiLogin() {
    try {
      if (!doBulkApiLogin()) {
        throw new RuntimeException("invalid login");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  /**
   * Login to salesforce
   * @return login status
   */
  private boolean doBulkApiLogin() throws Exception {
    log.info("Authenticating salesforce bulk api");
    boolean success = false;
    String hostName = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME);
    String apiVersion = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_VERSION);
    if (Strings.isNullOrEmpty(apiVersion)) {
      // queryAll was introduced in version 39.0, so need to use a higher version when using queryAll with the bulk api
      apiVersion = this.bulkApiUseQueryAll ? "42.0" : "29.0";
    }

    String soapAuthEndPoint = hostName + SALESFORCE_SOAP_SERVICE + "/" + apiVersion;
    try {
      ConnectorConfig partnerConfig = new ConnectorConfig();
      if (super.workUnitState.contains(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL)
          && !super.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL).isEmpty()) {
        partnerConfig.setProxy(super.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL),
            super.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT));
      }

      String accessToken = sfConnector.getAccessToken();

      if (accessToken == null) {
        boolean isConnectSuccess = sfConnector.connect();
        if (isConnectSuccess) {
          accessToken = sfConnector.getAccessToken();
        }
      }

      if (accessToken != null) {
        String serviceEndpoint = sfConnector.getInstanceUrl() + SALESFORCE_SOAP_SERVICE + "/" + apiVersion;
        partnerConfig.setSessionId(accessToken);
        partnerConfig.setServiceEndpoint(serviceEndpoint);
      } else {
        String securityToken = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_SECURITY_TOKEN);
        String password = PasswordManager.getInstance(this.workUnitState)
            .readPassword(this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD));
        partnerConfig.setUsername(this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME));
        partnerConfig.setPassword(password + securityToken);
      }

      partnerConfig.setAuthEndpoint(soapAuthEndPoint);

      new PartnerConnection(partnerConfig);
      String soapEndpoint = partnerConfig.getServiceEndpoint();
      String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion;

      ConnectorConfig config = createConfig();
      config.setSessionId(partnerConfig.getSessionId());
      config.setRestEndpoint(restEndpoint);


      this.bulkConnection = getBulkConnection(config);
      success = true;
    } catch (RuntimeException e) {
      throw new RuntimeException("Failed to connect to salesforce bulk api; error - " + e, e);
    }
    return success;
  }

  /**
   * same as getQueryResultIdsPkChunking but the arguments are different.
   * this function can take existing batch ids to return ResultFileIdsStruct
   * It is for test/debug. developers may want to skip execute query on SFDC, use a list of existing batch ids
   */
  public ResultFileIdsStruct getQueryResultIdsPkChunkingFetchOnly(String jobId, String batchIdListStr) {
    bulkApiLogin();
    try {
      int retryInterval = Math.min(MAX_RETRY_INTERVAL_SECS * 1000, 30 + this.pkChunkingSize  * 2);
      if (StringUtils.isNotEmpty(batchIdListStr)) {
        log.info("The batchId is specified.");
        return retrievePkChunkingResultIdsByBatchId(this.bulkConnection, jobId, batchIdListStr);
      } else {
        ResultFileIdsStruct resultStruct = retrievePkChunkingResultIds(this.bulkConnection, jobId, retryInterval);
        if (resultStruct.getBatchIdAndResultIdList().isEmpty()) {
          String msg = String.format("There are no result for the [jobId: %s, batchIds: %s]", jobId, batchIdListStr);
          throw new RuntimeException(msg);
        }
        return resultStruct;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  /**
   * get BulkConnection instance
   * @return
   */
  public BulkConnection getBulkConnection(ConnectorConfig config) throws AsyncApiException {
    return new BulkConnection(config);
  }

  /**
   *  this function currently only for pk-chunking. it is from getQueryResultIds
   *  TODO: abstract this function to a common function: arguments need to add connetion, header, output-format
   *  TODO: make it and its related functions pure function (no side effect). Currently still unnecesarily changing this.bulkJob)
   */
  public ResultFileIdsStruct getQueryResultIdsPkChunking(String entity, List<Predicate> predicateList) {
    bulkApiLogin();
    try {
      BulkConnection connection = this.bulkConnection;
      JobInfo jobRequest = new JobInfo();
      jobRequest.setObject(entity);
      jobRequest.setOperation(OperationEnum.queryAll);
      jobRequest.setConcurrencyMode(ConcurrencyMode.Parallel);
      log.info("Enabling pk chunking with size {}", this.pkChunkingSize);
      connection.addHeader("Sforce-Enable-PKChunking", "chunkSize=" + this.pkChunkingSize);
      // Result type as CSV
      jobRequest.setContentType(ContentType.CSV);
      JobInfo createdJob = connection.createJob(jobRequest);
      log.info("Created bulk job: {}", createdJob.getId());
      this.bulkJob = createdJob; // other functions need to use it TODO: remove bulkJob from this class
      String jobId = createdJob.getId();
      JobInfo jobResponse = connection.getJobStatus(jobId);
      // Construct query with the predicates
      String query = this.updatedQuery;
      if (!isNullPredicate(predicateList)) {
        String limitString = getLimitFromInputQuery(query);
        query = query.replace(limitString, "");
        Iterator<Predicate> i = predicateList.listIterator();
        while (i.hasNext()) {
          Predicate predicate = i.next();
          query = SqlQueryUtils.addPredicate(query, predicate.getCondition());
        }
        query = query + limitString;
      }
      log.info("Submitting PK Chunking query:" + query);
      ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING));

      BatchInfo executeQueryBatch = connection.createBatchFromStream(jobResponse, bout);
      String pkChunkingBatchId = executeQueryBatch.getId();

      int waitMilliSecond = 60 * 1000;

      // Get batch info with complete resultset (info id - refers to the resultset id corresponding to entire resultset)
      BatchInfo batchResponse = connection.getBatchInfo(jobId, pkChunkingBatchId);

      // wait for completion, failure, or formation of PK chunking batches
      // user query will be submitted to sfdc and create the first batch,
      // It is supposed to create more batch after the initial batch
      BatchStateEnum batchState = batchResponse.getState();
      while (batchState == BatchStateEnum.InProgress || batchState == BatchStateEnum.Queued) {
        Thread.sleep(waitMilliSecond);
        batchResponse = connection.getBatchInfo(createdJob.getId(), executeQueryBatch.getId());
        log.info("Waiting for first batch (jobId={}, pkChunkingBatchId={})", jobId, pkChunkingBatchId);
        batchState = batchResponse.getState();
      }

      if (batchResponse.getState() == BatchStateEnum.Failed) {
        log.error("Bulk batch failed: " + batchResponse.toString());
        throw new Exception("Failed to get bulk batch info for jobId " + jobId + " error - " + batchResponse.getStateMessage());
      }
      ResultFileIdsStruct resultFileIdsStruct = retrievePkChunkingResultIds(connection, jobId, waitMilliSecond);
      return resultFileIdsStruct;
    } catch (Exception e) {
      throw new RuntimeException("getQueryResultIdsPkChunking: error - " + e.getMessage(), e);
    }
  }

  /**
   * Get Record set using salesforce specific API(Bulk API)
   * @param entity/tablename
   * @param predicateList of all predicate conditions
   * @return iterator with batch of records
   */
  private List<BatchIdAndResultId> getQueryResultIds(String entity, List<Predicate> predicateList) throws Exception {
    bulkApiLogin();
    try {
      // Set bulk job attributes
      this.bulkJob.setObject(entity);
      this.bulkJob.setOperation(this.bulkApiUseQueryAll ? OperationEnum.queryAll : OperationEnum.query);
      this.bulkJob.setConcurrencyMode(ConcurrencyMode.Parallel);

      // Result type as CSV
      this.bulkJob.setContentType(ContentType.CSV);

      this.bulkJob = this.bulkConnection.createJob(this.bulkJob);
      log.info("Created bulk job [jobId={}]", this.bulkJob.getId());

      this.bulkJob = this.bulkConnection.getJobStatus(this.bulkJob.getId());

      // Construct query with the predicates
      String query = this.updatedQuery;
      if (!isNullPredicate(predicateList)) {
        String limitString = getLimitFromInputQuery(query);
        query = query.replace(limitString, "");

        Iterator<Predicate> i = predicateList.listIterator();
        while (i.hasNext()) {
          Predicate predicate = i.next();
          query = SqlQueryUtils.addPredicate(query, predicate.getCondition());
        }

        query = query + limitString;
      }

      log.info("getQueryResultIds - QUERY:" + query);
      ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING));

      BatchInfo bulkBatchInfo = this.bulkConnection.createBatchFromStream(this.bulkJob, bout);

      // Get batch info with complete resultset (info id - refers to the resultset id corresponding to entire resultset)
      bulkBatchInfo = this.bulkConnection.getBatchInfo(this.bulkJob.getId(), bulkBatchInfo.getId());

      // wait for completion, failure, or formation of PK chunking batches
      // if it is InProgress or Queued, continue to wait.
      int count = 0;
      long minWaitTimeInMilliSeconds = super.workUnitState.getPropAsLong(
          ConfigurationKeys.EXTRACT_SALESFORCE_BULK_API_MIN_WAIT_TIME_IN_MILLIS_KEY,
          ConfigurationKeys.DEFAULT_EXTRACT_SALESFORCE_BULK_API_MIN_WAIT_TIME_IN_MILLIS);
      long maxWaitTimeInMilliSeconds = super.workUnitState.getPropAsLong(
          ConfigurationKeys.EXTRACT_SALESFORCE_BULK_API_MAX_WAIT_TIME_IN_MILLIS_KEY,
          ConfigurationKeys.DEFAULT_EXTRACT_SALESFORCE_BULK_API_MAX_WAIT_TIME_IN_MILLIS);
      while (bulkBatchInfo.getState() == BatchStateEnum.InProgress || bulkBatchInfo.getState() == BatchStateEnum.Queued) {
        log.info("Waiting for bulk resultSetIds");
        // Exponential backoff
        long waitMilliSeconds = Math.min((long) (Math.pow(2, count) * minWaitTimeInMilliSeconds), maxWaitTimeInMilliSeconds);
        Thread.sleep(waitMilliSeconds);
        bulkBatchInfo = this.bulkConnection.getBatchInfo(this.bulkJob.getId(), bulkBatchInfo.getId());
        count++;
      }

      // Wait for pk chunking batches
      BatchInfoList batchInfoList = this.bulkConnection.getBatchInfoList(this.bulkJob.getId());
      BatchStateEnum state = bulkBatchInfo.getState();
      // not sure if the state can be "NotProcessed" in non-pk-chunking bulk request
      // SFDC doc says - This state is assigned when a job is aborted while the batch is queued
      if (state == BatchStateEnum.Failed || state == BatchStateEnum.InProgress) {
        log.error("Bulk batch failed: " + bulkBatchInfo.toString());
        throw new RuntimeException("Failed to get bulk batch info for jobId " + bulkBatchInfo.getJobId()
            + " error - " + bulkBatchInfo.getStateMessage());
      }

      // Get resultset ids of all the batches from the batch info list
      List<BatchIdAndResultId> batchIdAndResultIdList = Lists.newArrayList();

      for (BatchInfo bi : batchInfoList.getBatchInfo()) {
        QueryResultList list = this.bulkConnection.getQueryResultList(this.bulkJob.getId(), bi.getId());
        for (String result : list.getResult()) {
          batchIdAndResultIdList.add(new BatchIdAndResultId(bi.getId(), result));
        }
      }

      log.info("QueryResultList: " + batchIdAndResultIdList);

      return batchIdAndResultIdList;

    } catch (RuntimeException | AsyncApiException | InterruptedException e) {
      throw new RuntimeException(
          "Failed to get query result ids from salesforce using bulk api; error - " + e.getMessage(), e);
    }
  }

  @Override
  public void closeConnection() throws Exception {
    if (this.bulkConnection != null
        && !this.bulkConnection.getJobStatus(this.getBulkJobId()).getState().toString().equals("Closed")) {
      log.info("Closing salesforce bulk job connection");
      this.bulkConnection.closeJob(this.getBulkJobId());
    }
    this.sfConnector.close();
  }

  private static List<Command> constructGetCommand(String restQuery) {
    return Arrays.asList(new RestApiCommand().build(Arrays.asList(restQuery), RestApiCommandType.GET));
  }

  private ResultFileIdsStruct retrievePkChunkingResultIdsByBatchId(BulkConnection connection, String jobId, String batchIdListStr) {
    Iterator<String> batchIds = Arrays.stream(batchIdListStr.split(",")).map(x -> x.trim()).filter(x -> !x.equals("")).iterator();
    try {
      List<BatchIdAndResultId> batchIdAndResultIdList = fetchBatchResultIds(connection, jobId, batchIds);
      return new ResultFileIdsStruct(jobId, batchIdAndResultIdList);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Waits for the PK batches to complete. The wait will stop after all batches are complete or on the first failed batch
   */
  private ResultFileIdsStruct retrievePkChunkingResultIds(BulkConnection connection, String jobId, int waitMilliSecond) {
    log.info("Waiting for completion of the the bulk job [jobId={}])'s sub queries.", jobId);
    try {
      while (true) {
        BatchInfoList batchInfoList = connection.getBatchInfoList(jobId);
        BatchInfo[] batchInfos = batchInfoList.getBatchInfo();
        if (needContinueToPoll(batchInfos, waitMilliSecond)) {
          continue; // continue to wait
        }
        if (Arrays.stream(batchInfos).filter(x -> x.getState() == BatchStateEnum.NotProcessed).count() != 1) {
          throw new Exception("PK-Chunking query should have 1 and only 1 batch with state=NotProcessed.");
        }
        Stream<BatchInfo> stream = Arrays.stream(batchInfos);
        Iterator<String> batchIds = stream.filter(x -> x.getNumberRecordsProcessed() != 0).map(x -> x.getId()).iterator();
        List<BatchIdAndResultId> batchIdAndResultIdList = fetchBatchResultIds(connection, jobId, batchIds);
        return new ResultFileIdsStruct(jobId, batchIdAndResultIdList);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<BatchIdAndResultId> fetchBatchResultIds(BulkConnection connection, String jobId, Iterator<String> batchIds) throws Exception {
    List<BatchIdAndResultId> batchIdAndResultIdList = Lists.newArrayList();
    while (batchIds.hasNext()) {
      String batchId = batchIds.next();
      QueryResultList result = connection.getQueryResultList(jobId, batchId);
      Stream<String> stream = Arrays.stream(result.getResult());
      Iterator<BatchIdAndResultId> it = stream.map(rId -> new BatchIdAndResultId(batchId, rId)).iterator();
      Iterators.addAll(batchIdAndResultIdList, it);
    }
    return batchIdAndResultIdList;
  }

  private Boolean needContinueToPoll(BatchInfo[] batchInfos, long toWait) {
    long queued = Arrays.stream(batchInfos).filter(x -> x.getState() == BatchStateEnum.Queued).count();
    long inProgress = Arrays.stream(batchInfos).filter(x -> x.getState() == BatchStateEnum.InProgress).count();
    for (BatchInfo bi : batchInfos) {
      BatchStateEnum state = bi.getState();
      if (state == BatchStateEnum.InProgress || state == BatchStateEnum.Queued) {
        try {
          log.info("Total: {}, queued: {}, InProgress: {}, waiting for [batchId: {}, state: {}]", batchInfos.length, queued, inProgress, bi.getId(), state);
          Thread.sleep(toWait);
        } catch (InterruptedException e) { // skip
        }
        return true; // need to continue to wait
      }
      if (state == BatchStateEnum.Failed) {
        throw new RuntimeException(String.format("[batchId=%s] failed", bi.getId()));
      }
    }
    return false; // no need to wait
  }

  //Moving config creation in a separate method for custom config parameters like setting up transport factory.
  public ConnectorConfig createConfig() {
    ConnectorConfig config = new ConnectorConfig();
    config.setCompression(true);
    try {
      config.setTraceFile("traceLogs.txt");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    config.setTraceMessage(false);
    config.setPrettyPrintXml(true);

    if (super.workUnitState.contains(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL)
        && !super.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL).isEmpty()) {
      config.setProxy(super.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL),
          super.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT));
    }
    return config;
  }

  public String getBulkJobId() {
    return workUnit.getProp(PK_CHUNKING_JOB_ID, this.bulkJob.getId());
  }

  @Data
  public static class BatchIdAndResultId {
    private final String batchId;
    private final String resultId;
  }

  @Data
  public static class ResultFileIdsStruct {
    private final String jobId;
    private final List<BatchIdAndResultId> batchIdAndResultIdList;
  }
}
