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

package gobblin.salesforce;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectorConfig;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.password.PasswordManager;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.exception.HighWatermarkException;
import gobblin.source.extractor.exception.RecordCountException;
import gobblin.source.extractor.exception.RestApiClientException;
import gobblin.source.extractor.exception.RestApiConnectionException;
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.extract.CommandOutput;
import gobblin.source.jdbc.SqlQueryUtils;
import gobblin.source.extractor.extract.restapi.RestApiCommand;
import gobblin.source.extractor.extract.restapi.RestApiCommand.RestApiCommandType;
import gobblin.source.extractor.extract.restapi.RestApiConnector;
import gobblin.source.extractor.extract.restapi.RestApiExtractor;
import gobblin.source.extractor.resultset.RecordSet;
import gobblin.source.extractor.resultset.RecordSetList;
import gobblin.source.extractor.schema.Schema;
import gobblin.source.extractor.utils.InputStreamCSVReader;
import gobblin.source.extractor.utils.Utils;
import gobblin.source.extractor.watermark.Predicate;
import gobblin.source.extractor.watermark.WatermarkType;
import gobblin.source.workunit.WorkUnit;
import lombok.extern.slf4j.Slf4j;


/**
 * An implementation of salesforce extractor for extracting data from SFDC
 */
@Slf4j
public class SalesforceExtractor extends RestApiExtractor {

  private static final String SOQL_RESOURCE = "/queryAll";
  private static final String SALESFORCE_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'.000Z'";
  private static final String SALESFORCE_DATE_FORMAT = "yyyy-MM-dd";
  private static final String SALESFORCE_HOUR_FORMAT = "HH";
  private static final String SALESFORCE_SOAP_AUTH_SERVICE = "/services/Soap/u";
  private static final Gson GSON = new Gson();

  private boolean pullStatus = true;
  private String nextUrl;

  private BulkConnection bulkConnection = null;
  private boolean bulkApiInitialRun = true;
  private JobInfo bulkJob = new JobInfo();
  private BatchInfo bulkBatchInfo = null;
  private BufferedReader bulkBufferedReader = null;
  private List<String> bulkResultIdList = Lists.newArrayList();
  private int bulkResultIdCount = 0;
  private boolean bulkJobFinished = true;
  private List<String> bulkRecordHeader;
  private int bulkResultColumCount;
  private boolean newBulkResultSet = true;
  private int bulkRecordCount = 0;

  private final SalesforceConnector sfConnector;

  public SalesforceExtractor(WorkUnitState state) {
    super(state);
    this.sfConnector = (SalesforceConnector) this.connector;
  }

  @Override
  protected RestApiConnector getConnector(WorkUnitState state) {
    return new SalesforceConnector(state);
  }

  /**
   * true is further pull required else false
   */
  public void setPullStatus(boolean pullStatus) {
    this.pullStatus = pullStatus;
  }

  /**
   * url for the next pull from salesforce
   */
  public void setNextUrl(String nextUrl) {
    this.nextUrl = nextUrl;
  }

  private boolean isBulkJobFinished() {
    return this.bulkJobFinished;
  }

  private void setBulkJobFinished(boolean bulkJobFinished) {
    this.bulkJobFinished = bulkJobFinished;
  }

  public boolean isNewBulkResultSet() {
    return this.newBulkResultSet;
  }

  public void setNewBulkResultSet(boolean newBulkResultSet) {
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
    log.info("QUERY: " + query);

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
    long high_ts;
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
        high_ts = Long.parseLong(outFormat.format(date));
      } else {
        high_ts = Long.parseLong(value);
      }

    } catch (Exception e) {
      throw new HighWatermarkException("Failed to get high watermark from salesforce; error - " + e.getMessage(), e);
    }
    return high_ts;
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
      log.info("QUERY: " + query);
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
      if (this.getNextUrl() != null && this.pullStatus == true) {
        url = this.getNextUrl();
      } else {
        if (isNullPredicate(predicateList)) {
          log.info("QUERY:" + query);
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
        log.info("QUERY: " + query);
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
    String Formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, SALESFORCE_HOUR_FORMAT);
    return column + " " + operator + " " + Formattedvalue;
  }

  @Override
  public String getDatePredicateCondition(String column, long value, String valueFormat, String operator) {
    log.info("Getting date predicate from salesforce");
    String Formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, SALESFORCE_DATE_FORMAT);
    return column + " " + operator + " " + Formattedvalue;
  }

  @Override
  public String getTimestampPredicateCondition(String column, long value, String valueFormat, String operator) {
    log.info("Getting timestamp predicate from salesforce");
    String Formattedvalue = Utils.toDateTimeFormat(Long.toString(value), valueFormat, SALESFORCE_TIMESTAMP_FORMAT);
    return column + " " + operator + " " + Formattedvalue;
  }

  @Override
  public Map<String, String> getDataTypeMap() {
    Map<String, String> dataTypeMap = ImmutableMap.<String, String> builder().put("url", "string")
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

  @Override
  public Iterator<JsonElement> getRecordSetFromSourceApi(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList) throws IOException {
    log.debug("Getting salesforce data using bulk api");
    RecordSet<JsonElement> rs = null;

    try {
      //Get query result ids in the first run
      //result id is used to construct url while fetching data
      if (this.bulkApiInitialRun == true) {
        // set finish status to false before starting the bulk job
        this.setBulkJobFinished(false);
        this.bulkResultIdList = getQueryResultIds(entity, predicateList);
        log.info("Number of bulk api resultSet Ids:" + this.bulkResultIdList.size());
      }

      // Get data from input stream
      // If bulk load is not finished, get data from the stream
      if (!this.isBulkJobFinished()) {
        rs = getBulkData();
      }

      // Set bulkApiInitialRun to false after the completion of first run
      this.bulkApiInitialRun = false;

      // If bulk job is finished, get soft deleted records using Rest API
      boolean isSoftDeletesPullDisabled = Boolean.valueOf(this.workUnit
          .getProp(SalesforceConfigurationKeys.SOURCE_QUERYBASED_SALESFORCE_IS_SOFT_DELETES_PULL_DISABLED));
      if (rs == null || rs.isEmpty()) {
        // Get soft delete records only if IsDeleted column exists and soft deletes pull is not disabled
        if (this.columnList.contains("IsDeleted") && !isSoftDeletesPullDisabled) {
          return this.getSoftDeletedRecords(schema, entity, workUnit, predicateList);
        }
        log.info("Ignoring soft delete records");
      }

      return rs.iterator();

    } catch (Exception e) {
      throw new IOException("Failed to get records using bulk api; error - " + e.getMessage(), e);
    }
  }

  /**
   * Get soft deleted records using Rest Api
     * @return iterator with deleted records
   */
  private Iterator<JsonElement> getSoftDeletedRecords(String schema, String entity, WorkUnit workUnit,
      List<Predicate> predicateList) throws DataRecordException {
    return this.getRecordSet(schema, entity, workUnit, predicateList);
  }

  /**
   * Login to salesforce
   * @return login status
   */
  public boolean bulkApiLogin() throws Exception {
    log.info("Authenticating salesforce bulk api");
    boolean success = false;
    String hostName = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME);
    String apiVersion = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_VERSION);
    if (Strings.isNullOrEmpty(apiVersion)) {
      apiVersion = "29.0";
    }

    String soapAuthEndPoint = hostName + SALESFORCE_SOAP_AUTH_SERVICE + "/" + apiVersion;
    try {
      ConnectorConfig partnerConfig = new ConnectorConfig();
      if (super.workUnitState.contains(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL)
          && !super.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL).isEmpty()) {
        partnerConfig.setProxy(super.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL),
            super.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT));
      }

      String securityToken = this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_SECURITY_TOKEN);
      String password = PasswordManager.getInstance(this.workUnitState)
          .readPassword(this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD));
      partnerConfig.setUsername(this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USERNAME));
      partnerConfig.setPassword(password + securityToken);
      partnerConfig.setAuthEndpoint(soapAuthEndPoint);
      new PartnerConnection(partnerConfig);
      String soapEndpoint = partnerConfig.getServiceEndpoint();
      String restEndpoint = soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/")) + "async/" + apiVersion;

      ConnectorConfig config = new ConnectorConfig();
      config.setSessionId(partnerConfig.getSessionId());
      config.setRestEndpoint(restEndpoint);
      config.setCompression(true);
      config.setTraceFile("traceLogs.txt");
      config.setTraceMessage(false);
      config.setPrettyPrintXml(true);

      if (super.workUnitState.contains(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL)
          && !super.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL).isEmpty()) {
        config.setProxy(super.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL),
            super.workUnitState.getPropAsInt(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT));
      }

      this.bulkConnection = new BulkConnection(config);
      success = true;
    } catch (RuntimeException e) {
      throw new RuntimeException("Failed to connect to salesforce bulk api; error - " + e, e);
    }
    return success;
  }

  /**
   * Get Record set using salesforce specific API(Bulk API)
   * @param schema/databasename
   * @param entity/tablename
   * @param list of all predicate conditions
     * @return iterator with batch of records
   */
  private List<String> getQueryResultIds(String entity, List<Predicate> predicateList) throws Exception {
    if (!bulkApiLogin()) {
      throw new IllegalArgumentException("Invalid Login");
    }

    try {
      // Set bulk job attributes
      this.bulkJob.setObject(entity);
      this.bulkJob.setOperation(OperationEnum.query);
      this.bulkJob.setConcurrencyMode(ConcurrencyMode.Parallel);

      // Result type as CSV
      this.bulkJob.setContentType(ContentType.CSV);

      this.bulkJob = this.bulkConnection.createJob(this.bulkJob);
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

      log.info("QUERY:" + query);
      ByteArrayInputStream bout = new ByteArrayInputStream(query.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING));

      this.bulkBatchInfo = this.bulkConnection.createBatchFromStream(this.bulkJob, bout);

      int retryInterval = 30 + (int) Math.ceil((float) this.getExpectedRecordCount() / 10000) * 2;
      log.info("Salesforce bulk api retry interval in seconds:" + retryInterval);

      // Get batch info with complete resultset (info id - refers to the resultset id corresponding to entire resultset)
      this.bulkBatchInfo = this.bulkConnection.getBatchInfo(this.bulkJob.getId(), this.bulkBatchInfo.getId());

      while ((this.bulkBatchInfo.getState() != BatchStateEnum.Completed)
          && (this.bulkBatchInfo.getState() != BatchStateEnum.Failed)) {
        Thread.sleep(retryInterval * 1000);
        this.bulkBatchInfo = this.bulkConnection.getBatchInfo(this.bulkJob.getId(), this.bulkBatchInfo.getId());
        log.debug("Bulk Api Batch Info:" + this.bulkBatchInfo);
        log.info("Waiting for bulk resultSetIds");
      }

      if (this.bulkBatchInfo.getState() == BatchStateEnum.Failed) {
        log.error("Bulk batch failed: " + bulkBatchInfo.toString());
        throw new RuntimeException("Failed to get bulk batch info for jobId " + this.bulkBatchInfo.getJobId()
            + " error - " + this.bulkBatchInfo.getStateMessage());
      }

      // Get resultset ids from the batch info
      QueryResultList list = this.bulkConnection.getQueryResultList(this.bulkJob.getId(), this.bulkBatchInfo.getId());

      return Arrays.asList(list.getResult());

    } catch (RuntimeException | AsyncApiException | InterruptedException e) {
      throw new RuntimeException(
          "Failed to get query result ids from salesforce using bulk api; error - " + e.getMessage(), e);
    }
  }

  /**
   * Get data from the bulk api input stream
     * @return record set with each record as a JsonObject
   */
  private RecordSet<JsonElement> getBulkData() throws DataRecordException {
    log.debug("Processing bulk api batch...");
    RecordSetList<JsonElement> rs = new RecordSetList<>();

    try {
      // if Buffer is empty then get stream for the new resultset id
      if (this.bulkBufferedReader == null || !this.bulkBufferedReader.ready()) {

        // if there is unprocessed resultset id then get result stream for that id
        if (this.bulkResultIdCount < this.bulkResultIdList.size()) {
          log.info("Stream resultset for resultId:" + this.bulkResultIdList.get(this.bulkResultIdCount));
          this.setNewBulkResultSet(true);
          this.bulkBufferedReader =
              new BufferedReader(
                  new InputStreamReader(
                      this.bulkConnection.getQueryResultStream(this.bulkJob.getId(), this.bulkBatchInfo.getId(),
                          this.bulkResultIdList.get(this.bulkResultIdCount)),
                      ConfigurationKeys.DEFAULT_CHARSET_ENCODING));

          this.bulkResultIdCount++;
        } else {
          // if result stream processed for all resultset ids then finish the bulk job
          log.info("Bulk job is finished");
          this.setBulkJobFinished(true);
          return rs;
        }
      }

      // if Buffer stream has data then process the same

      // Get batch size from .pull file
      int batchSize = Utils.getAsInt(this.workUnitState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_FETCH_SIZE));
      if (batchSize == 0) {
        batchSize = ConfigurationKeys.DEFAULT_SOURCE_FETCH_SIZE;
      }

      // Stream the resultset through CSV reader to identify columns in each record
      InputStreamCSVReader reader = new InputStreamCSVReader(this.bulkBufferedReader);

      // Get header if it is first run of a new resultset
      if (this.isNewBulkResultSet()) {
        this.bulkRecordHeader = reader.nextRecord();
        this.bulkResultColumCount = this.bulkRecordHeader.size();
        this.setNewBulkResultSet(false);
      }

      List<String> csvRecord;
      int recordCount = 0;

      // Get record from CSV reader stream
      while ((csvRecord = reader.nextRecord()) != null) {
        // Convert CSV record to JsonObject
        JsonObject jsonObject = Utils.csvToJsonObject(this.bulkRecordHeader, csvRecord, this.bulkResultColumCount);
        rs.add(jsonObject);
        recordCount++;
        this.bulkRecordCount++;

        // Insert records in record set until it reaches the batch size
        if (recordCount >= batchSize) {
          log.info("Total number of records processed so far: " + this.bulkRecordCount);
          break;
        }
      }

    } catch (Exception e) {
      throw new DataRecordException("Failed to get records from salesforce; error - " + e.getMessage(), e);
    }

    return rs;
  }

  @Override
  public void closeConnection() throws Exception {
    if (this.bulkConnection != null
        && !this.bulkConnection.getJobStatus(this.bulkJob.getId()).getState().toString().equals("Closed")) {
      log.info("Closing salesforce bulk job connection");
      this.bulkConnection.closeJob(this.bulkJob.getId());
    }
  }

  public static List<Command> constructGetCommand(String restQuery) {
    return Arrays.asList(new RestApiCommand().build(Arrays.asList(restQuery), RestApiCommandType.GET));
  }

}
