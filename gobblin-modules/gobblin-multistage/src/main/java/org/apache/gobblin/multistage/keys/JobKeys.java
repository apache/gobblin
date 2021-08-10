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

package org.apache.gobblin.multistage.keys;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.util.DateTimeUtils;
import org.apache.gobblin.multistage.util.HdfsReader;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.apache.gobblin.multistage.util.JsonUtils;
import org.apache.gobblin.multistage.util.ParameterTypes;
import org.apache.gobblin.multistage.util.WorkUnitPartitionTypes;
import org.joda.time.DateTime;


/**
 * This class holds static Job parameters and it is initialized in the Source as part of
 * planning process, yet it can contain destination parameters as well in a egress scenario.
 *
 * Each of these keys provide information how to populate corresponding values in protocol
 * sub-classes. Each protocol is responsible for proper usage of these keys.
 *
 * The JobKeys class has 3 categories of functions:
 * 1. parsing: parse the complex job properties
 * 2. validating: validate job properties
 * 3. logging: log configurations
 *
 * @author chrli
 */

@Slf4j
@Getter(AccessLevel.PUBLIC)
@Setter(AccessLevel.PUBLIC)
public class JobKeys {
  final static public Gson GSON = new Gson();
  final static public List<MultistageProperties> ESSENTIAL_PARAMETERS = Lists.newArrayList(
      MultistageProperties.SOURCE_CLASS,
      MultistageProperties.EXTRACTOR_CLASSES,
      MultistageProperties.CONVERTER_CLASSES,
      MultistageProperties.EXTRACT_IS_FULL,
      MultistageProperties.EXTRACT_TABLE_TYPE_KEY,
      MultistageProperties.STATE_STORE_ENABLED,
      MultistageProperties.MSTAGE_ABSTINENT_PERIOD_DAYS,
      MultistageProperties.MSTAGE_DERIVED_FIELDS,
      MultistageProperties.MSTAGE_ENABLE_CLEANSING,
      MultistageProperties.MSTAGE_ENABLE_DYNAMIC_FULL_LOAD,
      MultistageProperties.MSTAGE_ENABLE_SCHEMA_BASED_FILTERING,
      MultistageProperties.MSTAGE_ENCODING,
      MultistageProperties.MSTAGE_ENCRYPTION_FIELDS,
      MultistageProperties.MSTAGE_GRACE_PERIOD_DAYS,
      MultistageProperties.MSTAGE_KRAKEN_ENABLED,
      MultistageProperties.MSTAGE_OUTPUT_SCHEMA,
      MultistageProperties.MSTAGE_PAGINATION,
      MultistageProperties.MSTAGE_PARAMETERS,
      MultistageProperties.MSTAGE_RETENTION,
      MultistageProperties.MSTAGE_SECONDARY_INPUT,
      MultistageProperties.MSTAGE_SESSION_KEY_FIELD,
      MultistageProperties.MSTAGE_SOURCE_DATA_CHARACTER_SET,
      MultistageProperties.MSTAGE_SOURCE_SCHEMA_URN,
      MultistageProperties.MSTAGE_SOURCE_URI,
      MultistageProperties.MSTAGE_TOTAL_COUNT_FIELD,
      MultistageProperties.MSTAGE_WAIT_TIMEOUT_SECONDS,
      MultistageProperties.MSTAGE_WORK_UNIT_PACING_SECONDS,
      MultistageProperties.MSTAGE_WORK_UNIT_PARALLELISM_MAX,
      MultistageProperties.MSTAGE_WORK_UNIT_PARTIAL_PARTITION,
      MultistageProperties.MSTAGE_WATERMARK);
  final private static String KEY_WORD_ACTIVATION = "activation";
  final private static String KEY_WORD_AUTHENTICATION = "authentication";
  final private static String KEY_WORD_CATEGORY = "category";
  final private static String KEY_WORD_COLUMN_NAME = "columnName";
  final private static String KEY_WORD_ITEMS = "items";
  final private static String KEY_WORD_RETRY = "retry";
  final private static String KEY_WORD_RETRY_DELAY_IN_SEC = "delayInSec";
  final private static String KEY_WORD_RETRY_COUNT = "retryCount";
  final private static String KEY_WORD_SNAPSHOT_ONLY = "SNAPSHOT_ONLY";
  final private static int RETRY_DELAY_IN_SEC_DEFAULT = 300;
  final private static int RETRY_COUNT_DEFAULT = 3;

  private Map<String, Map<String, String>> derivedFields = new HashMap<>();
  private Map<String, String> defaultFieldTypes = new HashMap<>();

  // sourceSchema is the schema provided or retrieved from source
  private JsonSchema sourceSchema = new JsonSchema();

  // outputSchema is the schema to be supplied to converters
  private JsonSchema outputSchema = new JsonSchema();
  private JsonObject sessionKeyField = new JsonObject();
  private String totalCountField = StringUtils.EMPTY;
  private JsonArray sourceParameters = new JsonArray();
  private Map<ParameterTypes, String> paginationFields = new HashMap<>();
  private Map<ParameterTypes, Long> paginationInitValues = new HashMap<>();
  private long sessionTimeout;
  private long callInterval;
  private JsonArray encryptionField = new JsonArray();
  private boolean enableCleansing;
  String dataField = StringUtils.EMPTY;
  private JsonArray watermarkDefinition = new JsonArray();
  private long retryDelayInSec;
  private long retryCount;
  private Boolean isPartialPartition;
  private JsonArray secondaryInputs = new JsonArray();
  private WorkUnitPartitionTypes workUnitPartitionType;
  private Boolean isSecondaryAuthenticationEnabled = false;

  public void initialize(State state) {
    parsePaginationFields(state);
    parsePaginationInitialValues(state);
    setSessionKeyField(MultistageProperties.MSTAGE_SESSION_KEY_FIELD.getValidNonblankWithDefault(state));
    setTotalCountField(MultistageProperties.MSTAGE_TOTAL_COUNT_FIELD.getValidNonblankWithDefault(state));
    setSourceParameters(MultistageProperties.MSTAGE_PARAMETERS.getValidNonblankWithDefault(state));
    setDefaultFieldTypes(parseDefaultFieldTypes(state));
    setDerivedFields(parseDerivedFields(state));
    setOutputSchema(parseOutputSchema(state));
    setEncryptionField(MultistageProperties.MSTAGE_ENCRYPTION_FIELDS.getValidNonblankWithDefault(state));
    setDataField(MultistageProperties.MSTAGE_DATA_FIELD.getValidNonblankWithDefault(state));
    setCallInterval(MultistageProperties.MSTAGE_CALL_INTERVAL.getProp(state));
    setSessionTimeout(MultistageProperties.MSTAGE_WAIT_TIMEOUT_SECONDS.getMillis(state));
    setEnableCleansing(MultistageProperties.MSTAGE_ENABLE_CLEANSING.getValidNonblankWithDefault(state));
    setIsPartialPartition(MultistageProperties.MSTAGE_WORK_UNIT_PARTIAL_PARTITION.getValidNonblankWithDefault(state));
    setWorkUnitPartitionType(parsePartitionType(state));
    setWatermarkDefinition(MultistageProperties.MSTAGE_WATERMARK.getValidNonblankWithDefault(state));
    Map<String, Long> retry = parseSecondaryInputRetry(
        MultistageProperties.MSTAGE_SECONDARY_INPUT.getValidNonblankWithDefault(state));
    setRetryDelayInSec(retry.get(KEY_WORD_RETRY_DELAY_IN_SEC));
    setRetryCount(retry.get(KEY_WORD_RETRY_COUNT));
    setSecondaryInputs(MultistageProperties.MSTAGE_SECONDARY_INPUT.getValidNonblankWithDefault(state));
    setIsSecondaryAuthenticationEnabled(checkSecondaryAuthenticationEnabled());
  }


  public boolean isPaginationEnabled() {
    // if a pagination key or an initial value is defined, then we have pagination enabled.
    // this flag will impact how session be handled, and each protocol can implement it
    // accordingly
    return paginationFields.size() > 0 || paginationInitValues.size() > 0;
  };

  public boolean isSessionStateEnabled() {
    return sessionKeyField != null
        && sessionKeyField.entrySet().size() > 0
        && sessionKeyField.has("condition")
        && sessionKeyField.get("condition").getAsJsonObject().has("regexp");
  }

  public String getSessionStateCondition() {
    if (isSessionStateEnabled()) {
      return sessionKeyField.get("condition").getAsJsonObject().get("regexp").getAsString();
    }
    return StringUtils.EMPTY;
  }

  /**
   * failCondition is optional in the definition
   * @return failCondition if it is defined
   */
  public String getSessionStateFailCondition() {
    String retValue = StringUtils.EMPTY;
    if (isSessionStateEnabled()) {
      try {
        retValue = sessionKeyField.get("failCondition").getAsJsonObject().get("regexp").getAsString();
      } catch (Exception e) {
        log.debug("failCondition is not defined: {}", sessionKeyField);
      }
    }
    return retValue;
  }

  public boolean hasSourceSchema() {
    return sourceSchema.getSchema().entrySet().size() > 0;
  }

  public boolean hasOutputSchema() {
    return outputSchema.getSchema().entrySet().size() > 0;
  }

  /**
   * override the setter and update output schema when source schema is available
   * @param sourceSchema source provided schema
   */
  public void setSourceSchema(JsonSchema sourceSchema) {
    this.sourceSchema = sourceSchema;
    JsonArray schemaArray = JsonUtils.deepCopy(
        sourceSchema.getAltSchema(defaultFieldTypes, enableCleansing)).getAsJsonArray();
    if (schemaArray.size() > 0) {
      outputSchema.addMember("items", schemaArray);
    }
    log.debug("Source Schema: {}", sourceSchema.getSchema().toString());
    log.debug("Output Schema: {}", outputSchema.getSchema().toString());
  }

  /**
   * Validate the configuration
   * @param state configuration state
   * @return true if validation was successful, otherwise false
   */
  public boolean validate(State state) {
    /**
     * If pagination is enabled,  we need one of following ways to stop pagination
     *  1. through a total count field, i.e. ms.total.count.field = data.
     *    This doesn't validate the correctness of the field. The correctness of this
     *    field will be validated at extraction time in extractor classes
     *  2. through a session cursor with a stop condition,
     *    i.e. ms.session.key.field = {"name": "status", "condition": {"regexp": "success"}}.
     *    This doesn't validate whether the stop condition can truly be met.
     *    If a condition cannot be met because of incorrect specification, eventually
     *    it will timeout and fail the task.
     *  3. through a condition that will eventually lead to a empty response from the source
     *    This condition cannot be done through a static check, therefore, here only a warning is
     *    provided.
     */
    if (isPaginationEnabled()) {
      if (totalCountField == null && !isSessionStateEnabled()) {
        log.warn("Pagination is enabled, but there is no total count field or session \n"
            + "control to stop it. Pagination will stop only when a blank page is returned from source. \n"
            + "Please check the configuration of essential parameters if such condition can happen.");
      }
    }

    /**
     * Check if output schema is correct.
     * When a string is present but cannot be parsed, log an error.
     * It is OK if output schema is intentionally left blank.
     */
    if (!hasOutputSchema()) {
      if (!state.getProp(MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getConfig(), StringUtils.EMPTY).isEmpty()) {
        log.error("Output schema is specified but it is an invalid or empty JsonArray");
        return false;
      }
    }

    /**
     * Check if partitioning property is correct
     */
    if (getWorkUnitPartitionType() == null) {
      String partTypeString = state.getProp(MultistageProperties.MSTAGE_WORK_UNIT_PARTITION.getConfig());
      if (!StringUtils.isBlank(partTypeString)) {
        log.error("ms.work.unit.partition has a unaccepted value: {}", partTypeString);
        return false;
      }
    } else if (getWorkUnitPartitionType() == WorkUnitPartitionTypes.COMPOSITE) {
      /**
       * for a broad range like this, it must generate at least 1 partition, otherwise
       * the partitioning ranges must have incorrect date strings
       */
      if (WorkUnitPartitionTypes.COMPOSITE.getRanges(
          DateTime.parse("2001-01-01"),
          DateTime.now(), true).size() < 1) {
        log.error("ms.work.unit.partition has incorrect or non-ISO-formatted date time values");
        return false;
      }
    }
    // TODO other checks
    // TODO validate master key location
    // TODO validate secondary input structure
    // TODO validate watermark structure
    // TODO validate parameters structure
    // TODO validate authentication structure

    return true;
  }

  public void logDebugAll() {
    log.debug("These are values in MultistageSource");
    log.debug("Total count field: {}", totalCountField);
    log.debug("Pagination: fields {}, initial values {}", paginationFields.toString(), paginationInitValues.toString());
    log.debug("Session key field definition: {}", sessionKeyField.toString());
    log.debug("Call interval in milliseconds: {}", callInterval);
    log.debug("Session timeout: {}", sessionTimeout);
    log.debug("Derived fields definition: {}", derivedFields.toString());
    log.debug("Output schema definition: {}", outputSchema.toString());
    log.debug("Watermark definition: {}", watermarkDefinition.toString());
    log.debug("Encrypted fields: {}", encryptionField);
    log.debug("Retry Delay: {}", retryDelayInSec);
    log.debug("Retry Count: {}", retryCount);
  }

  public void logUsage(State state) {
    for (MultistageProperties p: ESSENTIAL_PARAMETERS) {
      log.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getValidNonblankWithDefault(state));
    }
  }

  private void parsePaginationFields(State state) {
    List<ParameterTypes> paramTypes = Lists.newArrayList(
        ParameterTypes.PAGESTART,
        ParameterTypes.PAGESIZE,
        ParameterTypes.PAGENO
    );
    if (MultistageProperties.MSTAGE_PAGINATION.validateNonblank(state)) {
      JsonObject p = MultistageProperties.MSTAGE_PAGINATION.getProp(state);
      if (p.has("fields")) {
        JsonArray fields = p.get("fields").getAsJsonArray();
        for (int i = 0; i < fields.size(); i++) {
          if (StringUtils.isNoneBlank(fields.get(i).getAsString())) {
            paginationFields.put(paramTypes.get(i), fields.get(i).getAsString());
          }
        }
      }
    }
  }

  private void parsePaginationInitialValues(State state) {
    List<ParameterTypes> paramTypes = Lists.newArrayList(
        ParameterTypes.PAGESTART,
        ParameterTypes.PAGESIZE,
        ParameterTypes.PAGENO
    );
    if (MultistageProperties.MSTAGE_PAGINATION.validateNonblank(state)) {
      JsonObject p = MultistageProperties.MSTAGE_PAGINATION.getProp(state);
      if (p.has("initialvalues")) {
        JsonArray values = p.get("initialvalues").getAsJsonArray();
        for (int i = 0; i < values.size(); i++) {
          paginationInitValues.put(paramTypes.get(i), values.get(i).getAsLong());
        }
      }
    } else {
      setPaginationInitValues(new HashMap<>());
    }
  }

  /**
   * Default field types can be used in schema inferrence, this method
   * collect default field types if they are  specified in configuration.
   *
   * @return A map of fields and their default types
   */
  private Map<String, String> parseDefaultFieldTypes(State state) {
    if (MultistageProperties.MSTAGE_DATA_DEFAULT_TYPE.validateNonblank(state)) {
      return GSON.fromJson(MultistageProperties.MSTAGE_DATA_DEFAULT_TYPE.getProp(state).toString(),
          new TypeToken<HashMap<String, String>>() {
          }.getType());
    }
    return new HashMap<>();
  }

  /**
   * Sample derived field configuration:
   * [{"name": "activityDate", "formula": {"type": "epoc", "source": "fromDateTime", "format": "yyyy-MM-dd'T'HH:mm:ss'Z'"}}]
   *
   * Currently, only "epoc" and "string" are supported as derived field type.
   * For epoc type:
   * - Data will be saved as milliseconds in long data type.
   * - And the source data is supposed to be a date formatted as a string.
   *
   * TODO: support more types.
   *
   * @return derived fields and their definitions
   */
  @VisibleForTesting
  Map<String, Map<String, String>> parseDerivedFields(State state) {
    if (!MultistageProperties.MSTAGE_DERIVED_FIELDS.validateNonblank(state)) {
      return new HashMap<>();
    }

    Map<String, Map<String, String>> derivedFields = new HashMap<>();
    JsonArray jsonArray = MultistageProperties.MSTAGE_DERIVED_FIELDS.getProp(state);
    for (JsonElement field: jsonArray) {

      // change the formula part, which is JsonObject, into map
      derivedFields.put(field.getAsJsonObject().get("name").getAsString(),
          GSON.fromJson(
              field.getAsJsonObject().get("formula").getAsJsonObject().toString(),
              new TypeToken<HashMap<String, String>>() { }.getType()));
    }

    return derivedFields;
  }

  /**
   * parse output schema defined in ms.output.schema parameter
   *
   * @param state the Gobblin configurations
   * @return the output schema
   */
  public JsonSchema parseOutputSchema(State state) {
    if (MultistageProperties.MSTAGE_OUTPUT_SCHEMA.validateNonblank(state)) {
      JsonArray schema = MultistageProperties.MSTAGE_OUTPUT_SCHEMA.getProp(state);
      return new JsonSchema().addMember(KEY_WORD_ITEMS,
          JsonUtils.deepCopy(schema).getAsJsonArray());
    } else {
      return new JsonSchema();
    }
  }


  /**
   * This helper function parse out the WorkUnitPartitionTypes from ms.work.unit.partition property
   * @param state the State with all configurations
   * @return the WorkUnitPartitionTypes
   */
  WorkUnitPartitionTypes parsePartitionType(State state) {
    WorkUnitPartitionTypes partitionType = WorkUnitPartitionTypes.fromString(
        MultistageProperties.MSTAGE_WORK_UNIT_PARTITION.getValidNonblankWithDefault(state));

    if (partitionType != WorkUnitPartitionTypes.COMPOSITE) {
      return partitionType;
    }

    // add sub ranges for composite partition type
    WorkUnitPartitionTypes.COMPOSITE.resetSubRange();
    try {
      JsonObject jsonObject = GSON.fromJson(
          MultistageProperties.MSTAGE_WORK_UNIT_PARTITION.getValidNonblankWithDefault(state).toString(),
          JsonObject.class);

      for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
        String partitionTypeString = entry.getKey();
        DateTime start = DateTimeUtils.parse(jsonObject.get(entry.getKey()).getAsJsonArray().get(0).getAsString());
        String endDateTimeString = jsonObject.get(entry.getKey()).getAsJsonArray().get(1).getAsString();
        DateTime end;
        if (endDateTimeString.matches("-")) {
          end = DateTime.now();
        } else {
          end = DateTimeUtils.parse(endDateTimeString);
        }
        partitionType.addSubRange(start, end, WorkUnitPartitionTypes.fromString(partitionTypeString));
      }
    } catch (Exception e) {
      log.error("Error parsing composite partition string: "
              + MultistageProperties.MSTAGE_WORK_UNIT_PARTITION.getValidNonblankWithDefault(state).toString()
              + "\n partitions may not be generated properly.",
          e);
    }
    return partitionType;
  }

  /**
   *  This method populates the retry parameters (delayInSec, retryCount) via the secondary input.
   *   These values are used to retry connection whenever the "authentication" type category is defined and the token hasn't
   *   been populated yet. If un-defined, they will retain the default values as specified by RETRY_DEFAULT_DELAY and
   *   RETRY_DEFAULT_COUNT.
   *
   *   For e.g.
   *   ms.secondary.input : "[{"path": "/util/avro_retry", "fields": ["uuid"],
   *   "category": "authentication", "retry": {"delayInSec" : "1", "retryCount" : "2"}}]"
   * @param jsonArray the raw secondary input
   * @return the retry delay and count in a map structure
   */
  private Map<String, Long> parseSecondaryInputRetry(JsonArray jsonArray) {
    long retryDelay = RETRY_DELAY_IN_SEC_DEFAULT;
    long retryCount = RETRY_COUNT_DEFAULT;
    Map<String, Long> retry = new HashMap<>();
    for (JsonElement field: jsonArray) {
      JsonObject retryFields = new JsonObject();
      retryFields = (JsonObject) field.getAsJsonObject().get(KEY_WORD_RETRY);
      if (retryFields != null && !retryFields.isJsonNull()) {
        retryDelay = retryFields.has(KEY_WORD_RETRY_DELAY_IN_SEC)
            ? retryFields.get(KEY_WORD_RETRY_DELAY_IN_SEC).getAsLong() : retryDelay;
        retryCount = retryFields.has(KEY_WORD_RETRY_COUNT)
            ? retryFields.get(KEY_WORD_RETRY_COUNT).getAsLong() : retryCount;
      }
    }
    retry.put(KEY_WORD_RETRY_DELAY_IN_SEC, retryDelay);
    retry.put(KEY_WORD_RETRY_COUNT, retryCount);
    return retry;
  }

  /**
   * check if authentication is configured in secondary input
   * @return true if secondary input contains an authentication definition
   */
  protected boolean checkSecondaryAuthenticationEnabled() {
    for (JsonElement entry: getSecondaryInputs()) {
      if (entry.isJsonObject()
          && entry.getAsJsonObject().has(KEY_WORD_CATEGORY)
          && entry.getAsJsonObject().get(KEY_WORD_CATEGORY).getAsString()
          .equalsIgnoreCase(KEY_WORD_AUTHENTICATION)) {
        return true;
      }
    }
    return false;
  }

  public Map<String, JsonArray> readSecondaryInputs(State state, final long retries) throws InterruptedException {
    log.info("Trying to read secondary input with retry = {}", retries);
    Map<String, JsonArray> secondaryInputs = readContext(state);

    // Check if authentication is ready, and if not, whether retry is required
    JsonArray authentications = secondaryInputs.get(KEY_WORD_AUTHENTICATION);
    if ((authentications == null || authentications.size() == 0) && this.getIsSecondaryAuthenticationEnabled()
        && retries > 0) {
      log.info("Authentication tokens are expected from secondary input, but not ready");
      log.info("Will wait for {} seconds and then retry reading the secondary input", this.getRetryDelayInSec());
      TimeUnit.SECONDS.sleep(this.getRetryDelayInSec());
      return readSecondaryInputs(state, retries - 1);
    }
    log.info("Successfully read secondary input, no more retry");
    return secondaryInputs;
  }

  private Map<String, JsonArray> readContext(State state) {
    return new HdfsReader(state, this.getSecondaryInputs()).readAll();
  }
}
