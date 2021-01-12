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

package org.apache.gobblin.multistage.extractor;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.connection.MultistageReadConnection;
import org.apache.gobblin.multistage.exception.RetriableAuthenticationException;
import org.apache.gobblin.multistage.filter.JsonSchemaBasedFilter;
import org.apache.gobblin.multistage.filter.MultistageSchemaBasedFilter;
import org.apache.gobblin.multistage.keys.ExtractorKeys;
import org.apache.gobblin.multistage.keys.JobKeys;
import org.apache.gobblin.multistage.preprocessor.StreamProcessor;
import org.apache.gobblin.multistage.util.DateTimeUtils;
import org.apache.gobblin.multistage.util.EncryptionUtils;
import org.apache.gobblin.multistage.util.InputStreamUtils;
import org.apache.gobblin.multistage.util.JsonIntermediateSchema;
import org.apache.gobblin.multistage.util.JsonParameter;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.apache.gobblin.multistage.util.JsonUtils;
import org.apache.gobblin.multistage.util.ParameterTypes;
import org.apache.gobblin.multistage.util.VariableUtils;
import org.apache.gobblin.multistage.util.WorkUnitStatus;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.hadoop.util.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * MulistageExtractor is the base class of other format specific Extractors.
 *
 * The base class only defines function to deal with work units and activation parameters
 *
 * @author chrli
 * @param <S> The schema class
 * @param <D> The data class
 */
@Slf4j
public class MultistageExtractor<S, D> implements Extractor<S, D> {
  final private static Gson GSON = new Gson();
  protected final static String ITEMS_KEY = "items";
  protected final static String CURRENT_DATE = "currentdate";
  protected final static String PXD = "P\\d+D";
  protected final static List<String> SUPPORTED_DERIVED_FIELD_TYPES =
      Arrays.asList("epoc", "string", "integer", "number");
  @Getter(AccessLevel.PUBLIC)
  @Setter(AccessLevel.PACKAGE)
  protected WorkUnitStatus workUnitStatus = WorkUnitStatus.builder().build();

  @Getter(AccessLevel.PUBLIC)
  protected WorkUnitState state = null;

  @Getter ExtractorKeys extractorKeys = new ExtractorKeys();
  @Getter JsonObject currentParameters = null;

  protected MultistageSchemaBasedFilter<?> rowFilter = null;

  @Getter @Setter
  MultistageReadConnection connection = null;
  @Getter @Setter
  JobKeys jobKeys;

  public MultistageExtractor(WorkUnitState state, JobKeys jobKeys) {
    this.state = state;
    this.jobKeys = jobKeys;
  }

  protected void initialize(ExtractorKeys keys) {
    extractorKeys = keys;
    extractorKeys.setActivationParameters(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.getValidNonblankWithDefault(state));
    extractorKeys.setDelayStartTime(MultistageProperties.MSTAGE_WORKUNIT_STARTTIME_KEY.getProp(state));
    extractorKeys.setSignature(MultistageProperties.DATASET_URN_KEY.getProp(state));
    extractorKeys.setPreprocessors(getPreprocessors(state));
    extractorKeys.logDebugAll(state.getWorkunit());
  }

  @Override
  public S getSchema() {
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    return 0;
  }

  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Nullable
  @Override
  public D readRecord(D reuse) {
    return null;
  }

  @Override
  public void close() {
    if (state.getWorkingState().equals(WorkUnitState.WorkingState.SUCCESSFUL)) {
      state.setActualHighWatermark(state.getWorkunit().getExpectedHighWatermark(LongWatermark.class));
    }
    if (connection != null) {
      connection.closeAll(org.apache.commons.lang3.StringUtils.EMPTY);
    }
  }

  /**
   * Core data extract function that calls the Source to obtain an InputStream and then
   * decode the InputStream to records.
   *
   * @param starting the starting position of this extract, which mostly means the actual records
   *                 that have been extracted previously
   * @return false if no more data to be pulled or an significant error that requires early job termination
   */
  protected boolean processInputStream(long starting) {
    holdExecutionUnitPresetStartTime();

    if (isWorkUnitCompleted(starting)) {
      return false;
    }

    currentParameters = isFirst(starting)
        ? getInitialWorkUnitParameters() : getCurrentWorkUnitParameters();
    extractorKeys.setDynamicParameters(currentParameters);

    WorkUnitStatus updatedStatus = null;
    long retryies = Math.max(jobKeys.getRetryCount(), 1);
    while (retryies > 0) {
      try {
        updatedStatus = connection == null ? null : isFirst(starting)
            ? connection.getFirst(this.workUnitStatus) : connection.getNext(this.workUnitStatus);
        retryies = 0;
      } catch (RetriableAuthenticationException e) {
        // TODO update sourceKeys
        retryies--;
      }
    }

    if (updatedStatus == null) {
      this.failWorkUnit("Received a NULL WorkUnitStatus, fail the work unit");
      return false;
    }
    // update work unit status
    workUnitStatus.setBuffer(updatedStatus.getBuffer());
    workUnitStatus.setMessages(updatedStatus.getMessages());
    workUnitStatus.setSessionKey(getSessionKey(updatedStatus));

    // update extractor key
    extractorKeys.setSessionKeyValue(workUnitStatus.getSessionKey());

    // read source schema from the message if available
    if (jobKeys != null
        && !jobKeys.hasSourceSchema()
        && !jobKeys.hasOutputSchema()) {
      jobKeys.setSourceSchema(workUnitStatus.getSchema());
    }
    return true;
  }

  /**
   * Initialize row filter; by default, json schema based filter is used
   * @param schemaArray schema array
   */
  protected void setRowFilter(JsonArray schemaArray) {
    if (rowFilter == null) {
      if (MultistageProperties.MSTAGE_ENABLE_SCHEMA_BASED_FILTERING.getValidNonblankWithDefault(state)) {
        rowFilter = new JsonSchemaBasedFilter(new JsonIntermediateSchema(schemaArray));
      }
    }
  }

  /**
   * returns the schema definition from properties, or if not definition not present, returns the inferred schema
   *
   * @return the schema definition in a JsonArray
   */
  JsonArray getOrInferSchema() {
    if (this.jobKeys.getOutputSchema().getSchema().entrySet().size() == 0) {
      if (!processInputStream(0)) {
        return createMinimumSchema();
      }
    }

    JsonArray schemaArray = new JsonArray();
    if (this.jobKeys.hasOutputSchema()) {
      // take pre-defined fixed schema
      JsonObject schemaObject = JsonUtils.deepCopy(jobKeys.getOutputSchema().getSchema()).getAsJsonObject();
      schemaArray = schemaObject.get(ITEMS_KEY).getAsJsonArray();
      setRowFilter(schemaArray);
    } else {
      if (this.jobKeys.hasSourceSchema()) {
        schemaArray = this.jobKeys.getSourceSchema().getAltSchema(jobKeys.getDefaultFieldTypes(),
            jobKeys.isEnableCleansing());
        schemaArray = JsonUtils.deepCopy(schemaArray).getAsJsonArray();
        log.info("Source provided schema: {}", schemaArray.toString());
      } else if (extractorKeys.getInferredSchema() != null
          && !extractorKeys.getInferredSchema().getSchema().get("type").getAsString().equalsIgnoreCase("null")) {
        // schema inference is only possible when there is some data
        // otherwise the inferred schema type will be null
        schemaArray = extractorKeys.getInferredSchema().getAltSchema(jobKeys.getDefaultFieldTypes(),
            jobKeys.isEnableCleansing());
        schemaArray = JsonUtils.deepCopy(schemaArray).getAsJsonArray();
        log.info("Inferred schema: {}", schemaArray.toString());
      }
    }

    return schemaArray;
  }

  /**
   * Get the work unit watermarks from the work unit state
   *
   * the return value will have format like
   *
   * {"low": 123, "high": 456}
   *
   * @return the specified low and expected high wartermark in a JsonObject format
   */
  public JsonObject getWorkUnitWaterMarks() {
    Long lowWatermark = state.getWorkunit().getLowWatermark(LongWatermark.class).getValue();
    Long highWatermark = state.getWorkunit().getExpectedHighWatermark(LongWatermark.class).getValue();
    JsonObject watermark = new JsonObject();
    watermark.addProperty("low", lowWatermark);
    watermark.addProperty("high", highWatermark);
    return watermark;
  }

  /**
   *  a utility method to wait for its turn when multiple work units were started at the same time
   */
  protected void holdExecutionUnitPresetStartTime() {
    if (extractorKeys.getDelayStartTime() != 0) {
      while (DateTime.now().getMillis() < extractorKeys.getDelayStartTime()) {
        try {
          Thread.sleep(100L);
        } catch (Exception e) {
          log.warn(e.getMessage());
        }
      }
    }
  }

  /**
   * read preprocessor configuration and break it into an array of strings, and then
   * dynamically load each class and instantiate preprocessors.
   *
   * @param state the work unit state
   * @return a list of preprocessors
   */
  List<StreamProcessor<?>> getPreprocessors(State state) {
    ImmutableList.Builder<StreamProcessor<?>> builder = ImmutableList.builder();
    JsonObject preprocessorsParams =
        MultistageProperties.MSTAGE_EXTRACT_PREPROCESSORS_PARAMETERS.getValidNonblankWithDefault(state);
    String preprocessors = MultistageProperties.MSTAGE_EXTRACT_PREPROCESSORS.getValidNonblankWithDefault(state);
    JsonObject preprocessorParams;
    for (String preprocessor: preprocessors.split(StringUtils.COMMA_STR)) {
      String p = preprocessor.trim();
      if (!p.isEmpty()) {
        try {
          preprocessorParams = new JsonObject();
          if (preprocessorsParams.has(p)) {
            // Get the parameters for the given processor class
            preprocessorParams = preprocessorsParams.getAsJsonObject(p);

            // backward compatibility, by default create a decryption preprocessor
            if (p.contains("GpgProcessor")) {
              if (preprocessorParams.has("action")
                  && preprocessorParams.get("action").getAsString().equalsIgnoreCase("encrypt")) {
                p = p.replaceAll("GpgProcessor", "GpgEncryptProcessor");
              } else {
                p = p.replaceAll("GpgProcessor", "GpgDecryptProcessor");
              }
            }

            // Decrypt if any credential is encrypted
            for (Map.Entry<String, JsonElement> entry: preprocessorParams.entrySet()) {
              String key = entry.getKey();
              String value = preprocessorParams.get(key).getAsString();
              String decryptedValue = EncryptionUtils.decryptGobblin(value, state);
              preprocessorParams.addProperty(key, decryptedValue);
            }
          }
          Class<?> clazz = Class.forName(p);
          StreamProcessor<?> instance =
              (StreamProcessor<?>) clazz.getConstructor(JsonObject.class).newInstance(preprocessorParams);
          builder.add(instance);
        } catch (Exception e) {
          log.error("Error creating preprocessor: {}, Exception: {}", p, e.getMessage());
        }
      }
    }
    return builder.build();
  }

  /**
   * set work unit state to fail and log an error message as failure reason
   * @param error failure reason
   */
  protected void failWorkUnit(String error) {
    if (!Strings.isNullOrEmpty(error)) {
      log.error(error);
    }
    this.state.setWorkingState(WorkUnitState.WorkingState.FAILED);
  }

  /**
   * read the source and derive epoc from an existing field
   * @param format specified format of datetime string
   * @param strValue pre-fetched value from the data source
   * @return the epoc string: empty if failed to format strValue in the specified way
   */
  protected String deriveEpoc(String format, String strValue) {
    String epoc = "";
    // the source value must be a datetime string in the specified format
    try {
      DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern(format);
      DateTime dateTime = datetimeFormatter.parseDateTime(
          strValue.length() > format.length() ? strValue.substring(0, format.length()) : strValue);
      epoc = String.valueOf(dateTime.getMillis());
    } catch (IllegalArgumentException e) {
      try {
        epoc = String.valueOf(DateTimeUtils.parse(strValue).getMillis());
      } catch (Exception e1) {
        failWorkUnit(e1.getMessage() + e.getMessage());
      }
    }
    return epoc;
  }

  /***
   * Append the derived field definition to the output schema
   *
   * @return output schema with the added derived field
   */
  protected JsonArray addDerivedFieldsToAltSchema() {
    JsonArray columns = new JsonArray();
    /**
     * add derived fields
     *
     */
    for (Map.Entry<String, Map<String, String>> entry: jobKeys.getDerivedFields().entrySet()) {
      JsonObject column = new JsonObject();
      column.addProperty("columnName", entry.getKey());
      JsonObject dataType = new JsonObject();
      switch (entry.getValue().get("type")) {
        case "epoc":
          dataType.addProperty("type", "long");
          break;
        case "string":
        case "regexp":
          dataType.addProperty("type", "string");
          break;
        case "integer":
          dataType.addProperty("type", "integer");
          break;
        case "number":
          dataType.addProperty("type", "number");
          break;
        default:
          /**
           * by default take the source types
           */
          dataType.addProperty("type", jobKeys.getOutputSchema()
              .get("properties")
              .getAsJsonObject()
              .get(entry.getValue().get("source"))
              .getAsJsonObject()
              .get("type")
              .getAsString()
          );
          break;
      }
      column.add("dataType", dataType);
      columns.add(column);
    }
    return columns;
  }

  /**
   * Extract the text from input stream for scenarios where an error page is returned as successful response
   * @param input the InputStream, which most likely is from an HttpResponse
   * @return the String extracted from InputStream, if the InputStream cannot be converted to a String
   * then an exception should be logged in debug mode, and an empty string returned.
   */
  protected String extractText(InputStream input) {
    log.debug("Parsing response InputStream as Text");
    String data = "";
    if (input != null) {
      try {
        data = InputStreamUtils.extractText(input,
            MultistageProperties.MSTAGE_SOURCE_DATA_CHARACTER_SET.getValidNonblankWithDefault(state));
      } catch (Exception e) {
        log.debug(e.toString());
      }
    }
    return data;
  }

  /**
   *  If Content-Type is provided, but not as expected, the response can have
   *  useful error information
   *
   * @param wuStatus work unit status
   * @param expectedContentType expected content type
   * @return false if content type is present but not as expected otherwise true
   */
  protected boolean checkContentType(WorkUnitStatus wuStatus, String expectedContentType) {
    if (wuStatus.getMessages() != null
        && wuStatus.getMessages().containsKey("contentType")) {
      String contentType = wuStatus.getMessages().get("contentType");
      if (!contentType.equalsIgnoreCase(expectedContentType)) {
        log.info("Content is {}, expecting {}", contentType, expectedContentType);
        log.debug(extractText(wuStatus.getBuffer()));
        return false;
      }
    }
    return true;
  }

  /**
   * Retrieve session keys from the payload or header
   * @param wuStatus
   * @return the session key in the headers
   */
  protected String getSessionKey(WorkUnitStatus wuStatus) {
    if (wuStatus.getMessages() != null
        && wuStatus.getMessages().containsKey("headers")
        && jobKeys.getSessionKeyField() != null
        && jobKeys.getSessionKeyField().has("name")) {
      JsonObject headers = GSON.fromJson(wuStatus.getMessages().get("headers"), JsonObject.class);
      if (headers.has(this.jobKeys.getSessionKeyField().get("name").getAsString())) {
        return headers.get(this.jobKeys.getSessionKeyField().get("name").getAsString()).getAsString();
      }
    }
    return org.apache.commons.lang.StringUtils.EMPTY;
  }

  /**
   * Check if the work unit is completed
   * @param starting the starting position of the request
   * @return default is always false (not completed)
   */
  protected boolean isWorkUnitCompleted(long starting) {
    return false;
  }

  /**
   * If the position is 0, then it must be the first request
   * @param starting the starting position of the request
   * @return true if the starting position is 0, otherwise false
   */
  protected boolean isFirst(long starting) {
    return starting == 0;
  }

  /**
   * check if the stop condition has been met or if it should timeout,
   * however, when no condition is present, we assume no wait
   *
   * @return true if stop condition is met or it should timeout
   */
  protected boolean waitingBySessionKeyWithTimeout() {
    if (!jobKeys.isSessionStateEnabled() || isSessionStateMatch()) {
      return true;
    }

    // Fail if the session failCondition is met
    if (isSessionStateFailed()) {
      String message = String.format("Session fail condition is met: %s",
          jobKeys.getSessionStateFailCondition());
      log.warn(message);
      throw new RuntimeException(message);
    }

    // if stop condition is present but the condition has not been met, we
    // will check if the session should time out
    if (DateTime.now().getMillis() > extractorKeys.getStartTime()
        + jobKeys.getSessionTimeout()) {
      log.warn("Session time out after {} seconds", jobKeys.getSessionTimeout() / 1000);
      throw new RuntimeException("Session timed out before ending condition is met");
    }

    // return false to indicate wait should continue
    return false;
  }

  /**
   * Check if session state is enabled and session stop condition is met
   *
   * @return true if session state is enabled and session stop condition is met
   * otherwise return false
   */
  protected boolean isSessionStateMatch() {
    return jobKeys.isSessionStateEnabled()
        && extractorKeys.getSessionKeyValue().matches(jobKeys.getSessionStateCondition());
  }

  /**
   * Check if session state is enabled and session fail condition is met
   *
   * @return true if session state is enabled and session fail condition is met
   * otherwise return false
   */
  protected boolean isSessionStateFailed() {
    return jobKeys.isSessionStateEnabled()
        && extractorKeys.getSessionKeyValue().matches(jobKeys.getSessionStateFailCondition());
  }

  /**
   * This helper function determines whether to send a new pagination request. A new page
   * should be requested if:
   * 1. if session state control is enabled, then check if session stop condition is met or if timeout
   * 2. otherwise, check if pagination is enabled
   *
   * Sub-classes should further refine the new page condition.
   *
   * @return true if a new page should be requested
   */
  protected boolean hasNextPage() {
    try {
      if (jobKeys.isSessionStateEnabled()) {
        return !waitingBySessionKeyWithTimeout();
      } else {
        return jobKeys.isPaginationEnabled();
      }
    } catch (Exception e) {
      failWorkUnit(String.format("Timeout waiting for next page: %s", e.getMessage()));
      return false;
    }
  }

  /**
   * Utility function in the extractor to replace a variable
   * @param variableString variable string
   * @return actual value of a variable; empty string if variable not found
   */
  protected String replaceVariable(String variableString) {
    String finalString = "";
    try {
      finalString = VariableUtils.replaceWithTracking(
          variableString,
          currentParameters,
          false).getKey();
    } catch (IOException e) {
      failWorkUnit("Invalid parameter " + variableString);
    }
    return finalString;
  }

  /**
   * When there is no data return from the source, schema inferring will fail; however, Gobblin
   * will always call schema converter before record converter. When it does so in the event of
   * empty data, schema converter will fail.
   *
   * This function creates a dummy schema with primary keys and delta key to cheat converter
   *
   * @return the dummy schema with primary keys and delta keys
   */
  protected JsonArray createMinimumSchema() {
    JsonSchema dummySchema = new JsonSchema();
    if (state.contains(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY)) {
      String[] primaryKeys =
          state.getProp(ConfigurationKeys.EXTRACT_PRIMARY_KEY_FIELDS_KEY,
              org.apache.commons.lang3.StringUtils.EMPTY).split(StringUtils.COMMA_STR);
      for (String key: primaryKeys) {
        if (!key.isEmpty()) {
          dummySchema.addMember(key, new JsonSchema().addProperty("type", "string"));
        }
      }
    }
    if (state.contains(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY)) {
      String[] deltaKeys =
          state.getProp(ConfigurationKeys.EXTRACT_DELTA_FIELDS_KEY,
              org.apache.commons.lang3.StringUtils.EMPTY).split(StringUtils.COMMA_STR);
      for (String key: deltaKeys) {
        if (!key.isEmpty()) {
          dummySchema.addMember(key, new JsonSchema().addProperty("type", "timestamp"));
        }
      }
    }
    return dummySchema.getAltSchema(new HashMap<>(), false);
  }

  public boolean closeConnection() {
    if (connection != null) {
      connection.closeAll(org.apache.commons.lang3.StringUtils.EMPTY);
    }
    return true;
  }

  /**
   * ms.parameters have variables. For the initial execution of each work unit, we substitute those
   * variables with initial work unit variable values.
   *
   * @return the substituted parameters
   */
  protected JsonObject getInitialWorkUnitParameters() {
    JsonObject definedParameters = JsonParameter.getParametersAsJson(
        jobKeys.getSourceParameters().toString(),
        getInitialWorkUnitVariableValues(),
        this.state);
    return replaceVariablesInParameters(appendActivationParameter(definedParameters));
  }

  /**
   * Initial variable values are not specific to protocols, moving this method here
   * so that it can be shared among protocols.
   *
   * Initial work unit variable values include
   * - watermarks defined for each work unit
   * - initial pagination defined at the source level
   *
   * @return work unit specific initial parameters for the first request to source
   */
  private JsonObject getInitialWorkUnitVariableValues() {
    JsonObject variableValues = new JsonObject();

    variableValues.add(ParameterTypes.WATERMARK.toString(), getWorkUnitWaterMarks());
    for (Map.Entry<ParameterTypes, Long> entry: jobKeys.getPaginationInitValues().entrySet()) {
      variableValues.addProperty(entry.getKey().toString(), entry.getValue());
    }
    return variableValues;
  }

  /**
   * Replace variables in the parameters itself, so that ms.parameters can accept variables.
   * @param parameters the JsonObject with parameters
   * @return the replaced parameter object
   */
  JsonObject replaceVariablesInParameters(final JsonObject parameters) {
    JsonObject parametersCopy = JsonUtils.deepCopy(parameters).getAsJsonObject();
    JsonObject finalParameter = JsonUtils.deepCopy(parameters).getAsJsonObject();;
    try {
      Pair<String, JsonObject> replaced = VariableUtils.replaceWithTracking(
          parameters.toString(), parametersCopy, false);
      finalParameter = GSON.fromJson(replaced.getKey(), JsonObject.class);

      // for each parameter in the original parameter list, if the name of the parameter
      // name starts with "tmp" and the parameter was used once in this substitution operation,
      // then it shall be removed from the final list
      for (Map.Entry<String, JsonElement> entry: parameters.entrySet()) {
        if (entry.getKey().matches("tmp.*")
            && !replaced.getRight().has(entry.getKey())) {
          finalParameter.remove(entry.getKey());
        }
      }
    } catch (Exception e) {
      log.error("Encoding error is not expected, but : {}", e.getMessage());
    }
    log.debug("Final parameters: {}", finalParameter.toString());
    return finalParameter;
  }

  /**
   * Add activation parameters to work unit parameters
   * @param parameters the defined parameters
   * @return the set of parameters including activation parameters
   */
  private JsonObject appendActivationParameter(JsonObject parameters) {
    JsonObject activationParameters = extractorKeys.getActivationParameters();
    if (activationParameters.entrySet().size() > 0) {
      for (Map.Entry<String, JsonElement> entry: activationParameters.entrySet()) {
        String key = entry.getKey();
        parameters.add(key, activationParameters.get(key));
      }
    }
    return JsonUtils.deepCopy(parameters).getAsJsonObject();
  }

  protected JsonObject getCurrentWorkUnitParameters() {
    JsonObject definedParameters = JsonParameter.getParametersAsJson(
        jobKeys.getSourceParameters().toString(),
        getUpdatedWorkUnitVariableValues(getInitialWorkUnitVariableValues()),
        state);
    return replaceVariablesInParameters(appendActivationParameter(definedParameters));
  }

  /**
   * Update variable values based on work unit status
   *
   * Following variable values are updated:
   * 1. session key value if the work unit status has session key
   * 2. page start value if page start control is used
   * 3. page size value if page size control is used
   * 4. page number value if page number control is used
   *
   * Typically use case can use any of following pagination methods, some may use multiple:
   * 1. use page start (offset) and page size to control pagination
   * 2. use page number and page size to control pagination
   * 3. use page number to control pagination, while page size can be fixed
   * 4. use session key to control pagination, and the session key decides what to fetch next
   * 5. not use any variables, the session just keep going until following conditions are met:
   *    a. return an empty page
   *    b. return a specific status, such as "complete", in response
   *
   * @param initialVariableValues initial variable values
   * @return the updated variable values
   */
  private JsonObject getUpdatedWorkUnitVariableValues(JsonObject initialVariableValues) {
    JsonObject updatedVariableValues = JsonUtils.deepCopy(initialVariableValues).getAsJsonObject();
    WorkUnitStatus wuStatus = this.getWorkUnitStatus();

    // if session key is used, the extractor has to provide it int its work unit status
    // in order for this to work
    if (updatedVariableValues.has(ParameterTypes.SESSION.toString())) {
      updatedVariableValues.remove(ParameterTypes.SESSION.toString());
    }
    updatedVariableValues.addProperty(ParameterTypes.SESSION.toString(),
        wuStatus.getSessionKey());

    // if page start is used, the extractor has to provide it int its work unit status
    // in order for this to work
    if (updatedVariableValues.has(ParameterTypes.PAGESTART.toString())) {
      updatedVariableValues.remove(ParameterTypes.PAGESTART.toString());
    }
    updatedVariableValues.addProperty(ParameterTypes.PAGESTART.toString(),
        wuStatus.getPageStart());

    // page size doesn't change much often, if extractor doesn't provide
    // a page size, then assume it is the same as initial value
    if (updatedVariableValues.has(ParameterTypes.PAGESIZE.toString())
        && wuStatus.getPageSize() > 0) {
      updatedVariableValues.remove(ParameterTypes.PAGESIZE.toString());
    }
    if (wuStatus.getPageSize() > 0) {
      updatedVariableValues.addProperty(ParameterTypes.PAGESIZE.toString(),
          wuStatus.getPageSize());
    }

    // if page number is used, the extractor has to provide it in its work unit status
    // in order for this to work
    if (updatedVariableValues.has(ParameterTypes.PAGENO.toString())) {
      updatedVariableValues.remove(ParameterTypes.PAGENO.toString());
    }
    updatedVariableValues.addProperty(ParameterTypes.PAGENO.toString(),
        wuStatus.getPageNumber());

    return updatedVariableValues;
  }

  protected void logUsage(State state) {
    log.info("Checking essential (not always mandatory) parameters...");
    log.info("Values can be default values for the specific type if the property is not configured");
    for (MultistageProperties p: JobKeys.ESSENTIAL_PARAMETERS) {
      log.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getValidNonblankWithDefault(state));
    }
  }
}
