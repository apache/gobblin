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
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.filter.JsonSchemaBasedFilter;
import org.apache.gobblin.multistage.filter.MultistageSchemaBasedFilter;
import org.apache.gobblin.multistage.keys.ExtractorKeys;
import org.apache.gobblin.multistage.preprocessor.InputStreamProcessor;
import org.apache.gobblin.multistage.source.MultistageSource;
import org.apache.gobblin.multistage.util.DateTimeUtils;
import org.apache.gobblin.multistage.util.EncryptionUtils;
import org.apache.gobblin.multistage.util.JsonIntermediateSchema;
import org.apache.gobblin.multistage.util.JsonUtils;
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
  @Getter(AccessLevel.PUBLIC)
  @Setter(AccessLevel.PACKAGE)
  protected WorkUnitStatus workUnitStatus = WorkUnitStatus.builder()
      .messages(new HashMap<>())
      .sessionKey("")
      .pageNumber(0)
      .pageSize(0)
      .pageStart(0)
      .build();

  @Getter(AccessLevel.PUBLIC)
  protected WorkUnitState state = null;
  protected MultistageSource<S, D> source = null;

  @Getter
  ExtractorKeys extractorKeys = new ExtractorKeys();

  protected MultistageSchemaBasedFilter<?> rowFilter = null;

  public MultistageExtractor(WorkUnitState state, MultistageSource source) {
    this.state = state;
    this.source = source;
    extractorKeys.setActivationParameters(MultistageProperties.MSTAGE_ACTIVATION_PROPERTY.getValidNonblankWithDefault(state));
    extractorKeys.setDelayStartTime(MultistageProperties.MSTAGE_WORKUNIT_STARTTIME_KEY.getProp(state));
    extractorKeys.setSignature(state.getProp(ConfigurationKeys.DATASET_URN_KEY));
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
    source.closeStream(this);
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

    WorkUnitStatus tmpStatus;
    if (isFirst(starting)) {
      tmpStatus = source.getFirst(this);
    } else {
      tmpStatus = source.getNext(this);
    }
    if (tmpStatus == null) {
      this.failWorkUnit("Received a NULL WorkUnitStatus, fail the work unit");
      return false;
    }
    workUnitStatus.setBuffer(tmpStatus.getBuffer());
    workUnitStatus.setMessages(tmpStatus.getMessages());
    workUnitStatus.setSessionKey(getSessionKey(tmpStatus));
    extractorKeys.setSessionKeyValue(workUnitStatus.getSessionKey());

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
    JsonArray schemaArray = new JsonArray();
    if (this.source.getSourceKeys().getOutputSchema().getSchema().entrySet().size() == 0) {
      if (!processInputStream(0)) {
        return schemaArray;
      }
    }
    if (this.source.getSourceKeys().hasOutputSchema()) {
      // take pre-defined fixed schema
      JsonObject schemaObject = JsonUtils.deepCopy(source.getSourceKeys().getOutputSchema().getSchema()).getAsJsonObject();
      schemaArray = schemaObject.get("items").getAsJsonArray();
      setRowFilter(schemaArray);
    } else {
      if (this.source.getSourceKeys().hasSourceSchema()) {
        schemaArray = this.source.getSourceKeys().getSourceSchema().getAltSchema(source.getSourceKeys().getDefaultFieldTypes(),
            source.getSourceKeys().isEnableCleansing());
        schemaArray = JsonUtils.deepCopy(schemaArray).getAsJsonArray();
        log.info("Source provided schema: {}", schemaArray.toString());
      } else if (extractorKeys.getInferredSchema() != null
          && !extractorKeys.getInferredSchema().getSchema().get("type").getAsString().equalsIgnoreCase("null")) {
        // schema inference is only possible when there is some data
        // otherwise the inferred schema type will be null
        schemaArray = extractorKeys.getInferredSchema().getAltSchema(source.getSourceKeys().getDefaultFieldTypes(),
            source.getSourceKeys().isEnableCleansing());
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
  List<InputStreamProcessor> getPreprocessors(State state) {
    ImmutableList.Builder<InputStreamProcessor> builder = ImmutableList.builder();
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
            // Decrypt if any credential is encrypted
            for (Map.Entry<String, JsonElement> entry: preprocessorParams.entrySet()) {
              String key = entry.getKey();
              String value = preprocessorParams.get(key).getAsString();
              String decryptedValue = EncryptionUtils.decryptGobblin(value, state);
              preprocessorParams.addProperty(key, decryptedValue);
            }
          }
          Class clazz = Class.forName(p);
          InputStreamProcessor instance =
              (InputStreamProcessor) clazz.getConstructor(JsonObject.class).newInstance(preprocessorParams);
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
    for (Map.Entry<String, Map<String, String>> entry: source.getSourceKeys().getDerivedFields().entrySet()) {
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
        default:
          /**
           * by default take the source type
           * TODO: if source is integer or number
           * TODO: if source is nested
           */
          dataType.addProperty("type", source.getSourceKeys().getOutputSchema()
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
        data = IOUtils.toString(input,
            Charset.forName(MultistageProperties.MSTAGE_SOURCE_DATA_CHARACTER_SET.getValidNonblankWithDefault(state)));
      } catch (Exception e) {
        log.debug(e.toString());
      }
      source.closeStream(this);
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
        log.info("Content is not {}}", expectedContentType);
        log.info(extractText(wuStatus.getBuffer()));
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
        && source.getSourceKeys().getSessionKeyField() != null
        && source.getSourceKeys().getSessionKeyField().has("name")) {
      JsonObject headers = GSON.fromJson(wuStatus.getMessages().get("headers"), JsonObject.class);
      if (headers.has(this.source.getSourceKeys().getSessionKeyField().get("name").getAsString())) {
        return headers.get(this.source.getSourceKeys().getSessionKeyField().get("name").getAsString()).getAsString();
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
    if (!source.getSourceKeys().isSessionStateEnabled() || isSessionStateMatch()) {
      return true;
    }

    // if stop condition is present but the condition has not been met, we
    // will check if the session should time out
    if (DateTime.now().getMillis() > extractorKeys.getStartTime()
        + source.getSourceKeys().getSessionTimeout()) {
      log.warn("Session time out after {} seconds", source.getSourceKeys().getSessionTimeout() / 1000);
      throw new RuntimeException("Session timed out before ending condition is met");
    }

    // return false to indicate wait should continue
    return false;
  }

  /**
   * Check if session end condition is met if session state is enabled
   *
   * @return true if session state is enabled and session end condition is met
   * otherwise return false
   */
  protected boolean isSessionStateMatch() {
    return source.getSourceKeys().isSessionStateEnabled()
        && extractorKeys.getSessionKeyValue().matches(source.getSourceKeys().getSessionStateCondition());
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
      if (source.getSourceKeys().isSessionStateEnabled()) {
        return !waitingBySessionKeyWithTimeout();
      } else {
        return source.getSourceKeys().isPaginationEnabled();
      }
    } catch (Exception e) {
      failWorkUnit("Timeout waiting for next page");
      return false;
    }
  }
}
