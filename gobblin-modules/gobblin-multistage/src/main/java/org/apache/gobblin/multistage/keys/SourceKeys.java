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

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.apache.gobblin.multistage.util.JsonUtils;
import org.apache.gobblin.multistage.util.ParameterTypes;
import org.apache.gobblin.multistage.util.WorkUnitPartitionTypes;
import org.joda.time.DateTime;


/**
 * This structure holds static Source parameters that are commonly used in all protocol Sources.
 * Each of these keys provide information how to populate corresponding values in protocol
 * sub-classes. Each protocol Source is responsible for proper usage of these keys.
 *
 * @author chrli
 */

@Slf4j
@Getter(AccessLevel.PUBLIC)
@Setter(AccessLevel.PUBLIC)
public class SourceKeys {
  final static public Gson GSON = new Gson();
  final static public List<MultistageProperties> ESSENTIAL_PARAMETERS = Lists.newArrayList(
      MultistageProperties.MSTAGE_ABSTINENT_PERIOD_DAYS,
      MultistageProperties.MSTAGE_DERIVED_FIELDS,
      MultistageProperties.MSTAGE_ENABLE_CLEANSING,
      MultistageProperties.MSTAGE_ENABLE_DYNAMIC_FULL_LOAD,
      MultistageProperties.MSTAGE_ENABLE_SCHEMA_BASED_FILTERING,
      MultistageProperties.MSTAGE_ENCODING,
      MultistageProperties.MSTAGE_ENCRYPTION_FIELDS,
      MultistageProperties.MSTAGE_GRACE_PERIOD_DAYS,
      MultistageProperties.MSTAGE_OUTPUT_SCHEMA,
      MultistageProperties.MSTAGE_PAGINATION,
      MultistageProperties.MSTAGE_PARAMETERS,
      MultistageProperties.MSTAGE_RETENTION,
      MultistageProperties.MSTAGE_SECONDARY_INPUT,
      MultistageProperties.MSTAGE_SESSION_KEY_FIELD,
      MultistageProperties.MSTAGE_SOURCE_DATA_CHARACTER_SET,
      MultistageProperties.MSTAGE_SOURCE_URI,
      MultistageProperties.MSTAGE_TOTAL_COUNT_FIELD,
      MultistageProperties.MSTAGE_WAIT_TIMEOUT_SECONDS,
      MultistageProperties.MSTAGE_WORK_UNIT_PACING_SECONDS,
      MultistageProperties.MSTAGE_WORK_UNIT_PARALLELISM_MAX,
      MultistageProperties.MSTAGE_WORK_UNIT_PARTIAL_PARTITION,
      MultistageProperties.MSTAGE_WATERMARK);

  private Map<String, Map<String, String>> derivedFields = new HashMap<>();
  private Map<String, String> defaultFieldTypes = new HashMap<>();

  // sourceSchema is the schema provided or retrieved from source
  private JsonSchema sourceSchema = new JsonSchema();

  // outputSchema is the schema to be supplied to converters
  private JsonSchema outputSchema = new JsonSchema();
  private JsonObject sessionKeyField = null;
  private String totalCountField = null;
  private JsonArray sourceParameters = new JsonArray();
  private Map<ParameterTypes, String> paginationFields = new HashMap<>();
  private Map<ParameterTypes, Long> paginationInitValues = new HashMap<>();
  private long sessionTimeout;
  private long callInterval;
  private JsonArray encryptionField = new JsonArray();
  private boolean enableCleansing;
  String dataField = null;
  private JsonArray watermarkDefinition;
  private long retryDelayInSec;
  private long retryCount;
  private Boolean isPartialPartition;
  private JsonArray secondaryInputs = new JsonArray();
  private WorkUnitPartitionTypes workUnitPartitionType;

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

}
