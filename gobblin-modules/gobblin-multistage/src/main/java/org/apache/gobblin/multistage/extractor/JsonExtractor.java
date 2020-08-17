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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.filter.JsonSchemaBasedFilter;
import org.apache.gobblin.multistage.keys.JsonExtractorKeys;
import org.apache.gobblin.multistage.source.MultistageSource;
import org.apache.gobblin.multistage.util.EncryptionUtils;
import org.apache.gobblin.multistage.util.JsonSchemaGenerator;
import org.apache.gobblin.multistage.util.JsonUtils;
import org.apache.gobblin.multistage.util.ParameterTypes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;


/**
 * JsonExtractor reads Json formatted responses from HTTP sources, like Rest API source.
 *
 * This extractor will output schema in JsonArray format, such as
 * [{"columnName": "id", "type": "string"},{"columnName": "duration", "type": "integer"}]
 *
 * The rows will be pass output to converters in the form of JsonObjects, which represent
 * rows.
 *
 * This extractor can used to feed into a JsonIntermediateToAvroConverter and sink data into Avro.
 *
 * @author chrli
 */
@Slf4j
public class JsonExtractor extends MultistageExtractor<JsonArray, JsonObject> {
  final private static String DEFAULT_TIMEZONE = "America/Los_Angeles";

  private final static String JSON_MEMBER_SEPARATOR = ".";
  private final static Long SCHEMA_INFER_MAX_SAMPLE_SIZE = 100L;
  @Setter
  private String timezone = "";

  JsonExtractorKeys jsonExtractorKeys = new JsonExtractorKeys();

  public JsonExtractor(WorkUnitState state, MultistageSource source) {
    super(state, source);
  }

  /**
   * getSchema will be called by Gobblin to retrieve the schema of the output of this extract.
   * The returned schema will be used in subsequent converters. The alternative schema here suites
   * JsonIntermediateToAvroConverter. Future development can support other converter by making
   * the schema conversion configurable.
   *
   * typically a json schema would be like following with nesting structures
   * <p>{</p>
   * <p>  "type": "array",</p>
   * <p>  "items": {</p>
   * <p>    "id": {</p>
   * <p>      "type": "string"</p>
   * <p>    },</p>
   * <p>    "emailAddress": {</p>
   * <p>      "type": "string"</p>
   * <p>    },</p>
   * <p>    "emailAliases": {</p>
   * <p>      "type": "array",</p>
   * <p>      "items": {</p>
   * <p>        "type": ["string", "null"]</p>
   * <p>      }</p>
   * <p>    },
   * <p>    "personalMeetingUrls": {</p>
   * <p>      "type": "string",</p>
   * <p>      "items": {</p>
   * <p>        "type": "null"</p>
   * <p>      }</p>
   * <p>    },</p>
   * <p>    "settings": {</p>
   * <p>      "type": "object",</p>
   * <p>      "properties": {</p>
   * <p>        "webConferencesRecorded": {</p>
   * <p>          "type": "boolean"</p>
   * <p>        },</p>
   * <p>        "preventWebConferenceRecording": {</p>
   * <p>          "type": "boolean"</p>
   * <p>        },</p>
   * <p>        "preventEmailImport": {</p>
   * <p>          "type": "boolean"</p>
   * <p>        }</p>
   * <p>      }</p>
   * <p>    }</p>
   * <p>  }</p>
   * <p>}</p>
   *
   * <p>However an alternative or intermediate way of writing the schema</p>
   * <p>{"emailAddress": {"type": "string"}}</p>
   * <p>is:</p>
   * <p>{"columnName": "emailAddress", "dataType": {"type": "string'}}</p>
   *
   * @return the schema of the extracted record set in a JasonArray String
   */
  @Override
  public JsonArray getSchema() {
    log.debug("Retrieving schema definition");
    JsonArray schemaArray = super.getOrInferSchema();
    if (source.getSourceKeys().getDerivedFields().size() > 0) {
      schemaArray.addAll(addDerivedFieldsToAltSchema());
    }
    return schemaArray;
  }

  @Nullable
  @Override
  public JsonObject readRecord(JsonObject reuse) {
    if (jsonExtractorKeys.getJsonElementIterator() == null
        && !processInputStream(0)) {
      return null;
    }

    if (jsonExtractorKeys.getJsonElementIterator().hasNext()) {
      jsonExtractorKeys.setProcessedCount(1 + jsonExtractorKeys.getProcessedCount());
      JsonObject row = jsonExtractorKeys.getJsonElementIterator().next().getAsJsonObject();
      if (source.getSourceKeys().getEncryptionField() != null && !source.getSourceKeys()
          .getEncryptionField().equals("")) {
        row = encryptJsonFields("", row);
      }
      if (source.getSourceKeys().isEnableCleansing()) {
        row = limitedCleanse(row).getAsJsonObject();
      }
      JsonSchemaBasedFilter jsonSchemaBasedFilter = (JsonSchemaBasedFilter) rowFilter;
      return addDerivedFields(jsonSchemaBasedFilter != null ? jsonSchemaBasedFilter.filter(row) : row);
    } else {
      source.closeStream(this);
      if (hasNextPage() && processInputStream(jsonExtractorKeys.getProcessedCount())) {
        return readRecord(reuse);
      }
    }
    return null;
  }

  /**
   * This is the main method in this extractor, it extracts data from source and perform essential checks.
   *
   * @param starting [0, +INF), points to the last count of record processed, 0 means it's the first of a series of requests
   * @return true if Successful
   */
  @Override
  protected boolean processInputStream(long starting) {
    if (!super.processInputStream(starting)) {
      return false;
    }

    // if Content-Type is provided, but not application/json, the response can have
    // useful error information
    if (!checkContentType(workUnitStatus, "application/json")) {
        return false;
    }

    // get source schema if available and then set both sourceSchema and outputSchema
    if (!source.getSourceKeys().hasSourceSchema()
        && !source.getSourceKeys().hasOutputSchema()
        && workUnitStatus.getMessages().containsKey("schema")) {
      source.getSourceKeys().setSourceSchema(workUnitStatus.getSchema());
    }

    JsonElement data;
    try {
      data = extractJson(workUnitStatus.getBuffer());
      // return false to stop the job under these situations
      if (data == null || data.isJsonNull() || data.isJsonPrimitive()) {
        return false;
      }
    } catch (Exception e) {
      log.error("Source Error: {}", e.getMessage());
      state.setWorkingState(WorkUnitState.WorkingState.FAILED);
      return false;
    }

    log.debug("Checking parsed Json object");

    JsonArray coreData = new JsonArray();
    JsonElement payload;
    if (StringUtils.isNotBlank(source.getSourceKeys().getDataField())) {
      payload = getElementByJsonPath(data.getAsJsonObject(), source.getSourceKeys().getDataField());
      if (payload.isJsonNull()) {
        log.info("Terminate the ingestion because no actual payload in the response");
        return false;
      }
    } else {
      payload = data;
    }

    if (payload.isJsonArray()) {
      coreData = payload.getAsJsonArray();
    } else {
      log.info("Payload is not a Json Array, therefore add the whole payload a one single entry");
      coreData.add(payload);
    }

    // get basic profile of the returned data
    jsonExtractorKeys.setTotalCount(getTotalCountValue(data));
    jsonExtractorKeys.setPushDowns(retrievePushDowns(data, source.getSourceKeys().getDerivedFields()));
    extractorKeys.setSessionKeyValue(retrieveSessionKeyValue(data));
    jsonExtractorKeys.setCurrentPageNumber(jsonExtractorKeys.getCurrentPageNumber() + 1);

    // get profile of the payload
    if (source.getSourceKeys().getOutputSchema().getSchema().entrySet().size() == 0
        && starting == 0
        && coreData.size() > 0) {
      JsonArray sample  = new JsonArray();
      for (int i = 0; i < Long.min(coreData.size(), SCHEMA_INFER_MAX_SAMPLE_SIZE); i++) {
        sample.add(JsonUtils.deepCopy(coreData.get(i)));
      }
      extractorKeys.setInferredSchema(new JsonSchemaGenerator(sample).getSchema());
    }

    // update work unit status for next Source call
    workUnitStatus.setSetCount(coreData.size());
    workUnitStatus.setTotalCount(jsonExtractorKeys.getTotalCount());
    workUnitStatus.setSessionKey(extractorKeys.getSessionKeyValue());
    updatePaginationStatus(data);

    jsonExtractorKeys.logDebugAll(state.getWorkunit());
    workUnitStatus.logDebugAll();
    extractorKeys.logDebugAll(state.getWorkunit());

    jsonExtractorKeys.setJsonElementIterator(coreData.getAsJsonArray().iterator());
    return coreData.getAsJsonArray().size() > 0;
  }


  /**
   * calculate and add derived fields
   *
   * @param row original record
   * @return modified record
   */
  private JsonObject addDerivedFields(JsonObject row) {
    for (Map.Entry<String, Map<String, String>> entry: source.getSourceKeys().getDerivedFields().entrySet()) {
      String name = entry.getKey();
      String source = entry.getValue().get("source");
      String strValue = "";
      long longValue = 0L;
      DateTimeZone timeZone = DateTimeZone.forID(timezone.isEmpty() ? DEFAULT_TIMEZONE : timezone);
      if (jsonExtractorKeys.getPushDowns().entrySet().size() > 0 && jsonExtractorKeys.getPushDowns().has(name)) {
        strValue = jsonExtractorKeys.getPushDowns().get(name).getAsString();
      } else if (source.equalsIgnoreCase("currentdate")) {
        longValue = DateTime.now().getMillis();
      } else if (source.matches("P\\d+D")) {
        Period period = Period.parse(source);
        longValue  = DateTime.now().withZone(timeZone).minus(period).dayOfMonth().roundFloorCopy().getMillis();
      } else {
        JsonElement ele = getElementByJsonPath(row, source);
        if (ele != null && !ele.isJsonNull()) {
          strValue = ele.getAsString();
        }
      }
      switch (entry.getValue().get("type")) {
        case "epoc":
          if (source.equalsIgnoreCase("currentdate") || source.matches("P\\d+D")) {
            row.addProperty(name, longValue);
          } else {
            String format = entry.getValue().get("format");
            String epoc = deriveEpoc(format, strValue);
            if (epoc.length() > 0) {
              row.addProperty(name, epoc);
            }
          }
          break;
        case "string":
          row.addProperty(name, strValue);
          break;
        case "regexp":
          Pattern pattern = Pattern.compile(entry.getValue().getOrDefault("format", "(.*)"));
          Matcher matcher = pattern.matcher(strValue);
          if (matcher.find()) {
            String sub = matcher.group(1);
            row.addProperty(name, sub);
          } else {
            log.error("Regular expression finds no match!");
            row.addProperty(name, "no match");
          }
          break;
        default:
          // do nothing
          break;
      }
    }
    return row;
  }

  /**
   * Update pagination parameters
   * @param data response from the source, can be JsonArray or JsonObject
   */
  private Map<ParameterTypes, Long> getNextPaginationValues(JsonElement data) {
    Map<ParameterTypes, String> paginationKeys = source.getSourceKeys().getPaginationFields();
    Map<ParameterTypes, Long> paginationValues = new HashMap<>();

    if (data.isJsonObject()) {
      JsonElement pageStartElement = null;
      JsonElement pageSizeElement = null;
      JsonElement pageNumberElement = null;

      if (paginationKeys.containsKey(ParameterTypes.PAGESTART)) {
        pageStartElement = getElementByJsonPath(data.getAsJsonObject(), paginationKeys.get(ParameterTypes.PAGESTART));
      } else {
        // update page start directly to rows processed as Next page start
        paginationValues.put(ParameterTypes.PAGESTART, jsonExtractorKeys.getProcessedCount() + workUnitStatus.getSetCount());
      }

      if (paginationKeys.containsKey(ParameterTypes.PAGESIZE)) {
        pageSizeElement = getElementByJsonPath(data.getAsJsonObject(), paginationKeys.get(ParameterTypes.PAGESIZE));
      } else {
        paginationValues.put(ParameterTypes.PAGESIZE,
            source.getSourceKeys().getPaginationInitValues().getOrDefault(ParameterTypes.PAGESIZE, 0L));
      }

      if (paginationKeys.containsKey(ParameterTypes.PAGENO)) {
        pageNumberElement = getElementByJsonPath(data.getAsJsonObject(), paginationKeys.get(ParameterTypes.PAGENO));
      } else {
        paginationValues.put(ParameterTypes.PAGENO, jsonExtractorKeys.getCurrentPageNumber());
      }

      if (pageStartElement != null && pageSizeElement != null
          && !pageStartElement.isJsonNull() && !pageSizeElement.isJsonNull()) {
        paginationValues.put(ParameterTypes.PAGESTART, pageStartElement.getAsLong() + pageSizeElement.getAsLong());
        paginationValues.put(ParameterTypes.PAGESIZE, pageSizeElement.getAsLong());
      }
      if (pageNumberElement != null && !pageNumberElement.isJsonNull()) {
        paginationValues.put(ParameterTypes.PAGENO, pageNumberElement.getAsLong() + 1);
      }
    } else if (data.isJsonArray()) {
      paginationValues.put(ParameterTypes.PAGESTART, jsonExtractorKeys.getProcessedCount() + data.getAsJsonArray().size());
      paginationValues.put(ParameterTypes.PAGESIZE,
          source.getSourceKeys().getPaginationInitValues().getOrDefault(ParameterTypes.PAGESIZE, 0L));
      paginationValues.put(ParameterTypes.PAGENO, jsonExtractorKeys.getCurrentPageNumber());
    }
    return paginationValues;
  }

  /**
   * retrieveSessionKey() parses the response JSON and extract the session key value
   *
   * @param input the Json payload
   * @return the session key if the property is available
   */
  private String retrieveSessionKeyValue(JsonElement input) {
    if (source.getSourceKeys().getSessionKeyField().entrySet().size() == 0
        || !input.isJsonObject()) {
      return StringUtils.EMPTY;
    }

    JsonObject data = input.getAsJsonObject();

    Iterator<String> members = Splitter.on(JSON_MEMBER_SEPARATOR)
        .omitEmptyStrings()
        .trimResults()
        .split(source.getSourceKeys().getSessionKeyField().get("name").getAsString())
        .iterator();

    JsonElement e = data;
    while (members.hasNext()) {
      String member = members.next();
      if (e.getAsJsonObject().has(member)) {
        e = e.getAsJsonObject().get(member);
        if (!members.hasNext()) {
          return e.getAsString();
        }
      }
    }
    return extractorKeys.getSessionKeyValue() == null ? StringUtils.EMPTY : extractorKeys.getSessionKeyValue();
  }

  /**
   *
   * Retrieves the total row count member if it is expected. Without a total row count,
   * this request is considered completed after the first call or when the session
   * completion criteria is met, see {@link JsonExtractor#readRecord(JsonObject)} ()}
   *
   * @param data HTTP response JSON
   * @return the expected total record count if the property is available
   */
  private Long getTotalCountValue(JsonElement data) {
    if (StringUtils.isBlank(source.getSourceKeys().getTotalCountField())) {
      if (data.isJsonObject()) {
        if (StringUtils.isNotBlank(source.getSourceKeys().getDataField())) {
          JsonElement payload = getElementByJsonPath(data.getAsJsonObject(), source.getSourceKeys().getDataField());
          if (payload.isJsonNull()) {
            log.info("Expected payload at JsonPath={} doesn't exist", source.getSourceKeys().getDataField());
            return jsonExtractorKeys.getTotalCount();
          } else if (payload.isJsonArray()) {
            return jsonExtractorKeys.getTotalCount() + payload.getAsJsonArray().size();
          } else {
            throw new RuntimeException("Payload is not a JsonArray, only array payload is supported");
          }
        } else {
          // no total count field and no data field
          return jsonExtractorKeys.getTotalCount() + 1;
        }
      } else if (data.isJsonArray()) {
        return jsonExtractorKeys.getTotalCount() + data.getAsJsonArray().size();
      } else {
        return jsonExtractorKeys.getTotalCount();
      }
    }

    Iterator<String> members = Splitter.on(JSON_MEMBER_SEPARATOR)
        .omitEmptyStrings()
        .trimResults()
        .split(source.getSourceKeys().getTotalCountField())
        .iterator();

    JsonElement e = data;
    while (members.hasNext()) {
      String member = members.next();
      if (e.getAsJsonObject().has(member)) {
        e = e.getAsJsonObject().get(member);
        if (!members.hasNext()) {
          return e.getAsLong();
        }
      }
    }
    return jsonExtractorKeys.getTotalCount();
  }

  private void updatePaginationStatus(JsonElement data) {
    // update work unit status, and get ready for next calls, these steps are possible only
    // when data is a JsonObject
    Map<ParameterTypes, Long> pagination = getNextPaginationValues(data);
    workUnitStatus.setPageStart(pagination.getOrDefault(ParameterTypes.PAGESTART, 0L));
    workUnitStatus.setPageSize(pagination.getOrDefault(ParameterTypes.PAGESIZE, 0L));
    workUnitStatus.setPageNumber(pagination.getOrDefault(ParameterTypes.PAGENO, 0L));
  }

  /**
   * Get a JosnElement from a JsonObject based on the given JsonPath
   *
   * @param row the record contains the data element
   * @param jsonPath the JsonPath (string) how to get the data element
   * @return the data element at the JsonPath position, or JsonNull if error
   */
  JsonElement getElementByJsonPath(JsonObject row, String jsonPath) {
    JsonElement child = row;
    List<String> labels = Lists.newArrayList(jsonPath.split("\\."));

    if (labels.size() == 0 || row == null || row.isJsonNull()) {
      log.warn("Json path is not supplied or the row object is null.");
      return JsonNull.INSTANCE;
    }

    for (String label: labels) {
      if (child.isJsonObject()) {
        if (!child.getAsJsonObject().has(label)) {
          log.error("Object {} doesn't have element {} specified by the Json path", child.toString(), label);
          return JsonNull.INSTANCE;
        }
        child = child.getAsJsonObject().get(label);
      } else if (child.isJsonArray()) {
        int index = -1;
        try {
          index = Integer.parseInt(label);
        } catch (Exception e) {
          log.error("Error converting Json path {} to index.", label);
        } finally {
          if (index >= 0 && index < child.getAsJsonArray().size()) {
            child = child.getAsJsonArray().get(index);
          } else {
            log.error("Index value {} is out of boundary of JsonArray {}", label, child.toString());
            return JsonNull.INSTANCE;
          }
        }
      } else {
        log.error("JsonPath {} hit on a primitive {}", label, child.toString());
        return JsonNull.INSTANCE;
      }
    }
    return child;
  }

  /**
   * Perform limited cleansing so that data can be processed by converters
   *
   * TODO: make a dummy value for Null values
   * @param input the input data to be cleansed
   * @return  the cleansed data
   */
  private JsonElement limitedCleanse(JsonElement input) {
    JsonElement output = null;

    if (input.isJsonObject()) {
      output = new JsonObject();
      for (Map.Entry<String, JsonElement> entry: input.getAsJsonObject().entrySet()) {
        ((JsonObject) output).add(entry.getKey().replaceAll("(\\s|\\$)", "_"),
            limitedCleanse(entry.getValue()));
      }
    } else if (input.isJsonArray()) {
      output = new JsonArray();
      for (JsonElement ele: input.getAsJsonArray()) {
        ((JsonArray) output).add(limitedCleanse(ele));
      }
    } else {
      output = input;
    }
    return output;
  }

  /**
   * Function which iterates through the fields in a row and encrypts the particular field defined in the
   * ms.encrypted.field property.
   * @param input parentKey, JsonElement
   *              parentKey -> holds the key name in case of nested structures
   *                e.g. settings.webprocessor (parentkey = settings)
   * @return row with the field encrypted through the Gobblin Utility
   */
  private JsonObject encryptJsonFields(String parentKey, JsonElement input) {
    JsonObject output = new JsonObject();
    JsonArray encryptionFields = source.getSourceKeys().getEncryptionField();
    for (Map.Entry<String, JsonElement> entry: input.getAsJsonObject().entrySet()) {
      JsonElement value = entry.getValue();
      String key = entry.getKey();
      // absolutekey holds the complete path of the key for matching with the encryptedfield
      String absolutekey = (parentKey.length() == 0) ? key : (parentKey + "." + key);
      // this function assumes that the final value to be encrypted will always be a JsonPrimitive object and in case of
      // of JsonObject it will iterate recursively.
      if (value.isJsonPrimitive() && encryptionFields.contains(new JsonPrimitive(absolutekey))) {
        String valStr = EncryptionUtils.encryptGobblin(value.isJsonNull() ? "" : value.getAsString(), state);
        output.add(key, new JsonPrimitive(valStr));
      } else if (value.isJsonObject()) {
        output.add(key, encryptJsonFields(absolutekey, value));
      } else {
        output.add(key, value);
      }
    }
    return output;
  }

  /**
   * Save values that are not in the "data" payload, but will be used in de-normalization.
   * Values are saved by their derived field name.
   *
   * TODO: push down non-string values (low priority)
   *
   * @param response the Json response from source
   * @param derivedFields list of derived fields
   * @return list of values to be used in derived fields
   */
  private JsonObject retrievePushDowns(JsonElement response, Map<String, Map<String, String>> derivedFields) {
    if (response == null || response.isJsonNull() || response.isJsonArray()) {
      return new JsonObject();
    }
    JsonObject data = response.getAsJsonObject();
    JsonObject pushDowns = new JsonObject();
    for (Map.Entry<String, Map<String, String>> entry: derivedFields.entrySet()) {
      String source = entry.getValue().get("source");
      if (data.has(source)) {
        pushDowns.addProperty(entry.getKey(), data.get(source).getAsString());
        log.info("Identified push down value: {}", pushDowns);
      }
    }
    return pushDowns;
  }

  /**
   * Convert the input stream buffer to a Json object
   * @param input the InputStream buffer
   * @return a Json object of type JsonElement
   */
  private JsonElement extractJson(InputStream input) throws UnsupportedCharsetException {
    log.debug("Parsing response InputStream as Json");
    JsonElement data = null;
    if (input != null) {
      data = new JsonParser().parse(new InputStreamReader(input,
          Charset.forName(MultistageProperties.MSTAGE_SOURCE_DATA_CHARACTER_SET.getValidNonblankWithDefault(state))));
      source.closeStream(this);
    }
    return data;
  }

  /**
   *  Terminate the extraction if:
   *  1. total count has been initialized
   *  2. all expected rows are fetched
   *
   * @param starting the current position, or starting position of next request
   * @return true if all rows retrieve
   */
  @Override
  protected boolean isWorkUnitCompleted(long starting) {
    return starting != 0
        && StringUtils.isNotBlank(source.getSourceKeys().getTotalCountField())
        && starting >= jsonExtractorKeys.getTotalCount();
  }

  /**
   * If the iterator is null, then it must be the first request
   * @param starting the starting position of the request
   * @return true if the iterator is null, otherwise false
   */
  @Override
  protected boolean isFirst(long starting) {
    return jsonExtractorKeys.getJsonElementIterator() == null;
  }

  /**
   * Add condition to allow total row count can be used to control pagination.
   *
   * @return true if a new page request is needed
   */
  @Override
  protected boolean hasNextPage() {
    return super.hasNextPage()
        || jsonExtractorKeys.getProcessedCount() < jsonExtractorKeys.getTotalCount();
  }
}
