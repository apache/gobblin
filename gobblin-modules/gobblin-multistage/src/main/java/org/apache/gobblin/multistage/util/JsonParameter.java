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

package org.apache.gobblin.multistage.util;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 *
 * A sample parameter string with variables (fromDateTime, toDateTime
 * cursor) in it. The variables are to be replaced with actual values.
 *
 * [{
 *   "name": "filter",
 *   "type": "object",
 *   "value": [{
 *     "name": "fromDateTime",
 *     "type": "watermark",
 *     "watermark": "system",
 *     "value": "low",
 *     "format": "datetime",
 *     "pattern": "yyyy-MM-dd'T'HH:mm:ss'Z'"
 *   }, {
 *     "name": "toDateTime",
 *     "type": "watermark",
 *     "watermark": "system",
 *     "value": "high",
 *     "format": "datetime",
 *     "pattern": "yyyy-MM-dd'T'HH:mm:ss'Z'"
 *   }]
 * }, {
 *   "name": "cursor",
 *   "type": "session"
 * }, {
 *   "name": "contentSelector",
 *   "type": "object",
 *   "value": [{
 *     "name": "context",
 *     "type": "list",
 *     "value": "Extended"
 *   },{
 *     "name": "exposedFields",
 *     "type": "object",
 *     "value": [{
 *       "name": "collaboration",
 *       "type": "object",
 *       "value": {
 *         "name": "publicComments",
 *         "type": "list",
 *         "value": "false"
 *       }
 *     }, {
 *       "name": "content",
 *       "type": "object",
 *       "value": [{
 *         "name": "structure",
 *         "type": "list",
 *         "value": "false"
 *       }, {
 *         "name": "topics",
 *         "type": "list",
 *         "value": "false"
 *       }, {
 *         "name": "trackers",
 *         "type": "list",
 *         "value": "false"
 *       }]
 *     }, {
 *       "name": "interaction",
 *       "type": "object",
 *       "value": [{
 *         "name": "personInteractionStats",
 *         "type": "list",
 *         "value": "false"
 *       }, {
 *         "name": "speakers",
 *         "type": "list",
 *         "value": "false"
 *       }, {
 *         "name": "video",
 *         "type": "list",
 *         "value": "false"
 *       }]
 *     }, {
 *       "name": "parties",
 *       "type": "list",
 *       "value": "false"
 *     }]
 *   }]
 * }]
 *
 * @author chrli
 */
@Slf4j
public class JsonParameter {

  private JsonObject paramJson;
  private State state;
  final private static String DEFAULT_TIMEZONE = "America/Los_Angeles";

  /**
   * See sample-parameter.json regarding to how to compose parameters
   *
   * @param inputJsonArrayString a JsonArray formatted as String
   * @param values a list of input values for variable replacement
   * @param state source state
   */
  public JsonParameter(String inputJsonArrayString, JsonObject values, State state) {
    this.state = state;
    paramJson = new JsonObject();
    JsonArray jsonArray = new Gson().fromJson(inputJsonArrayString, JsonArray.class);

    if (jsonArray != null && !jsonArray.isJsonNull()) {
      for (JsonElement element : jsonArray) {
        JsonObject param = parseParameter(element.getAsJsonObject(), values);
        if (param != null) {
          for (Map.Entry<String, JsonElement> entry : param.entrySet()) {
            paramJson.add(entry.getKey(), entry.getValue());
          }
        }
      }
    }
  }

  /**
   * @return reformatted parameters as a Json Object
   */
  public JsonObject getParametersAsJson() {
    return paramJson;
  }

  /**
   * @param inputString a parameter string in Json format with the needed data elements
   * @param values the replacement values
   * @param state source state
   * @return reformatted parameters as a Json String
   */
  public static JsonObject getParametersAsJson(String inputString, JsonObject values, State state) {
    return new JsonParameter(inputString, values, state).getParametersAsJson();
  }

  /**
   * @return reformatted parameters as a Json String
   */
  public String getParametersAsJsonString() {
    return paramJson.toString();
  }

  /**
   * @param inputString a parameter string in Json format with the needed data elements
   * @param values the replacement values
   * @param state source state
   * @return reformatted parameters as a Json String
   */
  public static String getParametersAsJsonString(String inputString, JsonObject values, State state) {
    return new JsonParameter(inputString, values, state).getParametersAsJsonString();
  }

  /**
   *
   * @param inputString a parameter string in Json format with the needed data elements
   * @param values the replacement values
   * @param state source state
   * @return reformatted parameters in a Map object
   */
  public static Map<String, String> getParametersAsMap(String inputString, JsonObject values, State state) {
    return new JsonParameter(inputString, values, state).getParametersAsMap();
  }

  /**
   * @return reformatted parameters in a Map object
   */
  public Map<String, String> getParametersAsMap() {
    Map<String, String> params = new HashMap<>();
    for (Map.Entry<String, JsonElement> entry: paramJson.entrySet()) {
      params.put(entry.getKey(), entry.getValue().getAsString());
    }
    return params;
  }

  /**
   * core function parsing the parameter string and replace with input values
   * @param paramObject a JsonObject of parameter definitions
   * @param values replacement values
   * @return a JsonObject of parameters where watermarks are replaced with actual given values
   */
  private JsonObject parseParameter(JsonObject paramObject, JsonObject values) {
    log.debug("Parsing parameter string: {} with input: {}", paramObject.toString(), values.toString());

    ParameterTypes type = ParameterTypes.valueOf(
        paramObject.has("type")
            ? paramObject.get("type").getAsString().toUpperCase() : "LIST");

    JsonObject parsedObject = new JsonObject();
    String name = paramObject.get("name").getAsString();
    switch (type) {
      // an OBJECT would assume the elements could have substitution variables
      case OBJECT:
        if (paramObject.get("value").isJsonObject()
        && !paramObject.get("value").getAsJsonObject()
            .get("type").getAsString().equalsIgnoreCase(ParameterTypes.OBJECT.toString())) {
          parsedObject.add(name, parseParameter(paramObject.get("value").getAsJsonObject(), values));
        } else if (paramObject.get("value").isJsonArray()) {
          JsonObject members = new JsonObject();
          for (JsonElement member: paramObject.get("value").getAsJsonArray()) {
            JsonObject converted = parseParameter(member.getAsJsonObject(), values);
            if (converted != null) {
              for (Map.Entry<String, JsonElement> ele : converted.entrySet()) {
                members.add(ele.getKey(), ele.getValue());
              }
            }
          }
          parsedObject.add(name, members);
        }
        break;
      case LIST:
        // allow encryption on LIST type parameters
        parsedObject.addProperty(name, EncryptionUtils.decryptGobblin(
            parseListParameter(paramObject.get("value"), state),
            state));
        break;

      case JSONARRAY:
        parsedObject.add(name, paramObject.get("value").getAsJsonArray());
        break;

      // a JSONOBJECT, compared to OBJECT, would not allow substitution variables in its elements.
      // this type would simplify configuration in such case as the syntax is more straightforward
      case JSONOBJECT:
        parsedObject.add(name, paramObject.get("value").getAsJsonObject());
        break;

      case WATERMARK:

        // String watermarkName = paramObject.get("watermark").getAsString();
        String watermarkValue = paramObject.get("value").getAsString();
        String format = paramObject.get("format").getAsString();
        String timeZoneId = paramObject.has("timezone")
            ? paramObject.get("timezone").getAsString() : DEFAULT_TIMEZONE;
        DateTimeZone timeZone = DateTimeZone.forID(timeZoneId);
        if (values != null && values.get("watermark") != null) {

          // support only long watermark for now
          // support only one watermark called "watermark" for now
          // TODO: support multiple watermarks, each represented by a watermark name

          Long watermarkLow = values.get("watermark").getAsJsonObject().get("low").getAsLong();
          Long watermarkHigh = values.get("watermark").getAsJsonObject().get("high").getAsLong();
          log.debug("found watermark pair: {}, {} in replacement values.", watermarkLow, watermarkHigh);

          // ignore default watermarks
          if (watermarkLow < 0) {
            return null;
          }

          if (format.equals("datetime")) {
            String pattern = paramObject.get("pattern").getAsString();
            DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern(pattern).withZone(timeZone);
            if (watermarkValue.equalsIgnoreCase("low")) {
              parsedObject.addProperty(name, new DateTime().withMillis(watermarkLow).toString(datetimeFormatter));
            } else {
              parsedObject.addProperty(name, new DateTime().withMillis(watermarkHigh).toString(datetimeFormatter));
            }
          } else if (format.equals("epoc-second")) {
            if (watermarkValue.equalsIgnoreCase("low")) {
              parsedObject.addProperty(name, (watermarkLow / 1000));
            } else {
              parsedObject.addProperty(name, watermarkHigh / 1000);
            }
          } else {
            // By default, return the watermark in epoch millisecond format
            if (watermarkValue.equalsIgnoreCase("low")) {
              parsedObject.addProperty(name, watermarkLow);
            } else {
              parsedObject.addProperty(name, watermarkHigh);
            }
          }
        } else {
          return null;
        }
        break;

      case SESSION:
        if (valueCheck(values, ParameterTypes.SESSION.toString(), true, false)) {
          parsedObject.add(name, values.get(ParameterTypes.SESSION.toString()));
        }
        break;

      case PAGESTART:
        if (valueCheck(values, ParameterTypes.PAGESTART.toString(), true, false)) {
          parsedObject.add(name, values.get(ParameterTypes.PAGESTART.toString()));
        }
        break;

      case PAGESIZE:
        if (valueCheck(values, ParameterTypes.PAGESIZE.toString(), true, false)) {
          parsedObject.add(name, values.get(ParameterTypes.PAGESIZE.toString()));
        }
        break;

      case PAGENO:
        if (valueCheck(values, ParameterTypes.PAGENO.toString(), true, false)) {
          parsedObject.add(name, values.get(ParameterTypes.PAGENO.toString()));
        }
        break;

      default:
        break;
    }

    return parsedObject;
  }

  /**
   * check whether a component exists in a JsonObject and whether it meets requirements
   * @param values a JsonObject of parameters
   * @param element the element name
   * @param bRequirePrimitive whether it is required to be primitive
   * @param bAllowBlank whether blank / null is allowed
   * @return true if all checks went through otherwise false
   */
  private boolean valueCheck(JsonObject values, String element, boolean bRequirePrimitive, boolean bAllowBlank) {
    if (!values.has(element)) {
      return false;
    }

    if (bRequirePrimitive && !values.get(element).isJsonPrimitive()) {
      return false;
    }

    if (!bAllowBlank && Strings.isNullOrEmpty(values.get(element).getAsString())) {
      return false;
    }

    return true;
  }

  /**
   *  Support choices on LIST type parameters
   *
   *  If the value given is an array, the first value will be used in a FULL load,
   *  and the second value will be used in Incremental load
   *
   * @param listValue the definition of LIST parameter, which can be a String or JsonArray
   * @param state the State object
   * @return the string value if it is primitive, or the chosen string value based on extract mode
   */
  private String parseListParameter(JsonElement listValue, State state) {
    String listValueString = "";

    if (listValue == null || listValue.isJsonNull()) {
      return listValueString;
    }

    if (listValue.isJsonPrimitive()) {
      listValueString = listValue.getAsString();
    } else if (listValue.isJsonArray() && listValue.getAsJsonArray().size() > 0) {
      if (state.getPropAsBoolean(ConfigurationKeys.EXTRACT_IS_FULL_KEY, Boolean.FALSE)) {
        listValueString = listValue.getAsJsonArray().get(0).getAsString();
      } else {
        listValueString = listValue.getAsJsonArray().size() > 1
            ? listValue.getAsJsonArray().get(1).getAsString()
            : listValue.getAsJsonArray().get(0).getAsString();
      }
    } else {
      listValueString = "";
      log.warn("Unable to parse LIST parameter {}, will use a BLANK string", listValue.toString());
    }
    return listValueString;
  }
}