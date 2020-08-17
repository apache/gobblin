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

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


/**
 * Utility functions for string manipulation
 */
public interface VariableUtils {
  Gson GSON = new Gson();
  String OPENING_RE = "\\{\\{";
  String OPENING = "{{";
  String CLOSING = "}}";
  Pattern PATTERN = Pattern.compile(OPENING_RE + "([a-zA-Z0-9_\\-.$]+)" + CLOSING);

  /**
   * Replace placeholders or variables in a JsonObject
   *
   * @param templateJsonObject the template JsonObject with placeholders
   * @param parameters the replacement values
   * @return the replaced JsonObject
   * @throws UnsupportedEncodingException
   */
  static JsonObject replace(JsonObject templateJsonObject, JsonObject parameters)
      throws UnsupportedEncodingException {
    String replacedString = replaceWithTracking(templateJsonObject.toString(), parameters, false).getKey();
    return GSON.fromJson(replacedString, JsonObject.class);
  }

  /**
   * Replace placeholders or variables in a JsonObject
   *
   * @param templateJsonObject the template JsonObject with placeholders
   * @param parameters the replacement values
   * @param encode whether to encode the value string, note this function will not encode
   *               the template string in any case, the encoding only applies to the replacement values
   * @return the replaced JsonObject
   * @throws UnsupportedEncodingException
   */
  public static JsonObject replace(JsonObject templateJsonObject, JsonObject parameters, Boolean encode)
      throws UnsupportedEncodingException {
    String replacedString = replaceWithTracking(templateJsonObject.toString(), parameters, encode).getKey();
    return GSON.fromJson(replacedString, JsonObject.class);
  }

  /**
   *
   * @param templateString a template string with placeholders or variables
   * @param parameters the replacement values coded in a JsonObject format
   * @return a pair made of replaced string and whatever parameters that were not used
   * @throws UnsupportedEncodingException
   */
  static Pair<String, JsonObject> replaceWithTracking(String templateString, JsonObject parameters)
      throws UnsupportedEncodingException {
    return replaceWithTracking(templateString, parameters, false);
  }

  /**
   *
   * @param templateString a template string with placeholders or variables
   * @param parameters the replacement values coded in a JsonObject format
   * @param encode whether to encode the value string, note this function will not encode
   *               the template string in any case, the encoding only applies to the replacement values
   * @return a pair made of replaced string and whatever parameters that were not used
   * @throws UnsupportedEncodingException
   */
  static Pair<String, JsonObject> replaceWithTracking(String templateString, JsonObject parameters, Boolean encode)
      throws UnsupportedEncodingException {
    String replacedString = templateString;
    JsonObject remainingParameters = new JsonObject();

    List<String> variables = getVariables(templateString);

    for (Map.Entry<String, JsonElement> entry : parameters.entrySet()) {
      if (variables.contains(entry.getKey())) {
        replacedString = replacedString.replace(OPENING + entry.getKey() + CLOSING,
            encode ? URLEncoder.encode(entry.getValue().getAsString(), "UTF-8") : entry.getValue().getAsString());
      } else {
        remainingParameters.add(entry.getKey(), entry.getValue());
      }
    }
    return new ImmutablePair<>(replacedString, remainingParameters);
  }

  /**
   * retrieve a list of placeholders or variables from the template, placeholders or variables are
   * identified by alpha numeric strings surrounded by {{}}
   *
   * @param templateString the template with placeholders or variables
   * @return a list of placeholders or variables
   */
  static List<String> getVariables(String templateString) {
    List<String> paramList = Lists.newArrayList();
    Matcher matcher = PATTERN.matcher(templateString);
    while (matcher.find()) {
      paramList.add(matcher.group(1));
    }
    return paramList;
  }
}
