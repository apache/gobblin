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
import com.google.gson.JsonObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.util.HttpRequestMethod;


/**
 * This structure holds static parameters that are commonly used in HTTP protocol.
 *
 * @author chrli
 */
@Slf4j
@Getter (AccessLevel.PUBLIC)
@Setter(AccessLevel.PUBLIC)
public class HttpKeys extends JobKeys {
  final private static List<MultistageProperties> ESSENTIAL_PARAMETERS = Lists.newArrayList(
      MultistageProperties.MSTAGE_SOURCE_URI,
      MultistageProperties.SOURCE_CONN_USERNAME,
      MultistageProperties.SOURCE_CONN_PASSWORD,
      MultistageProperties.MSTAGE_AUTHENTICATION,
      MultistageProperties.MSTAGE_HTTP_REQUEST_METHOD,
      MultistageProperties.MSTAGE_HTTP_REQUEST_HEADERS,
      MultistageProperties.MSTAGE_SESSION_KEY_FIELD);

  private JsonObject authentication = new JsonObject();
  private JsonObject httpRequestHeaders = new JsonObject();
  private Map<String, String> httpRequestHeadersWithAuthentication = new HashMap<>();
  private String httpSourceUri = null;
  private String httpRequestMethod = HttpRequestMethod.GET.toString();
  private JsonObject initialParameters = new JsonObject();
  private Map<String, List<Integer>> httpStatuses = new HashMap<>();
  private Map<String, List<String>> httpStatusReasons = new HashMap<>();

  @Override
  public void logDebugAll() {
    super.logDebugAll();
    log.debug("These are values in HttpSource");
    log.debug("Http Source Uri: {}", httpSourceUri);
    log.debug("Http Request Headers: {}", httpRequestHeaders);
    //log.debug("Http Request Headers with Authentication: {}", httpRequestHeadersWithAuthentication.toString());
    log.debug("Http Request Method: {}", httpRequestMethod);
    log.debug("Http Statuses: {}", httpStatuses);
    log.debug("Initial values of dynamic parameters: {}", initialParameters);
  }

  @Override
  public void logUsage(State state) {
    super.logUsage(state);
    for (MultistageProperties p: ESSENTIAL_PARAMETERS) {
      log.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getValidNonblankWithDefault(state));
    }
  }
}
