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

package org.apache.gobblin.service;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import com.linkedin.restli.server.resources.BaseResource;
import com.typesafe.config.Config;

/**
 * Use this class to get who sends the request.
 */
public abstract class RequesterService {

  protected Config sysConfig;

  public RequesterService(Config config) {
    sysConfig = config;
  }

  public static final String REQUESTER_LIST = "gobblin.service.requester.list";

  private static final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * <p> This implementation converts a given list to a json string.
   * Due to json string may have reserved keyword that can confuse
   * {@link Config}, we first use Base64 to encode the json string,
   * then use URL encoding to remove characters like '+,/,='.
   */
  public String serialize(List<ServiceRequester> requestersList) throws IOException {
    String arrayToJson = objectMapper.writeValueAsString(requestersList);
    String encodedString = Base64.getEncoder().encodeToString(arrayToJson.getBytes());
    return URLEncoder.encode(encodedString, "UTF-8");
  }

  /**
   * <p> This implementation decode a given string encoded by
   * {@link #serialize(List)}.
   */
  public List<ServiceRequester> deserialize(String encodedString) throws IOException {
    String urlDecoded = URLDecoder.decode(encodedString, "UTF-8");
    byte[] decodedBytes = Base64.getDecoder().decode(urlDecoded);
    String serialized = new String(decodedBytes);
    TypeReference<List<ServiceRequester>> mapType = new TypeReference<List<ServiceRequester>>() {};
    List<ServiceRequester> jsonToPersonList = objectMapper.readValue(serialized, mapType);
    return jsonToPersonList;
  }

  protected abstract List<ServiceRequester> findRequesters(BaseResource resource);
}
