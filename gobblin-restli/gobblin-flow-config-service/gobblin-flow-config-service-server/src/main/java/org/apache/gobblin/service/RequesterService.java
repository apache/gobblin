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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
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

  private static final Gson gson = new Gson();

  /**
   * <p> This implementation converts a given list to a json string.
   */
  public static String serialize(List<ServiceRequester> requesterList) throws IOException {
    try {
      return gson.toJson(requesterList);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  /**
   * <p> This implementation decode a given string encoded by
   * {@link #serialize(List)}.
   */
  public static List<ServiceRequester> deserialize(String encodedString) throws IOException {
    try {
      return gson.fromJson(encodedString, new TypeToken<List<ServiceRequester>>() {}.getType());
    } catch (JsonSyntaxException e) {
      // For backward compatibility
      String base64Str = URLDecoder.decode(encodedString, StandardCharsets.UTF_8.name());
      byte[] decodedBytes = Base64.getDecoder().decode(base64Str);
      String jsonList = new String(decodedBytes, StandardCharsets.UTF_8);
      TypeReference<List<ServiceRequester>> mapType = new TypeReference<List<ServiceRequester>>() {};
      return new ObjectMapper().readValue(jsonList, mapType);
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  protected abstract List<ServiceRequester> findRequesters(BaseResource resource);

  /**
   * Return true if the requester is whitelisted to always be accepted
   */
  public boolean isRequesterWhitelisted(List<ServiceRequester> requesterList) {
    return false;
  }

  /**
   * returns true if the requester is allowed to make this request.
   * This default implementation accepts all requesters.
   * @param originalRequesterList original requester list
   * @param currentRequesterList current requester list
   * @return true if the requester is allowed to make this request, false otherwise
   */
  protected boolean isRequesterAllowed(
      List<ServiceRequester> originalRequesterList, List<ServiceRequester> currentRequesterList) {
    return true;
  }
}
