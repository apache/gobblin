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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.squareup.okhttp.HttpUrl;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;


/**
 * Enum object to facilitate the handling of different types of HTTP requests.
 *
 * The difference between GET and POST/PUT is that URI parameters are coded differently.
 *
 * However, in all request types, an URI path might be dynamically constructed. For
 * example, https://domain/api/v1.5/surveys/{{id}} is a dynamic URI. The end point might
 * support GET or POST.
 *
 * So if a GET request has 2 parameters, id=1 and format=avro, then the URI will be transformed to
 * https://domain/api/v1.5/surveys/1?format=avro.
 *
 * However if a POST request has 2 parameters, id=1 and name=xxx, then the URI will be transformed to
 * https://domain/api/v1.5/surveys/1, and the name=xxx will be in the POST request's entity content.
 *
 * Note:
 *
 * - URI path variables or placeholders are defined using {{placeholder-name}}
 * - Placeholders or URI variables have to be alpha numeric
 *
 * @author chrli
 */

@Slf4j
public enum HttpRequestMethod {
  GET("GET") {
    @Override
    protected HttpUriRequest getHttpRequestContentJson(String uriTemplate, JsonObject parameters)
        throws UnsupportedEncodingException {
      Pair<String, JsonObject> replaced = replaceVariables(uriTemplate, parameters);
      return new HttpGet(appendParameters(replaced.getKey(), replaced.getValue()));
    }

    @Override
    protected HttpUriRequest getHttpRequestContentUrlEncoded(String uriTemplate, JsonObject parameters)
        throws UnsupportedEncodingException {
      return getHttpRequestContentJson(uriTemplate, parameters);
    }
  },

  POST("POST") {
    @Override
    protected HttpUriRequest getHttpRequestContentJson(String uriTemplate, JsonObject parameters)
        throws UnsupportedEncodingException {
      Pair<String, JsonObject> replaced = replaceVariables(uriTemplate, parameters);
      return setEntity(new HttpPost(replaced.getKey()), replaced.getValue().toString());
    }

    @Override
    protected HttpUriRequest getHttpRequestContentUrlEncoded(String uriTemplate, JsonObject parameters)
        throws UnsupportedEncodingException {
      Pair<String, JsonObject> replaced = replaceVariables(uriTemplate, parameters);
      String urlEncoded = jsonToUrlEncoded(replaced.getValue());
      return setEntity(new HttpPost(replaced.getKey()), urlEncoded);
    }
  },

  PUT("PUT") {
    @Override
    protected HttpUriRequest getHttpRequestContentJson(String uriTemplate, JsonObject parameters)
        throws UnsupportedEncodingException {
      Pair<String, JsonObject> replaced = replaceVariables(uriTemplate, parameters);
      return setEntity(new HttpPut(replaced.getKey()), replaced.getValue().toString());
    }

    @Override
    protected HttpUriRequest getHttpRequestContentUrlEncoded(String uriTemplate, JsonObject parameters)
        throws UnsupportedEncodingException {
      Pair<String, JsonObject> replaced = replaceVariables(uriTemplate, parameters);
      return setEntity(new HttpPut(replaced.getKey()), jsonToUrlEncoded(replaced.getValue()));
    }
  },

  DELETE("DELETE") {
    @Override
    protected HttpUriRequest getHttpRequestContentJson(String uriTemplate, JsonObject parameters)
        throws UnsupportedEncodingException {
      Pair<String, JsonObject> replaced = replaceVariables(uriTemplate, parameters);
      return new HttpDelete(replaced.getKey());
    }

    @Override
    protected HttpUriRequest getHttpRequestContentUrlEncoded(String uriTemplate, JsonObject parameters)
        throws UnsupportedEncodingException {
      Pair<String, JsonObject> replaced = replaceVariables(uriTemplate, parameters);
      return new HttpDelete(replaced.getKey());
    }
  };

  private String name;

  HttpRequestMethod(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }

  /**
   * This is the public method to generate HttpUriRequest for each type of Http Method
   * @param uriTemplate input URI, which might contain place holders
   * @param parameters parameters to be add to URI or to request Entity
   * @param headers Http header tags
   * @return HttpUriRequest ready for connection
   */
  public HttpUriRequest getHttpRequest(final String uriTemplate, final JsonObject parameters, final Map<String, String> headers)
      throws UnsupportedEncodingException {
    HttpUriRequest request;

    // substitute variables in headers
    Map<String, String> headersCopy = new HashMap<>();
    JsonObject parametersCopy = JsonUtils.deepCopy(parameters).getAsJsonObject();
    for (Map.Entry<String, String> entry: headers.entrySet()) {
      Pair<String, JsonObject> replaced = VariableUtils.replaceWithTracking(entry.getValue(), parameters);
      if (!replaced.getLeft().equals(entry.getValue())) {
        parametersCopy = JsonUtils.deepCopy(replaced.getRight()).getAsJsonObject();
        headersCopy.put(entry.getKey(), replaced.getLeft());
        log.info("Substituted header string: {} = {}", entry.getKey(), replaced.getLeft());
      } else {
        headersCopy.put(entry.getKey(), entry.getValue());
      }
    }

    log.info("Final parameters for HttpRequest: {}", parametersCopy.toString());
    if (headersCopy.containsKey("Content-Type")
        && headersCopy.get("Content-Type").equals("application/x-www-form-urlencoded")) {
      request = getHttpRequestContentUrlEncoded(uriTemplate, parametersCopy);
    } else {
      request = getHttpRequestContentJson(uriTemplate, parametersCopy);
    }

    for (Map.Entry<String, String> entry: headersCopy.entrySet()) {
      request.addHeader(entry.getKey(), entry.getValue());
    }
    return request;
  }

  /**
   * This method shall be overwritten by each enum element.
   * @param uriTemplate input URI, which might contain place holders
   * @param parameters parameters to be add to URI or to request Entity
   * @return HttpUriRequest object where content is set per application/json
   */
  protected abstract HttpUriRequest getHttpRequestContentJson(String uriTemplate, JsonObject parameters)
      throws UnsupportedEncodingException;

  /**
   * This method shall be overwritten by each enum element.
   * @param uriTemplate input URI, which might contain place holders
   * @param parameters parameters to be add to URI or to request Entity
   * @return HttpUriRequest object where content is set per application/x-wwww-form-urlencoded
   */
  protected abstract HttpUriRequest getHttpRequestContentUrlEncoded(String uriTemplate, JsonObject parameters)
      throws UnsupportedEncodingException;

  protected Pair<String, JsonObject> replaceVariables(String uriTemplate, JsonObject parameters)
      throws UnsupportedEncodingException {
    return VariableUtils.replaceWithTracking(uriTemplate, parameters, true);
  }

  protected String appendParameters(String uri, JsonObject parameters) throws UnsupportedEncodingException {
    if (uri == null) {
      return uri;
    }

    HttpUrl url = HttpUrl.parse(uri);
    if (url != null) {
      HttpUrl.Builder builder = url.newBuilder();
      for (Map.Entry<String, JsonElement> entry : parameters.entrySet()) {
        String key = entry.getKey();
        builder.addEncodedQueryParameter(key,
            URLEncoder.encode(parameters.get(key).getAsString(), StandardCharsets.UTF_8.toString()));
      }
      url = builder.build();
    }
    return url != null ? url.toString() : uri;
  }

  /**
   * Convert Json formatted parameter set to Url Encoded format as requested by
   * Content-Type: application/x-www-form-urlencoded
   * Json Example:
   *    {"param1": "value1", "param2": "value2"}
   *
   * URL Encoded Example:
   *   param1=value1&param2=value2
   *
   * @param parameters Json structured parameters
   * @return URL encoded parameters
   */
  protected String jsonToUrlEncoded(JsonObject parameters) {
    HttpUrl.Builder builder = new HttpUrl.Builder().scheme("https").host("www.dummy.com");
    for (Map.Entry<String, JsonElement> entry : parameters.entrySet()) {
      String key = entry.getKey();
      builder.addQueryParameter(key, parameters.get(key).getAsString());
    }
    return builder.build().encodedQuery();
  }

  protected HttpUriRequest setEntity(HttpEntityEnclosingRequestBase requestBase, String stringEntity)
      throws UnsupportedEncodingException {
    requestBase.setEntity(new StringEntity(stringEntity));
    return requestBase;
  }
}
