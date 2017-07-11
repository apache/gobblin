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

package gobblin.compaction.audit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import gobblin.configuration.State;

import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A {@link AuditCountClient} which uses {@link org.apache.http.client.HttpClient}
 * to perform audit count query.
 */
@Slf4j
@ThreadSafe
public class KafkaAuditCountHttpClient implements AuditCountClient {

  // Keys
  public static final String KAFKA_AUDIT_HTTP = "kafka.audit.http";
  public static final String CONNECTION_MAX_TOTAL = KAFKA_AUDIT_HTTP + "max.total";
  public static final int DEFAULT_CONNECTION_MAX_TOTAL = 10;
  public static final String MAX_PER_ROUTE = KAFKA_AUDIT_HTTP + "max.per.route";
  public static final int DEFAULT_MAX_PER_ROUTE = 10;


  public static final String KAFKA_AUDIT_REST_BASE_URL = "kafka.audit.rest.base.url";
  public static final String KAFKA_AUDIT_REST_MAX_TRIES = "kafka.audit.rest.max.tries";
  public static final String KAFKA_AUDIT_REST_START_QUERYSTRING_KEY = "kafka.audit.rest.querystring.start";
  public static final String KAFKA_AUDIT_REST_END_QUERYSTRING_KEY = "kafka.audit.rest.querystring.end";
  public static final String KAFKA_AUDIT_REST_START_QUERYSTRING_DEFAULT = "begin";
  public static final String KAFKA_AUDIT_REST_END_QUERYSTRING_DEFAULT = "end";


  // Http Client
  private PoolingHttpClientConnectionManager cm;
  private CloseableHttpClient httpClient;
  private static final JsonParser PARSER = new JsonParser();

  private final String baseUrl;
  private final String startQueryString;
  private final String endQueryString;
  private final int maxNumTries;
  /**
   * Constructor
   */
  public KafkaAuditCountHttpClient (State state) {
    int maxTotal = state.getPropAsInt(CONNECTION_MAX_TOTAL, DEFAULT_CONNECTION_MAX_TOTAL);
    int maxPerRoute = state.getPropAsInt(MAX_PER_ROUTE, DEFAULT_MAX_PER_ROUTE);

    cm = new PoolingHttpClientConnectionManager();
    cm.setMaxTotal(maxTotal);
    cm.setDefaultMaxPerRoute(maxPerRoute);
    httpClient = HttpClients.custom()
            .setConnectionManager(cm)
            .build();

    this.baseUrl = state.getProp(KAFKA_AUDIT_REST_BASE_URL);
    this.maxNumTries = state.getPropAsInt(KAFKA_AUDIT_REST_MAX_TRIES, 5);
    this.startQueryString = state.getProp(KAFKA_AUDIT_REST_START_QUERYSTRING_KEY, KAFKA_AUDIT_REST_START_QUERYSTRING_DEFAULT);
    this.endQueryString = state.getProp(KAFKA_AUDIT_REST_END_QUERYSTRING_KEY, KAFKA_AUDIT_REST_END_QUERYSTRING_DEFAULT);
  }


  public Map<String, Long> fetch (String datasetName, long start, long end)  throws IOException {
    String fullUrl =
            (this.baseUrl.endsWith("/") ? this.baseUrl : this.baseUrl + "/") + StringUtils.replaceChars(datasetName, '/', '.')
                    + "?" + this.startQueryString + "=" + start + "&" + this.endQueryString + "=" + end;
    log.info("Full URL is " + fullUrl);
    String response = getHttpResponse(fullUrl);
   return parseResponse (fullUrl, response, datasetName);
  }



  /**
   * Expects <code>response</code> being parsed to be as below.
   *
   * <pre>
   * {
   *  "result": {
   *     "hadoop-tracking-lva1tarock-08": 79341895,
   *     "hadoop-tracking-uno-08": 79341892,
   *     "kafka-08-tracking-local": 79341968,
   *     "kafka-corp-lca1-tracking-agg": 79341968,
   *     "kafka-corp-ltx1-tracking-agg": 79341968,
   *     "producer": 69483513
   *   }
   * }
   * </pre>
   */
  @VisibleForTesting
  public static Map<String, Long> parseResponse(String fullUrl, String response, String topic) throws IOException {
    Map<String, Long> result = Maps.newHashMap();
    JsonObject countsPerTier = null;
    try {
      JsonObject jsonObj = PARSER.parse(response).getAsJsonObject();

      countsPerTier = jsonObj.getAsJsonObject("result");
    } catch (Exception e) {
      throw new IOException(String.format("Unable to parse JSON response: %s for request url: %s ", response,
              fullUrl), e);
    }

    Set<Map.Entry<String, JsonElement>> entrySet = countsPerTier.entrySet();

    for(Map.Entry<String, JsonElement> entry : entrySet) {
      String tier = entry.getKey();
      long count = Long.parseLong(entry.getValue().getAsString());
      result.put(tier, count);
    }

    return result;
  }



  private String getHttpResponse(String fullUrl) throws IOException {
    HttpUriRequest req = new HttpGet(fullUrl);

    for (int numTries = 0;; numTries++) {
      try (CloseableHttpResponse response = this.httpClient.execute(req)) {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode < 200 || statusCode >= 300) {
          throw new IOException(
                  String.format("status code: %d, reason: %s", statusCode, response.getStatusLine().getReasonPhrase()));
        }
        return EntityUtils.toString(response.getEntity());
      } catch (IOException e) {
        String errMsg = "Unable to get or parse HTTP response for " + fullUrl;
        if (numTries >= this.maxNumTries) {
          throw new IOException (errMsg, e);
        }
        long backOffSec = (numTries + 1) * 2;
        log.error(errMsg + ", will retry in " + backOffSec + " sec", e);
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(backOffSec));
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
