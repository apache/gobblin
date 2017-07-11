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

import com.google.api.client.util.Charsets;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import gobblin.configuration.State;

import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link AuditCountClient} which uses {@link org.apache.http.client.HttpClient}
 * to perform audit count query.
 */
@Slf4j
@ThreadSafe
public class PinotAuditCountHttpClient implements AuditCountClient {

  // Keys
  public static final String PINOT_AUDIT_HTTP = "pinot.audit.http";
  public static final String CONNECTION_MAX_TOTAL = PINOT_AUDIT_HTTP + "max.total";
  public static final int DEFAULT_CONNECTION_MAX_TOTAL = 10;
  public static final String MAX_PER_ROUTE = PINOT_AUDIT_HTTP + "max.per.route";
  public static final int DEFAULT_MAX_PER_ROUTE = 10;
  public static final String TARGET_HOST = PINOT_AUDIT_HTTP + "target.host";
  public static final String TARGET_PORT = PINOT_AUDIT_HTTP + "target.port";

  // Http Client
  private PoolingHttpClientConnectionManager cm;
  private CloseableHttpClient httpClient;
  private static final JsonParser PARSER = new JsonParser();

  private String targetUrl;

  /**
   * Constructor
   */
  public PinotAuditCountHttpClient(State state) {
    int maxTotal = state.getPropAsInt(CONNECTION_MAX_TOTAL, DEFAULT_CONNECTION_MAX_TOTAL);
    int maxPerRoute = state.getPropAsInt(MAX_PER_ROUTE, DEFAULT_MAX_PER_ROUTE);

    cm = new PoolingHttpClientConnectionManager();
    cm.setMaxTotal(maxTotal);
    cm.setDefaultMaxPerRoute(maxPerRoute);
    httpClient = HttpClients.custom()
            .setConnectionManager(cm)
            .build();

    String host = state.getProp(TARGET_HOST);
    int port = state.getPropAsInt(TARGET_PORT);
    targetUrl = host + ":" + port + "/pql?pql=";
  }

  /**
   * A thread-safe method which fetches a tier-to-count mapping.
   * The returned json object from Pinot contains below information
   * {
   *    "aggregationResults":[
   *      {
   *          "groupByResult":[
   *            {
   *              "value":"172765137.00000",
   *              "group":[
   *                "kafka-08-tracking-local"
   *              ]
   *            }
   *          ]
   *      }
   *    ],
   *    "exceptions":[
   *    ]
   *    .....
   * }
   * @param datasetName name of dataset
   * @param start time start point in milliseconds
   * @param end time end point in milliseconds
   * @return A tier to record count mapping when succeeded. Otherwise a null value is returned
   */
  public Map<String, Long> fetch (String datasetName, long start, long end)  throws IOException {
    Map<String, Long> map = new HashMap<>();
    String query = "select tier, sum(count) from kafkaAudit where " +
            "eventType=\""            + datasetName   + "\" and " +
            "beginTimestamp >= \""    + start         + "\" and " +
            "beginTimestamp < \""     + end           + "\" group by tier";

    String fullURL = targetUrl + URLEncoder.encode (query, Charsets.UTF_8.toString());
    HttpGet req = new HttpGet(fullURL);
    String rst = null;
    HttpEntity entity = null;
    log.info ("Full url for {} is {}", datasetName, fullURL);

    try {
      CloseableHttpResponse response = httpClient.execute(req, HttpClientContext.create());
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode < 200 || statusCode >= 300) {
        throw new IOException(
                String.format("status code: %d, reason: %s", statusCode, response.getStatusLine().getReasonPhrase()));
      }

      entity = response.getEntity();
      rst = EntityUtils.toString(entity);
    } finally {
      if (entity != null) {
        EntityUtils.consume(entity);
      }
    }

    JsonObject all = PARSER.parse(rst).getAsJsonObject();
    JsonArray aggregationResults = all.getAsJsonArray("aggregationResults");
    if (aggregationResults == null || aggregationResults.size() == 0) {
      log.error (all.toString());
      throw new IOException("No aggregation results " + all.toString());
    }

    JsonObject aggregation = (JsonObject) aggregationResults.get(0);
    JsonArray groupByResult = aggregation.getAsJsonArray("groupByResult");
    if (groupByResult == null || groupByResult.size() == 0) {
      log.error (aggregation.toString());
      throw new IOException("No aggregation results " + aggregation.toString());
    }

    log.info ("Audit count for {} is {}", datasetName, groupByResult);
    for (JsonElement ele : groupByResult){
      JsonObject record = (JsonObject) ele;
      map.put(record.getAsJsonArray("group").get(0).getAsString(), (long) Double.parseDouble(record.get("value").getAsString()));
    }

    return map;
  }
}
