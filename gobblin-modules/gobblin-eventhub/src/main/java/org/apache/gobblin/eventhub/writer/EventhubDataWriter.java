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
package org.apache.gobblin.eventhub.writer;

import java.io.IOException;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.eventhub.EventhubMetricNames;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.util.concurrent.Futures;
import com.microsoft.azure.servicebus.SharedAccessSignatureTokenProvider;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.writer.Batch;
import org.apache.gobblin.writer.BatchAsyncDataWriter;
import org.apache.gobblin.writer.SyncDataWriter;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;
import org.apache.gobblin.writer.WriteResponseFuture;
import org.apache.gobblin.writer.WriteResponseMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import com.codahale.metrics.Timer;


/**
 * Data Writer for Eventhub.
 * This Data Writer use HttpClient internally and publish data to Eventhub via Post REST API
 * Synchronous model is used here that after each data is sent through httpClient, a response is consumed
 * immediately. Also this class supports sending multiple records in a batch manner.
 *
 * The String input needs to be Unicode based because it will convert to JSON format when using REST API
 *
 * For batch sending, please refer to https://docs.microsoft.com/en-us/rest/api/eventhub/send-batch-events for sending batch records
 * For unicode based json string, please refer to http://rfc7159.net/
 */

@Slf4j
public class EventhubDataWriter implements SyncDataWriter<String>, BatchAsyncDataWriter<String> {

  private static final Logger LOG = LoggerFactory.getLogger(EventhubDataWriter.class);
  private HttpClient httpclient;


  private final String namespaceName;
  private final String eventHubName;
  private final String sasKeyName;
  private final String sasKey;
  private final String targetURI;

  private final Meter bytesWritten;
  private final Meter recordsAttempted;
  private final Meter recordsSuccess;
  private final Meter recordsFailed;
  private final Timer writeTimer;

  private long postStartTimestamp = 0;
  private long sigExpireInMinute = 1;
  private String signature = "";
  private MetricContext metricContext;

  private static final ObjectMapper mapper = new ObjectMapper();

  private static final WriteResponseMapper<Integer> WRITE_RESPONSE_WRAPPER =
      new WriteResponseMapper<Integer>() {

        @Override
        public WriteResponse wrap(final Integer returnCode) {
          return new WriteResponse<Integer>() {
            @Override
            public Integer getRawResponse() {
              return returnCode;
            }

            @Override
            public String getStringResponse() {
              return returnCode.toString();
            }

            @Override
            public long bytesWritten() {
              // Don't know how many bytes were written
              return -1;
            }
          };
        }
      };

  /** User needs to provide eventhub properties */
  public EventhubDataWriter(Properties properties) {
    PasswordManager manager = PasswordManager.getInstance(properties);

    namespaceName = properties.getProperty(BatchedEventhubDataWriter.EVH_NAMESPACE);
    eventHubName =  properties.getProperty(BatchedEventhubDataWriter.EVH_HUBNAME);
    sasKeyName = properties.getProperty(BatchedEventhubDataWriter.EVH_SAS_KEYNAME);
    String encodedSasKey = properties.getProperty(BatchedEventhubDataWriter.EVH_SAS_KEYVALUE);
    sasKey = manager.readPassword(encodedSasKey);
    targetURI = "https://" + namespaceName + ".servicebus.windows.net/" + eventHubName + "/messages";
    httpclient = HttpClients.createDefault();
    metricContext = Instrumented.getMetricContext(new State(properties),EventhubDataWriter.class);
    recordsAttempted = this.metricContext.meter(EventhubMetricNames.EventhubDataWriterMetrics.RECORDS_ATTEMPTED_METER);
    recordsSuccess = this.metricContext.meter(EventhubMetricNames.EventhubDataWriterMetrics.RECORDS_SUCCESS_METER);
    recordsFailed = this.metricContext.meter(EventhubMetricNames.EventhubDataWriterMetrics.RECORDS_FAILED_METER);
    bytesWritten = this.metricContext.meter(EventhubMetricNames.EventhubDataWriterMetrics.BYTES_WRITTEN_METER);
    writeTimer = this.metricContext.timer(EventhubMetricNames.EventhubDataWriterMetrics.WRITE_TIMER);
  }

  /** User needs to provide eventhub properties and an httpClient */
  public EventhubDataWriter(Properties properties, HttpClient httpclient) {
    this (properties);
    this.httpclient = httpclient;
  }

  /**
   * Write a whole batch to eventhub
   */
  public Future<WriteResponse> write (Batch<String> batch, WriteCallback callback) {
    Timer.Context context = writeTimer.time();
    int returnCode = 0;
    LOG.info ("Dispatching batch " + batch.getId());
    recordsAttempted.mark(batch.getRecords().size());
    try {
      String encoded = encodeBatch(batch);
      returnCode = request (encoded);
      WriteResponse<Integer> response = WRITE_RESPONSE_WRAPPER.wrap(returnCode);
      callback.onSuccess(response);
      bytesWritten.mark(encoded.length());
      recordsSuccess.mark(batch.getRecords().size());
    } catch (Exception e) {
      LOG.error("Dispatching batch " + batch.getId() + " failed :" + e.toString());
      callback.onFailure(e);
      recordsFailed.mark(batch.getRecords().size());
    }

    context.close();
    Future<Integer> future = Futures.immediateFuture(returnCode);
    return new WriteResponseFuture<>(future, WRITE_RESPONSE_WRAPPER);
  }

  /**
   * Write a single record to eventhub
   */
  public WriteResponse write (String record) throws IOException {
    recordsAttempted.mark();
    String encoded = encodeRecord(record);
    int returnCode = request (encoded);
    recordsSuccess.mark();
    bytesWritten.mark(encoded.length());
    return WRITE_RESPONSE_WRAPPER.wrap(returnCode);
  }

  /**
   * A signature which contains the duration.
   * After the duration is expired, the signature becomes invalid
   */
  public void refreshSignature () {
    if (postStartTimestamp == 0 || (System.nanoTime() - postStartTimestamp) > Duration.ofMinutes(sigExpireInMinute).toNanos()) {
      // generate signature
      try {
        signature = SharedAccessSignatureTokenProvider
            .generateSharedAccessSignature(sasKeyName, sasKey, namespaceName, Duration.ofMinutes(sigExpireInMinute));
        postStartTimestamp = System.nanoTime();
        LOG.info ("Signature is refreshing: " + signature);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Send an encoded string to the Eventhub using post method
   */
  private int request (String encoded) throws IOException {
    refreshSignature();
    HttpPost httpPost = new HttpPost(targetURI);
    httpPost.setHeader("Content-type", "application/vnd.microsoft.servicebus.json");
    httpPost.setHeader("Authorization", signature);
    httpPost.setHeader("Host", namespaceName + ".servicebus.windows.net ");

    StringEntity entity = new StringEntity(encoded);
    httpPost.setEntity(entity);

    HttpResponse response = httpclient.execute(httpPost);
    StatusLine status = response.getStatusLine();
    HttpEntity entity2 = response.getEntity();
    // do something useful with the response body
    // and ensure it is fully consumed
    EntityUtils.consume(entity2);

    int returnCode = status.getStatusCode();
    if (returnCode != HttpStatus.SC_CREATED) {
      LOG.error (new IOException(status.getReasonPhrase()).toString());
      throw new IOException(status.getReasonPhrase());
    }

    return returnCode;
  }

  /**
   * Each record of batch is wrapped by a 'Body' json object
   * put this new object into an array, encode the whole array
   */
  private String encodeBatch (Batch<String> batch) throws IOException {
    // Convert original json object to a new json object with format {"Body": "originalJson"}
    // Add new json object to an array and send the whole array to eventhub using REST api
    // Refer to https://docs.microsoft.com/en-us/rest/api/eventhub/send-batch-events
    List<String> records = batch.getRecords();
    ArrayList<EventhubRequest> arrayList = new ArrayList<>();


    for (String record: records) {
      arrayList.add(new EventhubRequest(record));
    }
    return mapper.writeValueAsString (arrayList);
  }

  /**
   * A single record is wrapped by a 'Body' json object
   * encode this json object
   */
  private String encodeRecord (String record)throws  IOException {
    // Convert original json object to a new json object with format {"Body": "originalJson"}
    // Add new json object to an array and send the whole array to eventhub using REST api
    // Refer to https://docs.microsoft.com/en-us/rest/api/eventhub/send-batch-events
    ArrayList<EventhubRequest> arrayList = new ArrayList<>();
    arrayList.add(new EventhubRequest(record));

    return mapper.writeValueAsString (arrayList);
  }

  /**
   * Close the HttpClient
   */
  public void close() throws IOException {
    if (httpclient instanceof CloseableHttpClient) {
      ((CloseableHttpClient)httpclient).close();
    }
  }

  public void cleanup() {
    // do nothing
  }

  public void flush() {
    // do nothing
  }
}
