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
package org.apache.gobblin.converter;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.async.AsyncRequest;
import org.apache.gobblin.async.AsyncRequestBuilder;
import org.apache.gobblin.async.BufferedRecord;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.http.HttpClient;
import org.apache.gobblin.http.HttpOperation;
import org.apache.gobblin.http.ResponseHandler;
import org.apache.gobblin.http.ResponseStatus;
import org.apache.gobblin.utils.HttpConstants;
import org.apache.gobblin.writer.WriteCallback;

/**
 * This converter converts an input record (DI) to an output record (DO) which
 * contains original input data and http request & response info.
 *
 * Sequence:
 * Convert DI to HttpOperation
 * Convert HttpOperation to RQ (by internal AsyncRequestBuilder)
 * Execute http request, get response RP (by HttpClient)
 * Combine info (DI, RQ, RP, status, etc..) to generate output DO
 */
@Slf4j
public abstract class HttpJoinConverter<SI, SO, DI, DO, RQ, RP> extends Converter<SI, SO, DI, DO> {
  public static final String CONF_PREFIX = "gobblin.converter.http.";
  public static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(HttpConstants.CONTENT_TYPE, "application/json")
          .put(HttpConstants.VERB, "GET")
          .build());

  protected HttpClient<RQ, RP> httpClient = null;
  protected ResponseHandler<RQ, RP> responseHandler = null;
  protected AsyncRequestBuilder<GenericRecord, RQ> requestBuilder = null;

  public HttpJoinConverter init(WorkUnitState workUnitState) {
    super.init(workUnitState);
    Config config = ConfigBuilder.create().loadProps(workUnitState.getProperties(), CONF_PREFIX).build();
    config = config.withFallback(DEFAULT_FALLBACK);

    httpClient = createHttpClient(config, workUnitState.getTaskBroker());
    responseHandler = createResponseHandler(config);
    requestBuilder = createRequestBuilder(config);
    return this;
  }

  @Override
  public final SO convertSchema(SI inputSchema, WorkUnitState workUnitState)
      throws SchemaConversionException {
    return convertSchemaImpl(inputSchema, workUnitState);
  }

  protected abstract HttpClient<RQ, RP>   createHttpClient(Config config, SharedResourcesBroker<GobblinScopeTypes> broker);
  protected abstract ResponseHandler<RQ, RP> createResponseHandler(Config config);
  protected abstract AsyncRequestBuilder<GenericRecord, RQ> createRequestBuilder(Config config);
  protected abstract HttpOperation generateHttpOperation (DI inputRecord, State state);
  protected abstract SO convertSchemaImpl (SI inputSchema, WorkUnitState workUnitState) throws SchemaConversionException;
  protected abstract DO convertRecordImpl (SO outputSchema, DI input, RQ rawRequest, ResponseStatus status) throws DataConversionException;

  @Override
  public final Iterable<DO> convertRecord(SO outputSchema, DI inputRecord, WorkUnitState workUnitState)
      throws DataConversionException {

    // Convert DI to HttpOperation
    HttpOperation operation = generateHttpOperation(inputRecord, workUnitState);
    BufferedRecord<GenericRecord> bufferedRecord = new BufferedRecord<>(operation, WriteCallback.EMPTY);

    // Convert HttpOperation to RQ
    Queue<BufferedRecord<GenericRecord>> buffer = new LinkedBlockingDeque<>();
    buffer.add(bufferedRecord);
    AsyncRequest<GenericRecord, RQ> request = this.requestBuilder.buildRequest(buffer);
    RQ rawRequest = request.getRawRequest();

    // Execute query and get response

    try {
      RP response = httpClient.sendRequest(rawRequest);

      ResponseStatus status = responseHandler.handleResponse(request, response);


      switch (status.getType()) {
        case OK:
        case CLIENT_ERROR:
          // Convert (DI, RQ, RP etc..) to output DO
          log.debug ("{} send with status type {}", rawRequest, status.getType());
          DO output = convertRecordImpl (outputSchema, inputRecord, rawRequest, status);
          return new SingleRecordIterable<>(output);
        case SERVER_ERROR:
          // Server side error. Retry
          throw new DataConversionException(rawRequest + " send failed due to server error");
        default:
          throw new DataConversionException(rawRequest + " Should not reach here");
      }
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
  }

  public void close() throws IOException {
    this.httpClient.close();
  }
}
