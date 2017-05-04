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
package gobblin.writer.http;

import java.io.IOException;

import lombok.extern.log4j.Log4j;

import gobblin.http.RequestBuilder;


/**
 * Synchronous HTTP writer which sends one record at time
 *
 * @param <D> type of record
 * @param <RQ> type of request
 * @param <RP> type of response
 */
@Log4j
public class RecordSyncHttpWriter<D, RQ, RP> extends HttpWriterBase<D, RQ, RP> {
  RequestBuilder<D, RQ> requestBuilder;

  public RecordSyncHttpWriter(HttpWriterBaseBuilder builder) {
    super(builder);
    requestBuilder = builder.getRequestBuilder();
  }

  /**
   * Process the record
   * {@inheritDoc}
   */
  @Override
  public void writeImpl(D record) throws IOException {
    RQ request = requestBuilder.buildRequest(record);
    if (request != null) {
      RP response = client.sendRequest(request);
      responseHandler.handleResponse(response);
      numRecordsWritten++;
    }
  }
}
