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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.log4j.Log4j;

import gobblin.http.ResponseStatus;


/**
 * Synchronous HTTP writer which sends one record at a time
 *
 * @param <D> type of record
 * @param <RQ> type of request
 * @param <RP> type of response
 */
@Log4j
public class RecordSyncHttpWriter<D, RQ, RP> extends HttpWriterBase<D, RQ, RP> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncHttpWriter.class);

  public RecordSyncHttpWriter(HttpWriterBaseBuilder builder) {
    super(builder);
  }

  /**
   * Process the record
   * {@inheritDoc}
   */
  @Override
  public void writeImpl(D record)
      throws IOException {
    WriteRequest<RQ> writeRequest = requestBuilder.buildRequest(record);
    if (writeRequest == null) {
      return;
    }

    RP response = client.sendRequest(writeRequest.getRawRequest());
    ResponseStatus status = responseHandler.handleResponse(response);

    int statusCode = status.getStatusCode();
    if (statusCode >= 200 && statusCode < 400) {
      // Write succeeds
      bytesWritten += writeRequest.getBytesWritten();
      numRecordsWritten++;
    } else if (statusCode >= 400 && statusCode < 500) {
      // Client error. Fail!
      IOException e = new IOException("Write failed on invalid request");
      LOG.error("Got status code: " + statusCode, e);
      throw e;
    } else {
      // Server side error. Fail!
      IOException e = new IOException("Server side error");
      LOG.error("Got status code: " + statusCode, e);
      throw e;
    }
  }
}
