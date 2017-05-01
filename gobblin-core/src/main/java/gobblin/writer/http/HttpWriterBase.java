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

import gobblin.http.HttpClient;
import gobblin.http.ResponseHandler;
import gobblin.instrumented.writer.InstrumentedDataWriter;


/**
 * Base class for HTTP writers
 *
 * @param <D> type of record
 * @param <RQ> type of request
 * @param <RP> type of response
 */
@Log4j
public abstract class HttpWriterBase<D, RQ, RP> extends InstrumentedDataWriter<D> {
  HttpClient<RQ, RP> client;
  WriteRequestBuilder<D, RQ> requestBuilder;
  ResponseHandler<RP> responseHandler;

  protected long numRecordsWritten = 0L;
  protected long bytesWritten = 0L;

  public HttpWriterBase(HttpWriterBaseBuilder builder) {
    super(builder.getState());
    client = builder.getClient();
    requestBuilder = builder.getRequestBuilder();
    responseHandler = builder.getResponseHandler();
  }

  @Override
  public long recordsWritten() {
    return numRecordsWritten;
  }

  @Override
  public long bytesWritten() throws IOException {
    return bytesWritten;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void cleanup() throws IOException {
    client.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    cleanup();
    super.close();
  }
}
