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

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import lombok.Getter;

import gobblin.config.ConfigBuilder;
import gobblin.configuration.State;
import gobblin.http.HttpClient;
import gobblin.http.ResponseHandler;
import gobblin.writer.Destination;
import gobblin.writer.FluentDataWriterBuilder;


/**
 * Base builder for http writers
 *
 * @param <D> type of record
 * @param <RQ> type of request
 * @param <RP> type of response
 */
public abstract class HttpWriterBaseBuilder<D, RQ, RP> extends FluentDataWriterBuilder<Void, D, HttpWriterBaseBuilder<D, RQ, RP>> {
  public static final String CONF_PREFIX = "gobblin.writer.http.";

  @Getter
  private State state;
  @Getter
  protected HttpClient<RQ, RP> client = null;
  @Getter
  protected WriteRequestBuilder<D, RQ> requestBuilder = null;
  @Getter
  protected ResponseHandler<RP> responseHandler = null;

  /**
   * For backward compatibility on how Fork creates writer, invoke fromState when it's called writeTo method.
   * @param destination
   * @return this
   */
  @Override
  public HttpWriterBaseBuilder<D, RQ, RP> writeTo(Destination destination) {
    super.writeTo(destination);
    return fromState(destination.getProperties());
  }

  HttpWriterBaseBuilder<D, RQ, RP> fromState(State state) {
    this.state = state;
    Config config = ConfigBuilder.create().loadProps(state.getProperties(), CONF_PREFIX).build();
    return fromConfig(config);
  }

  public abstract HttpWriterBaseBuilder<D, RQ, RP> fromConfig(Config config);

  protected void validate() {
    Preconditions.checkNotNull(getState(), "State is required for " + this.getClass().getSimpleName());
    Preconditions.checkNotNull(getClient(), "Client is required for " + this.getClass().getSimpleName());
    Preconditions
        .checkNotNull(getRequestBuilder(), "WriteRequestBuilder is required for " + this.getClass().getSimpleName());
    Preconditions
        .checkNotNull(getResponseHandler(), "ResponseHandler is required for " + this.getClass().getSimpleName());
  }
}
