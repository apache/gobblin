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
import gobblin.http.BatchRequestBuilder;
import gobblin.http.HttpClient;
import gobblin.http.RequestBuilder;
import gobblin.http.ResponseHandler;
import gobblin.writer.Destination;
import gobblin.writer.FluentDataWriterBuilder;


/**
 * Base builder for http writers
 *
 * @param <D> type of record
 * @param <RQ> type of request
 * @param <RP> type of response
 * @param <B> type of builder
 */
@Getter
public abstract class HttpWriterBaseBuilder<D, RQ, RP, B extends HttpWriterBaseBuilder<D, RQ, RP, B>>
    extends FluentDataWriterBuilder<Void, D, B> {
  public static final String CONF_PREFIX = "gobblin.writer.http.";

  private State state;
  protected HttpClient<RQ, RP> client;
  protected RequestBuilder<D, RQ> requestBuilder;
  protected BatchRequestBuilder<D, RQ> batchRequestBuilder;
  protected ResponseHandler<RP> responseHandler;

  /**
   * For backward compatibility on how Fork creates writer, invoke fromState when it's called writeTo method.
   * @param destination
   * @return
   */
  @Override
  public B writeTo(Destination destination) {
    super.writeTo(destination);
    return fromState(destination.getProperties());
  }

  B fromState(State state) {
    this.state = state;
    Config config = ConfigBuilder.create().loadProps(state.getProperties(), CONF_PREFIX).build();
    return fromConfig(config);
  }

  B fromConfig(Config config) {
    createComponents(getState(), config);
    return typedSelf();
  }

  protected abstract void createComponents(State state, Config config);

  protected void validate() {
    Preconditions.checkNotNull(getState(), "State is required for " + this.getClass().getSimpleName());
    Preconditions
        .checkNotNull(getClient(), "Client is required for " + this.getClass().getSimpleName());
    Preconditions.checkArgument(getRequestBuilder() != null || getBatchRequestBuilder() != null,
        "Must provide either a RequestBuilder or a BatchRequestBuilder for " + this.getClass().getSimpleName());
    Preconditions
        .checkNotNull(getResponseHandler(), "ResponseHandler is required for " + this.getClass().getSimpleName());
  }
}
