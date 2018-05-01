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

package org.apache.gobblin.writer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.gobblin.async.AsyncRequestBuilder;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.http.HttpClient;
import org.apache.gobblin.http.ResponseHandler;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.utils.HttpConstants;
import java.io.IOException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Base builder for async http writers
 *
 * @param <D> type of record
 * @param <RQ> type of request
 * @param <RP> type of response
 */
@Slf4j
public abstract class AsyncHttpWriterBuilder<D, RQ, RP> extends FluentDataWriterBuilder<Void, D, AsyncHttpWriterBuilder<D, RQ, RP>> {
  public static final String CONF_PREFIX = "gobblin.writer.http.";

  private static final String MAX_OUTSTANDING_WRITES = "maxOutstandingWrites";
  private static final String MAX_ATTEMPTS = "maxAttempts";

  private static final Config FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(HttpConstants.ERROR_CODE_WHITELIST, "")
          .put(MAX_OUTSTANDING_WRITES, AsyncWriterManager.MAX_OUTSTANDING_WRITES_DEFAULT)
          .put(MAX_ATTEMPTS, AsyncHttpWriter.DEFAULT_MAX_ATTEMPTS)
          .build());

  @Getter
  MetricContext metricContext;
  @Getter
  protected WorkUnitState state;
  @Getter
  protected HttpClient<RQ, RP> client = null;
  @Getter
  protected AsyncRequestBuilder<D, RQ> asyncRequestBuilder = null;
  @Getter
  protected ResponseHandler<RQ, RP> responseHandler = null;
  @Getter
  protected int queueCapacity = AbstractAsyncDataWriter.DEFAULT_BUFFER_CAPACITY;
  @Getter
  protected SharedResourcesBroker<GobblinScopeTypes> broker = null;
  @Getter
  protected int maxAttempts;
  private int maxOutstandingWrites;

  /**
   * For backward compatibility on how Fork creates writer, invoke fromState when it's called writeTo method.
   * @param destination
   * @return this
   */
  @Override
  public AsyncHttpWriterBuilder<D, RQ, RP> writeTo(Destination destination) {
    super.writeTo(destination);
    return fromState(destination.getProperties());
  }

  AsyncHttpWriterBuilder<D, RQ, RP> fromState(State state) {
    if (!(state instanceof WorkUnitState)) {
      throw new IllegalStateException(String.format("AsyncHttpWriterBuilder requires a %s on construction.", WorkUnitState.class.getSimpleName()));
    }

    this.state = (WorkUnitState) state;
    this.metricContext = Instrumented.getMetricContext(this.state, AsyncHttpWriter.class);
    this.broker = this.state.getTaskBroker();
    Config config = ConfigBuilder.create().loadProps(state.getProperties(), CONF_PREFIX).build();
    config = config.withFallback(FALLBACK);
    this.maxOutstandingWrites = config.getInt(MAX_OUTSTANDING_WRITES);
    this.maxAttempts = config.getInt(MAX_ATTEMPTS);
    return fromConfig(config);
  }

  public abstract AsyncHttpWriterBuilder<D, RQ, RP> fromConfig(Config config);

  protected void validate() {
    Preconditions.checkNotNull(getState(), "State is required for " + this.getClass().getSimpleName());
    Preconditions.checkNotNull(getClient(), "Client is required for " + this.getClass().getSimpleName());
    Preconditions.checkNotNull(getAsyncRequestBuilder(),
        "AsyncWriteRequestBuilder is required for " + this.getClass().getSimpleName());
    Preconditions
        .checkNotNull(getResponseHandler(), "ResponseHandler is required for " + this.getClass().getSimpleName());
  }

  @Override
  public DataWriter<D> build()
      throws IOException {
    validate();
    return AsyncWriterManager.builder()
        .config(ConfigUtils.propertiesToConfig(getState().getProperties()))
        .asyncDataWriter(new AsyncHttpWriter(this))
        .maxOutstandingWrites(maxOutstandingWrites)
        .retriesEnabled(false) // retries are done in HttpBatchDispatcher
        .commitTimeoutMillis(10000L)
        .failureAllowanceRatio(0).build();
  }
}
