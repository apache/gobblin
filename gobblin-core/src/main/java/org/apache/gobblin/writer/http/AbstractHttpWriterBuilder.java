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
package org.apache.gobblin.writer.http;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.http.HttpClientConfiguratorLoader;
import org.apache.gobblin.writer.Destination;
import org.apache.gobblin.writer.FluentDataWriterBuilder;

import lombok.Getter;

@Getter
public abstract class AbstractHttpWriterBuilder<S, D, B extends AbstractHttpWriterBuilder<S, D, B>>
    extends FluentDataWriterBuilder<S, D, B> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHttpWriterBuilder.class);

  public static final String CONF_PREFIX = "gobblin.writer.http.";
  public static final String HTTP_CONN_MANAGER = "conn_mgr_type";
  public static final String POOLING_CONN_MANAGER_MAX_TOTAL_CONN = "conn_mgr.pooling.max_conn_total";
  public static final String POOLING_CONN_MANAGER_MAX_PER_CONN = "conn_mgr.pooling.max_per_conn";
  public static final String REQUEST_TIME_OUT_MS_KEY = "req_time_out";
  public static final String CONNECTION_TIME_OUT_MS_KEY = "conn_time_out";
  public static final String STATIC_SVC_ENDPOINT = "static_svc_endpoint";

  public static enum ConnManager {
    POOLING,
    BASIC;
  }

  private static final Config FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(REQUEST_TIME_OUT_MS_KEY, TimeUnit.SECONDS.toMillis(5L))
          .put(CONNECTION_TIME_OUT_MS_KEY, TimeUnit.SECONDS.toMillis(5L))
          .put(HTTP_CONN_MANAGER, ConnManager.BASIC.name())
          .put(POOLING_CONN_MANAGER_MAX_TOTAL_CONN, 20)
          .put(POOLING_CONN_MANAGER_MAX_PER_CONN, 2)
          .build());

  private State state = new State();
  private Optional<HttpClientBuilder> httpClientBuilder = Optional.absent();

  private HttpClientConnectionManager httpConnManager;
  private long reqTimeOut;
  private Optional<Logger> logger = Optional.absent();
  private Optional<URI> svcEndpoint = Optional.absent();

  /**
   * For backward compatibility on how Fork creates writer, invoke fromState when it's called writeTo method.
   * @param destination
   * @return
   */
  @Override
  public B writeTo(Destination destination) {
    super.writeTo(destination);
    fromState(destination.getProperties());
    return typedSelf();
  }

  public B fromState(State state) {
    this.state = state;
    Config config = ConfigBuilder.create().loadProps(state.getProperties(), CONF_PREFIX).build();
    fromConfig(config);
    return typedSelf();
  }

  public B fromConfig(Config config) {
    config = config.withFallback(FALLBACK);
    RequestConfig requestConfig = RequestConfig.copy(RequestConfig.DEFAULT)
                                               .setSocketTimeout(config.getInt(REQUEST_TIME_OUT_MS_KEY))
                                               .setConnectTimeout(config.getInt(CONNECTION_TIME_OUT_MS_KEY))
                                               .setConnectionRequestTimeout(config.getInt(CONNECTION_TIME_OUT_MS_KEY))
                                               .build();

    getHttpClientBuilder().setDefaultRequestConfig(requestConfig);

    if (config.hasPath(STATIC_SVC_ENDPOINT)) {
      try {
        svcEndpoint = Optional.of(new URI(config.getString(AbstractHttpWriterBuilder.STATIC_SVC_ENDPOINT)));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    String connMgrStr = config.getString(HTTP_CONN_MANAGER);
    switch (ConnManager.valueOf(connMgrStr.toUpperCase())) {
      case BASIC:
        httpConnManager = new BasicHttpClientConnectionManager();
        break;
      case POOLING:
        PoolingHttpClientConnectionManager poolingConnMgr = new PoolingHttpClientConnectionManager();
        poolingConnMgr.setMaxTotal(config.getInt(POOLING_CONN_MANAGER_MAX_TOTAL_CONN));
        poolingConnMgr.setDefaultMaxPerRoute(config.getInt(POOLING_CONN_MANAGER_MAX_PER_CONN));
        httpConnManager = poolingConnMgr;
        break;
      default:
        throw new IllegalArgumentException(connMgrStr + " is not supported");
    }
    LOG.info("Using " + httpConnManager.getClass().getSimpleName());
    return typedSelf();
  }

  public HttpClientBuilder getDefaultHttpClientBuilder() {
    HttpClientConfiguratorLoader clientConfiguratorLoader =
        new HttpClientConfiguratorLoader(getState());
    clientConfiguratorLoader.getConfigurator().setStatePropertiesPrefix(AbstractHttpWriterBuilder.CONF_PREFIX);
    return clientConfiguratorLoader.getConfigurator().configure(getState())
        .getBuilder().disableCookieManagement().useSystemProperties();
  }

  public HttpClientBuilder getHttpClientBuilder() {
    if (!this.httpClientBuilder.isPresent()) {
      this.httpClientBuilder = Optional.of(getDefaultHttpClientBuilder());
    }

    return this.httpClientBuilder.get();
  }

  public B withHttpClientBuilder(HttpClientBuilder builder) {
    this.httpClientBuilder = Optional.of(builder);
    return typedSelf();
  }

  public B withHttpClientConnectionManager(HttpClientConnectionManager connManager) {
    this.httpConnManager = connManager;
    return typedSelf();
  }

  public B withLogger(Logger logger) {
    this.logger = Optional.fromNullable(logger);
    return typedSelf();
  }

  void validate() {
    Preconditions.checkNotNull(getState(), "State is required for " + this.getClass().getSimpleName());
    Preconditions.checkNotNull(getHttpClientBuilder(), "HttpClientBuilder is required for " + this.getClass().getSimpleName());
    Preconditions.checkNotNull(getHttpConnManager(), "HttpConnManager is required for " + this.getClass().getSimpleName());
  }
}
