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
package gobblin.http;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import gobblin.annotation.Alias;
import gobblin.configuration.State;

/**
 * Default implementation that uses the following properties to configure an {@link HttpClient}.
 *
 * <ul>
 *  <li>{@link #PROXY_HOSTPORT_KEY}
 *  <li>{@link #PROXY_URL_KEY}
 *  <li>{@link #PROXY_PORT_KEY}
 * </ul>
 */
@Alias(value="default")
public class DefaultHttpClientConfigurator implements HttpClientConfigurator {
  // IMPORTANT: don't change the values for PROXY_URL_KEY and PROXY_PORT_KEY as they are meant to
  // be backwards compatible with SOURCE_CONN_USE_PROXY_URL and SOURCE_CONN_USE_PROXY_PORT when
  // the statePropertiesPrefix is "source.conn."
  /** The hostname of the HTTP proxy to use */
  public static final String PROXY_URL_KEY = "use.proxy.url";
  /** The port of the HTTP proxy to use */
  public static final String PROXY_PORT_KEY = "use.proxy.port";
  /** Similar to {@link #PROXY_URL_KEY} and {@link #PROXY_PORT_KEY} but allows you to set it on
   * one property as <host>:<port> . This property takes precedence over those properties.  */
  public static final String PROXY_HOSTPORT_KEY = "proxyHostport";
  /** Port to use if the HTTP Proxy is enabled but no port is specified */
  public static final int DEFAULT_HTTP_PROXY_PORT = 8080;

  private static final Pattern HOSTPORT_PATTERN = Pattern.compile("([^:]+)(:([0-9]+))?");

  protected final HttpClientBuilder _builder = HttpClientBuilder.create();
  protected String _statePropertiesPrefix = null;

  /** {@inheritDoc} */
  @Override
  public DefaultHttpClientConfigurator configure(Config httpClientConfig) {
    Optional<HttpHost> proxy = getProxyAddr(httpClientConfig);
    if (proxy.isPresent()) {
      getBuilder().setProxy(proxy.get());
    }
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public DefaultHttpClientConfigurator configure(State state) {
    Config cfg = stateToConfig(state);
    return configure(cfg);
  }

  protected Config stateToConfig(State state) {
    String proxyUrlKey = getPrefixedPropertyName(PROXY_URL_KEY);
    String proxyPortKey = getPrefixedPropertyName(PROXY_PORT_KEY);
    String proxyHostportKey = getPrefixedPropertyName(PROXY_HOSTPORT_KEY);

    Config cfg = ConfigFactory.empty();
    if (state.contains(proxyUrlKey)) {
      cfg = cfg.withValue(PROXY_URL_KEY, ConfigValueFactory.fromAnyRef(state.getProp(proxyUrlKey)));
    }
    if (state.contains(proxyPortKey)) {
      cfg = cfg.withValue(PROXY_PORT_KEY, ConfigValueFactory.fromAnyRef(state.getPropAsInt(proxyPortKey)));
    }
    if (state.contains(proxyHostportKey)) {
      cfg = cfg.withValue(PROXY_HOSTPORT_KEY, ConfigValueFactory.fromAnyRef(state.getProp(proxyHostportKey)));
    }
    return cfg;
  }

  /** {@inheritDoc} */
  @Override
  public CloseableHttpClient createClient() {
    return _builder.build();
  }

  @VisibleForTesting
  public static Optional<HttpHost> getProxyAddr(Config httpClientConfig) {
    String proxyHost = null;
    int proxyPort = DEFAULT_HTTP_PROXY_PORT;
    if (httpClientConfig.hasPath(PROXY_URL_KEY) &&
        !httpClientConfig.getString(PROXY_URL_KEY).isEmpty()) {
      proxyHost = httpClientConfig.getString(PROXY_URL_KEY);
    }
    if (httpClientConfig.hasPath(PROXY_PORT_KEY)) {
      proxyPort = httpClientConfig.getInt(PROXY_PORT_KEY);
    }
    if (httpClientConfig.hasPath(PROXY_HOSTPORT_KEY)) {
      String hostport = httpClientConfig.getString(PROXY_HOSTPORT_KEY);
      Matcher hostportMatcher = HOSTPORT_PATTERN.matcher(hostport);
      if (!hostportMatcher.matches()) {
        throw new IllegalArgumentException("Invalid HTTP proxy hostport: " + hostport);
      }
      proxyHost = hostportMatcher.group(1);
      if (!Strings.isNullOrEmpty(hostportMatcher.group(3))) {
        proxyPort = Integer.parseInt(hostportMatcher.group(3));
      }
    }
    return null != proxyHost ? Optional.of(new HttpHost(proxyHost, proxyPort))
        : Optional.<HttpHost>absent();
  }

  @Override
  public DefaultHttpClientConfigurator setStatePropertiesPrefix(String propertiesPrefix) {
    _statePropertiesPrefix = propertiesPrefix;
    return this;
  }

  String getPrefixedPropertyName(String propertyName) {
    return null != _statePropertiesPrefix ? _statePropertiesPrefix + propertyName : propertyName;
  }

  @Override
  public HttpClientBuilder getBuilder() {
    return _builder;
  }

}
