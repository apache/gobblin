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

import org.apache.http.client.HttpClient;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.configuration.State;
import gobblin.util.ClassAliasResolver;

/**
 * Creates an instance of HttpClientConfigurator using dependency injection from configuration.
 */
public class HttpClientConfiguratorLoader {

  /** Classname or alias for an {@link HttpClientConfigurator} instance to use for configuring and
   * instantiating of {@link HttpClient} instances. */
  public static final String HTTP_CLIENT_CONFIGURATOR_TYPE_KEY = "httpClientConfigurator.type";
  public static final String HTTP_CLIENT_CONFIGURATOR_TYPE_FULL_KEY =
      "gobblin." + HTTP_CLIENT_CONFIGURATOR_TYPE_KEY;
  public static final Class<? extends HttpClientConfigurator> DEFAULT_CONFIGURATOR_CLASS =
      DefaultHttpClientConfigurator.class;

  private static final ClassAliasResolver<HttpClientConfigurator> TYPE_RESOLVER =
      new ClassAliasResolver<>(HttpClientConfigurator.class);
  private final HttpClientConfigurator _configurator;

  /**
   * Loads a HttpClientConfigurator using the value of the {@link #HTTP_CLIENT_CONFIGURATOR_TYPE_FULL_KEY}
   * property in the state.
   */
  public HttpClientConfiguratorLoader(State state) {
    this(Optional.<String>fromNullable(state.getProp(HTTP_CLIENT_CONFIGURATOR_TYPE_FULL_KEY)));
  }

  /** Loads a HttpClientConfigurator using the value of {@link #HTTP_CLIENT_CONFIGURATOR_TYPE_KEY}
   * in the local typesafe config. */
  public HttpClientConfiguratorLoader(Config config) {
    this(Optional.<String>fromNullable(config.hasPath(HTTP_CLIENT_CONFIGURATOR_TYPE_KEY) ?
              config.getString(HTTP_CLIENT_CONFIGURATOR_TYPE_KEY) : null));
  }

  /** Loads a HttpClientConfigurator with the specified class or alias. If not specified,
   * {@link #DEFAULT_CONFIGURATOR_CLASS} is used. */
  public HttpClientConfiguratorLoader(Optional<String> configuratorType) {
    try {
      _configurator = getConfiguratorClass(configuratorType).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException("Unable to find HttpClientConfigurator:" + e, e);
    }
  }

  private static Class<? extends HttpClientConfigurator>
          getConfiguratorClass(Optional<String> configuratorType) throws ClassNotFoundException {
    return configuratorType.isPresent() ? TYPE_RESOLVER.resolveClass(configuratorType.get()) :
        DEFAULT_CONFIGURATOR_CLASS;
  }

  public HttpClientConfigurator getConfigurator() {
    return _configurator;
  }

}
