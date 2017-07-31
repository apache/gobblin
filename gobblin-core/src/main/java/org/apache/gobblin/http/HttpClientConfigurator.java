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
package org.apache.gobblin.http;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import com.typesafe.config.Config;

import org.apache.gobblin.configuration.State;

/**
 * An adapter from Gobblin configuration to {@link HttpClientBuilder}. It can also be used to
 * create {@link HttpClient} instances.
 */
public interface HttpClientConfigurator {

  /** Sets a prefix to use when extracting the configuration from {@link State}. The default is
   * empty. */
  HttpClientConfigurator setStatePropertiesPrefix(String propertiesPrefix);

  /**
   * Extracts the HttpClient configuration from a typesafe config. Supported configuration options
   * may vary from implementation to implementation.
   * */
  HttpClientConfigurator configure(Config httpClientConfig);

  /** Same as {@link #configure(Config)} but for legacy cases using State. */
  HttpClientConfigurator configure(State httpClientConfig);

  /** The underlying client builder */
  HttpClientBuilder getBuilder();

  /**
   * Typically this will use {@link HttpClientBuilder#build()} based on the configuration but
   * implementations may also return decorated instances. */
  HttpClient createClient();

}
