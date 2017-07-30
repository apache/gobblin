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

package gobblin.r2;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.linkedin.d2.balancer.*;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import gobblin.security.ssl.SSLContextFactory;


/**
 * Create a corresponding {@link Client} based on different {@link Schema}
 */
public class R2ClientFactory {
  public static final String SSL_ENABLED = "ssl";
  public static final String PROPERTIES = "properties";
  public static final String CLIENT_SERVICES_CONFIG = "clientServicesConfig";
  private static final String ZOOKEEPER_HOSTS = "zkHosts";

  private static final Config FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(SSL_ENABLED, false)
          .put("d2.ssl", false)
          .build());

  public enum Schema {
    HTTP,
    D2
  }

  private Schema schema;

  public R2ClientFactory(Schema schema) {
    this.schema = schema;
  }

  /**
   * Given a {@link Config}, create an instance of {@link Client}
   *
   * <p>
   *   A sample configuration for https based client is:
   *   <br> ssl=true
   *   <br> keyStoreFilePath=/path/to/key/store
   *   <br> keyStorePassword=password
   *   <br> keyStoreType=PKCS12
   *   <br> trustStoreFilePath=/path/to/trust/store
   *   <br> trustStorePassword=password
   *
   *   <p>
   *     Http configurations(see {@link HttpClientFactory}) like http.responseMaxSize, http.idleTimeout, etc, can
   *     be set as:
   *     <br> properties.http.responseMaxSize=10000
   *     <br> properties.http.idleTimeout=3000
   *   </p>
   * </p>
   *
   * <p>
   *   A sample configuration for a secured d2 client is:
   *   <br> d2.zkHosts=zk1.host.com:12000
   *   <br> d2.ssl=true
   *   <br> d2.keyStoreFilePath=/path/to/key/store
   *   <br> d2.keyStorePassword=password
   *   <br> d2.keyStoreType=PKCS12
   *   <br> d2.trustStoreFilePath=/path/to/trust/store
   *   <br> d2.trustStorePassword=password
   *
   *   <p>
   *     Http configurations(see {@link HttpClientFactory}) like http.responseMaxSize, http.idleTimeout, etc, can
   *     be set as:
   *     <br> d2.clientServicesConfig.[client_name].http.responseMaxSize=10000
   *     <br> d2.clientServicesConfig.[client_name].http.idleTimeout=3000
   *   </p>
   * </p>
   *
   * @param srcConfig configuration
   * @return an instance of {@link Client}
   */
  public Client createInstance(Config srcConfig) {
    Config config = srcConfig.withFallback(FALLBACK);
    switch (schema) {
      case HTTP:
        return createHttpClient(config);
      case D2:
        String confPrefix = schema.name().toLowerCase();
        if (config.hasPath(confPrefix)) {
          Config d2Config = config.getConfig(confPrefix);
          return createD2Client(d2Config);
        } else {
          throw new ConfigException.Missing(confPrefix);
        }
      default:
        throw new RuntimeException("Schema not supported: " + schema.name());
    }
  }

  private Client createHttpClient(Config config) {
    boolean isSSLEnabled = config.getBoolean(SSL_ENABLED);
    SSLContext sslContext = null;
    SSLParameters sslParameters = null;

    if (isSSLEnabled) {
      sslContext = SSLContextFactory.createInstance(config);
      sslParameters = sslContext.getDefaultSSLParameters();
    }
    Map<String, Object> properties = new HashMap<>();
    properties.put(HttpClientFactory.HTTP_SSL_CONTEXT, sslContext);
    properties.put(HttpClientFactory.HTTP_SSL_PARAMS, sslParameters);

    if (config.hasPath(PROPERTIES)) {
      properties.putAll(toMap(config.getConfig(PROPERTIES)));
    }

    return new R2HttpClientProxy(new HttpClientFactory(), properties);
  }

  private Client createD2Client(Config config) {
    String zkhosts = config.getString(ZOOKEEPER_HOSTS);
    if (zkhosts == null || zkhosts.length() == 0) {
      throw new ConfigException.Missing(ZOOKEEPER_HOSTS);
    }

    D2ClientBuilder d2Builder = new D2ClientBuilder().setZkHosts(zkhosts);

    boolean isSSLEnabled = config.getBoolean(SSL_ENABLED);
    if (isSSLEnabled) {
      d2Builder.setIsSSLEnabled(true);
      SSLContext sslContext = SSLContextFactory.createInstance(config);
      d2Builder.setSSLContext(sslContext);
      d2Builder.setSSLParameters(sslContext.getDefaultSSLParameters());
    }

    if (config.hasPath(CLIENT_SERVICES_CONFIG)) {
      Config clientServiceConfig = config.getConfig(CLIENT_SERVICES_CONFIG);
      Map<String, Map<String, Object>> result = new HashMap<>();
      for (String key: clientServiceConfig.root().keySet()) {
        Config value = clientServiceConfig.getConfig(key);
        result.put(key, toMap(value));
      }
      d2Builder.setClientServicesConfig(result);
    }

    return new D2ClientProxy(d2Builder, isSSLEnabled);
  }

  private static Map<String, Object> toMap(Config config) {
    Map<String, Object> map = new HashMap<>();
    for (Map.Entry<String, ConfigValue> entry: config.entrySet()) {
      map.put(entry.getKey(), entry.getValue().unwrapped());
    }
    return map;
  }
}
