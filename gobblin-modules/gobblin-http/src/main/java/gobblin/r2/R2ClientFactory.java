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
import java.util.concurrent.ExecutionException;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.*;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.common.bridge.client.TransportClient;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import gobblin.security.ssl.SSLContextFactory;


/**
 * Create a corresponding {@link Client} based on different {@link Schema}
 */
public class R2ClientFactory {
  public static final String SSL_ENABLED = "ssl";
  public static final String ZOOKEEPER_HOSTS = "zkHosts";

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
   *   A sample configuration for https based client is
   *   <br> ssl=true
   *   <br> keyStoreFilePath=/path/to/key/store
   *   <br> keyStorePassword=password
   *   <br> keyStoreType=PKCS12
   *   <br> trustStoreFilePath=/path/to/trust/store
   *   <br> trustStorePassword=password
   * </p>
   *
   * <p>
   *   A sample configuration for a secured d2 client is
   *   <br> d2.zkHosts=zk1.host.com:12000
   *   <br> d2.ssl=true
   *   <br> d2.keyStoreFilePath=/path/to/key/store
   *   <br> d2.keyStorePassword=password
   *   <br> d2.keyStoreType=PKCS12
   *   <br> d2.trustStoreFilePath=/path/to/trust/store
   *   <br> d2.trustStorePassword=password
   * </p>
   *
   * @param srcConfig configuration
   * @return an instance of {@link Client}
   */
  public Client createInstance(Config srcConfig) {
    switch (schema) {
      case HTTP:
        return createHttpClient(srcConfig);
      case D2:
        String confPrefix = schema.name().toLowerCase();
        if (srcConfig.hasPath(confPrefix)) {
          Config config = srcConfig.getConfig(confPrefix);
          return createD2Client(config);
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

    TransportClient client = new HttpClientFactory().getClient(properties);
    return new TransportClientAdapter(client);
  }

  private Client createD2Client(Config config) {
    String zkhosts = config.getString(ZOOKEEPER_HOSTS);
    if (zkhosts == null || zkhosts.length() == 0) {
      throw new ConfigException.Missing(ZOOKEEPER_HOSTS);
    }

    D2ClientBuilder d2Builder = new D2ClientBuilder().setZkHosts(zkhosts);

    Map<String, TransportClientFactory> clientFactories = createTransportClientFactories();
    d2Builder.setClientFactories(clientFactories);

    boolean isSSLEnabled = config.getBoolean(SSL_ENABLED);
    if (isSSLEnabled) {
      d2Builder.setIsSSLEnabled(true);
      SSLContext sslContext = SSLContextFactory.createInstance(config);
      d2Builder.setSSLContext(sslContext);
      d2Builder.setSSLParameters(sslContext.getDefaultSSLParameters());
    }

    com.linkedin.d2.balancer.D2Client d2 = d2Builder.build();

    final SettableFuture<None> d2ClientFuture = SettableFuture.create();
    d2.start(new Callback<None>() {
      @Override
      public void onError(Throwable e) {
        d2ClientFuture.setException(e);
      }
      @Override
      public void onSuccess(None none) {
        d2ClientFuture.set(none);
      }
    });

    try {
      // Synchronously wait for d2 to start
      d2ClientFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    return new D2ClientProxy(d2, clientFactories.values());
  }

  private static Map<String, TransportClientFactory> createTransportClientFactories() {
    return ImmutableMap.<String, TransportClientFactory>builder()
        .put("http", new HttpClientFactory())
        //It won't route to SSL port without this.
        .put("https", new HttpClientFactory())
        .build();
  }
}
