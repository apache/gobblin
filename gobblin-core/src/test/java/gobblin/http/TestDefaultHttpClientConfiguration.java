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

import org.apache.http.HttpHost;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;

/**
 * Unit tests for {@link DefaultHttpClientConfigurator}
 */
public class TestDefaultHttpClientConfiguration {

  @Test
  public void testConfigureFromTypesafe() {
    {
      Config cfg =
        ConfigFactory.empty().withValue(DefaultHttpClientConfigurator.PROXY_HOSTPORT_KEY,
                                        ConfigValueFactory.fromAnyRef("localhost:12345"));
      Optional<HttpHost> proxyHost = DefaultHttpClientConfigurator.getProxyAddr(cfg);
      Assert.assertTrue(proxyHost.isPresent());
      Assert.assertEquals(proxyHost.get(), new HttpHost("localhost", 12345));
    }
    {
      Config cfg =
        ConfigFactory.empty().withValue(DefaultHttpClientConfigurator.PROXY_HOSTPORT_KEY,
                                        ConfigValueFactory.fromAnyRef("localhost"));
      Optional<HttpHost> proxyHost = DefaultHttpClientConfigurator.getProxyAddr(cfg);
      Assert.assertTrue(proxyHost.isPresent());
      Assert.assertEquals(proxyHost.get(), new HttpHost("localhost",
          DefaultHttpClientConfigurator.DEFAULT_HTTP_PROXY_PORT));
    }
    {
      Config cfg =
        ConfigFactory.empty().withValue(DefaultHttpClientConfigurator.PROXY_URL_KEY,
                                        ConfigValueFactory.fromAnyRef("host123"));
      Optional<HttpHost> proxyHost = DefaultHttpClientConfigurator.getProxyAddr(cfg);
      Assert.assertTrue(proxyHost.isPresent());
      Assert.assertEquals(proxyHost.get(), new HttpHost("host123",
          DefaultHttpClientConfigurator.DEFAULT_HTTP_PROXY_PORT));
    }
    {
      Config cfg =
        ConfigFactory.empty().withValue(DefaultHttpClientConfigurator.PROXY_URL_KEY,
                                        ConfigValueFactory.fromAnyRef("host123"))
                              .withValue(DefaultHttpClientConfigurator.PROXY_PORT_KEY,
                                        ConfigValueFactory.fromAnyRef(54321));
      Optional<HttpHost> proxyHost = DefaultHttpClientConfigurator.getProxyAddr(cfg);
      Assert.assertTrue(proxyHost.isPresent());
      Assert.assertEquals(proxyHost.get(), new HttpHost("host123",54321));
    }
    {
      Config cfg =
        ConfigFactory.empty();
      Optional<HttpHost> proxyHost = DefaultHttpClientConfigurator.getProxyAddr(cfg);
      Assert.assertFalse(proxyHost.isPresent());
    }
  }

  @Test
  public void testConfigureFromState() {
    {
      State state = new State();
      state.setProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL, "localhost");
      state.setProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_PORT, "11111");
      DefaultHttpClientConfigurator configurator = new DefaultHttpClientConfigurator();
      Config cfg = configurator.setStatePropertiesPrefix("source.conn.").stateToConfig(state);
      Assert.assertEquals(cfg.getString(DefaultHttpClientConfigurator.PROXY_URL_KEY), "localhost");
      Assert.assertEquals(cfg.getInt(DefaultHttpClientConfigurator.PROXY_PORT_KEY), 11111);
    }
    {
      State state = new State();
      state.setProp(ConfigurationKeys.SOURCE_CONN_USE_PROXY_URL, "localhost");
      DefaultHttpClientConfigurator configurator = new DefaultHttpClientConfigurator();
      Config cfg = configurator.setStatePropertiesPrefix("source.conn.").stateToConfig(state);
      Assert.assertEquals(cfg.getString(DefaultHttpClientConfigurator.PROXY_URL_KEY), "localhost");
      Assert.assertFalse(cfg.hasPath(DefaultHttpClientConfigurator.PROXY_PORT_KEY));
    }
    {
      State state = new State();
      state.setProp(DefaultHttpClientConfigurator.PROXY_HOSTPORT_KEY, "localhost:22222");
      DefaultHttpClientConfigurator configurator = new DefaultHttpClientConfigurator();
      Config cfg = configurator.stateToConfig(state);
      Assert.assertEquals(cfg.getString(DefaultHttpClientConfigurator.PROXY_HOSTPORT_KEY),
                          "localhost:22222");
    }
  }

}
