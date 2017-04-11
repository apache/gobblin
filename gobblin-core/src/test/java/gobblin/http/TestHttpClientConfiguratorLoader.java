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

import org.testng.Assert;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import gobblin.configuration.State;

/**
 * Unit tests for {@link HttpClientConfiguratorLoader}
 */
public class TestHttpClientConfiguratorLoader {

  @Test
  public void testConfigureFromState() {
    HttpClientConfiguratorLoader loader = new HttpClientConfiguratorLoader(new State());
    Assert.assertEquals(loader.getConfigurator().getClass(), DefaultHttpClientConfigurator.class);

    State state = new State();
    state.setProp(HttpClientConfiguratorLoader.HTTP_CLIENT_CONFIGURATOR_TYPE_FULL_KEY, "default");
    loader = new HttpClientConfiguratorLoader(state);
    Assert.assertEquals(loader.getConfigurator().getClass(), DefaultHttpClientConfigurator.class);
  }

  @Test
  public void testConfigureFromConfig() {
    final Config config = ConfigFactory.empty()
        .withValue(HttpClientConfiguratorLoader.HTTP_CLIENT_CONFIGURATOR_TYPE_KEY,
                   ConfigValueFactory.fromAnyRef("blah"));
    Assert.assertThrows(new ThrowingRunnable() {
      @Override public void run() throws Throwable {
        new HttpClientConfiguratorLoader(config);
      }
    });
  }

}
