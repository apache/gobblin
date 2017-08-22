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
package org.apache.gobblin.r2;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.curator.test.TestingServer;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.SettableFuture;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.r2.transport.common.Client;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import junit.framework.Assert;

import org.apache.gobblin.security.ssl.SSLContextFactory;


@Test
public class R2ClientFactoryTest {

  public void testHttpClient() {
    R2ClientFactory factory = new R2ClientFactory(R2ClientFactory.Schema.HTTP);

    Map<String, Object> values = new HashMap<>();
    // No SSL
    Client client = factory.createInstance(ConfigFactory.parseMap(values));
    shutdown(client);

    // With SSL
    values.put(R2ClientFactory.SSL_ENABLED, true);
    values.put(SSLContextFactory.KEY_STORE_FILE_PATH, "identity.p12");
    values.put(SSLContextFactory.TRUST_STORE_FILE_PATH, "certs");
    values.put(SSLContextFactory.KEY_STORE_PASSWORD, "keyStorePassword");
    values.put(SSLContextFactory.TRUST_STORE_PASSWORD, "trustStorePassword");
    values.put(SSLContextFactory.KEY_STORE_TYPE, "PKCS12");

    try {
      factory.createInstance(ConfigFactory.parseMap(values));
    } catch (ConfigException | IllegalArgumentException e) {
      Assert.fail();
    } catch (Exception e) {
      // OK
    }
  }

  public void testD2Client()
      throws Exception {
    R2ClientFactory factory = new R2ClientFactory(R2ClientFactory.Schema.D2);
    TestingServer zkServer = new TestingServer(-1);

    Map<String, Object> values = new HashMap<>();
    values.put("d2.zkHosts", zkServer.getConnectString());
    // No SSL
    Client client = factory.createInstance(ConfigFactory.parseMap(values));
    shutdown(client);

    // With SSL
    final String confPrefix = "d2.";
    values.put(confPrefix + R2ClientFactory.SSL_ENABLED, true);
    values.put(confPrefix + SSLContextFactory.KEY_STORE_FILE_PATH, "identity.p12");
    values.put(confPrefix + SSLContextFactory.TRUST_STORE_FILE_PATH, "certs");
    values.put(confPrefix + SSLContextFactory.KEY_STORE_PASSWORD, "keyStorePassword");
    values.put(confPrefix + SSLContextFactory.TRUST_STORE_PASSWORD, "trustStorePassword");
    values.put(confPrefix + SSLContextFactory.KEY_STORE_TYPE, "PKCS12");

    try {
      factory.createInstance(ConfigFactory.parseMap(values));
    } catch (ConfigException | IllegalArgumentException e) {
      Assert.fail("Unexpected config exception");
    } catch (Exception e) {
      // OK
    }

    zkServer.close();
  }

  private void shutdown(Client client) {
    final SettableFuture<None> future = SettableFuture.create();
    client.shutdown(new Callback<None>() {
      @Override
      public void onError(Throwable e) {
        future.setException(e);
      }

      @Override
      public void onSuccess(None result) {
        // OK
        future.set(result);
      }
    });
    try {
      // Synchronously wait for shutdown to complete
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      Assert.fail("Client shutdown failed");
    }
  }
}
