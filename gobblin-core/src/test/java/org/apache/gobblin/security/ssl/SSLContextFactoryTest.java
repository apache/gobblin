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
package org.apache.gobblin.security.ssl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;


@Test
public class SSLContextFactoryTest {
  public void testCreateSSLContext()
      throws IOException {
    Map<String, String> values = new HashMap<>();

    boolean hasException = false;

    // KEY_STORE_FILE_PATH is required
    try {
      SSLContextFactory.createInstance(ConfigFactory.parseMap(values));
    } catch (ConfigException e) {
      hasException = true;
    }
    Assert.assertTrue(hasException);

    hasException = false;
    // TRUST_STORE_FILE_PATH is required
    try {
      SSLContextFactory.createInstance(ConfigFactory.parseMap(values));
    } catch (ConfigException e) {
      hasException = true;
    }
    Assert.assertTrue(hasException);

    values.put(SSLContextFactory.KEY_STORE_FILE_PATH, "identity.p12");
    values.put(SSLContextFactory.TRUST_STORE_FILE_PATH, "certs");
    values.put(SSLContextFactory.KEY_STORE_PASSWORD, "keyStorePassword");
    values.put(SSLContextFactory.TRUST_STORE_PASSWORD, "trustStorePassword");
    values.put(SSLContextFactory.KEY_STORE_TYPE, "XX");

    hasException = false;
    // KEY_STORE_TYPE not legal
    try {
      SSLContextFactory.createInstance(ConfigFactory.parseMap(values));
    } catch (IllegalArgumentException e) {
      hasException = true;
    }
    Assert.assertTrue(hasException);

    values.put(SSLContextFactory.KEY_STORE_TYPE, "PKCS12");
    try {
      SSLContextFactory.createInstance(ConfigFactory.parseMap(values));
    } catch (ConfigException | IllegalArgumentException e) {
      Assert.fail();
    } catch (Exception e) {
      // OK
    }
  }
}
