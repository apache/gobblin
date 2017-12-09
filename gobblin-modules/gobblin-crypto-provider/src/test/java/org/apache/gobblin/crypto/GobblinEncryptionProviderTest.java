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
package org.apache.gobblin.crypto;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.codec.StreamCodec;


public class GobblinEncryptionProviderTest {
  @Test
  public void testCanBuildAes() throws IOException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "aes_rotating");
    properties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY, getClass().getResource(
        "/encryption_provider_test_keystore").toString());
    properties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY, "abcd");

    StreamCodec c = EncryptionFactory.buildStreamCryptoProvider(properties);
    Assert.assertNotNull(c);

    byte[] toEncrypt = "Hello!".getBytes(StandardCharsets.UTF_8);

    ByteArrayOutputStream cipherOut = new ByteArrayOutputStream();
    OutputStream cipherStream = c.encodeOutputStream(cipherOut);
    cipherStream.write(toEncrypt);
    cipherStream.close();

    Assert.assertTrue("Expected to be able to write ciphertext!", cipherOut.size() > 0);
  }
}
