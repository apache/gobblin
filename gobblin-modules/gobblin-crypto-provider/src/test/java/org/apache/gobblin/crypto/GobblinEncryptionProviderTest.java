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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.codec.StreamCodec;


public class GobblinEncryptionProviderTest {
  private static final long KEY_ID = -4435883136602571409L;
  private static final String PRIVATE_KEY = "/testPrivate.key";
  private static final String PUBLIC_KEY = "/testPublic.key";

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

    Assert.assertTrue(cipherOut.size() > 0, "Expected to be able to write ciphertext!");
  }

  @Test
  public void testCanBuildGPG() throws IOException {
    Map<String, Object> encryptionProperties = new HashMap<>();
    encryptionProperties.put(EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, GPGCodec.TAG);
    encryptionProperties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY, GPGFileEncryptor.class.getResource(
        PUBLIC_KEY).toString());
    encryptionProperties.put(EncryptionConfigParser.ENCRYPTION_KEY_NAME, String.valueOf(GPGFileEncryptorTest.KEY_ID));

    testGPG(encryptionProperties);
  }

  @Test
  public void testBuildGPGGoodCipher() throws IOException {
    Map<String, Object> encryptionProperties = new HashMap<>();
    encryptionProperties.put(EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, GPGCodec.TAG);
    encryptionProperties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY, GPGFileEncryptor.class.getResource(
        PUBLIC_KEY).toString());
    encryptionProperties.put(EncryptionConfigParser.ENCRYPTION_KEY_NAME, String.valueOf(GPGFileEncryptorTest.KEY_ID));
    encryptionProperties.put(EncryptionConfigParser.ENCRYPTION_CIPHER_KEY, "CAST5");

    testGPG(encryptionProperties);
  }

  @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*BadCipher.*")
  public void testBuildGPGBadCipher() throws IOException {
    Map<String, Object> encryptionProperties = new HashMap<>();
    encryptionProperties.put(EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, GPGCodec.TAG);
    encryptionProperties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY, GPGFileEncryptor.class.getResource(
        PUBLIC_KEY).toString());
    encryptionProperties.put(EncryptionConfigParser.ENCRYPTION_KEY_NAME, String.valueOf(GPGFileEncryptorTest.KEY_ID));
    encryptionProperties.put(EncryptionConfigParser.ENCRYPTION_CIPHER_KEY, "BadCipher");

    testGPG(encryptionProperties);
  }

  private void testGPG(Map<String, Object> encryptionProperties) throws IOException {
    StreamCodec encryptor = EncryptionFactory.buildStreamCryptoProvider(encryptionProperties);
    Assert.assertNotNull(encryptor);

    Map<String, Object> decryptionProperties = new HashMap<>();
    decryptionProperties.put(EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, GPGCodec.TAG);
    decryptionProperties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY, GPGFileEncryptor.class.getResource(
        PRIVATE_KEY).toString());
    decryptionProperties.put(EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY, GPGFileEncryptorTest.PASSPHRASE);
    StreamCodec decryptor = EncryptionFactory.buildStreamCryptoProvider(decryptionProperties);
    Assert.assertNotNull(decryptor);

    ByteArrayOutputStream cipherOut = new ByteArrayOutputStream();
    OutputStream cipherStream = encryptor.encodeOutputStream(cipherOut);
    cipherStream.write(GPGFileEncryptorTest.EXPECTED_FILE_CONTENT_BYTES);
    cipherStream.close();

    byte[] encryptedBytes = cipherOut.toByteArray();
    Assert.assertTrue(encryptedBytes.length > 0, "Expected to be able to write ciphertext!");

    try (InputStream is = decryptor.decodeInputStream(new ByteArrayInputStream(encryptedBytes))) {
      byte[] decryptedBytes = IOUtils.toByteArray(is);

      Assert.assertNotEquals(GPGFileEncryptorTest.EXPECTED_FILE_CONTENT_BYTES, encryptedBytes);
      Assert.assertEquals(GPGFileEncryptorTest.EXPECTED_FILE_CONTENT_BYTES, decryptedBytes);
    }
  }
}
