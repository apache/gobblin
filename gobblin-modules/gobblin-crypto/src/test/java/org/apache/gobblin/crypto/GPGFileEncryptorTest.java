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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.bouncycastle.openpgp.PGPException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test class for {@link GPGFileDecryptor}
 * Test key and test passphrase are generated offline
 */
public class GPGFileEncryptorTest {
  public static final String PASSWORD = "test";
  public static final String PASSPHRASE = "gobblin";
  public static final String PUBLIC_KEY = "/crypto/gpg/testPublic.key";
  public static final String PRIVATE_KEY = "/crypto/gpg/testPrivate.key";
  public static final String KEY_ID = "c27093CA21A87D6F";
  public static final String EXPECTED_FILE_CONTENT = "This is a key based encryption file.";
  public static final byte[] EXPECTED_FILE_CONTENT_BYTES = EXPECTED_FILE_CONTENT.getBytes(StandardCharsets.UTF_8);

  /**
   * Encrypt a test string with a symmetric key and check that it can be decrypted
   * @throws IOException
   * @throws PGPException
   */
  @Test
  public void encryptSym() throws IOException, PGPException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream os = GPGFileEncryptor.encryptFile(baos, PASSWORD, "DES");

    os.write(EXPECTED_FILE_CONTENT_BYTES);
    os.close();
    baos.close();

    byte[] encryptedBytes = baos.toByteArray();

    try (InputStream is = GPGFileDecryptor.decryptFile(new ByteArrayInputStream(encryptedBytes), "test")) {
      byte[] decryptedBytes = IOUtils.toByteArray(is);

      Assert.assertNotEquals(EXPECTED_FILE_CONTENT_BYTES, encryptedBytes);
      Assert.assertEquals(EXPECTED_FILE_CONTENT_BYTES, decryptedBytes);
    }
  }

  /**
   * Encrypt a test string with an asymmetric key and check that it can be decrypted
   * @throws IOException
   * @throws PGPException
   */
  @Test
  public void encryptAsym() throws IOException, PGPException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream os = GPGFileEncryptor.encryptFile(baos, getClass().getResourceAsStream(PUBLIC_KEY),
        Long.parseUnsignedLong(KEY_ID, 16), "CAST5");

    os.write(EXPECTED_FILE_CONTENT_BYTES);
    os.close();
    baos.close();

    byte[] encryptedBytes = baos.toByteArray();

    try (InputStream is = GPGFileDecryptor.decryptFile(new ByteArrayInputStream(encryptedBytes),
        getClass().getResourceAsStream(PRIVATE_KEY), PASSPHRASE)) {
      byte[] decryptedBytes = IOUtils.toByteArray(is);

      Assert.assertNotEquals(EXPECTED_FILE_CONTENT_BYTES, encryptedBytes);
      Assert.assertEquals(EXPECTED_FILE_CONTENT_BYTES, decryptedBytes);
    }
  }

  /**
   * Test error with bad cipher
   */
  @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*BadCipher.*")
  public void badCipher() throws IOException, PGPException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    OutputStream os = GPGFileEncryptor.encryptFile(baos, getClass().getResourceAsStream(PUBLIC_KEY),
        Long.parseUnsignedLong(KEY_ID, 16), "BadCipher");
  }
}
