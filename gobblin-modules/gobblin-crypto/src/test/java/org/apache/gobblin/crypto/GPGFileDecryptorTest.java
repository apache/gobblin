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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.bouncycastle.openpgp.PGPException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;


/**
 * Test class for {@link GPGFileDecryptor}
 * Test key and test passphrase are generated offline
 */
public class GPGFileDecryptorTest {

  private static final String fileDir = "src/test/resources/crypto/gpg/";
  private static final String privateKey = "private.key";
  private static final String passwdBasedFile = "PasswordBasedEncryptionFile.txt.gpg";
  private static final String keyBasedFile = "KeyBasedEncryptionFile.txt.gpg";
  private static final String passPhrase = "test";

  private static final String expectedPasswdFileContent = "This is a password based encryption file.\n";
  private static final String expectedKeyFileContent = "This is a key based encryption file.\n";

  @Test (enabled=false)
  public void keyBasedDecryptionTest() throws IOException {
    try(InputStream is = GPGFileDecryptor.decryptFile(
        FileUtils.openInputStream(
            new File(fileDir, keyBasedFile)), FileUtils.openInputStream(new File(fileDir, privateKey)), passPhrase)) {
      Assert.assertEquals(IOUtils.toString(is, Charsets.UTF_8), expectedKeyFileContent);
    }
  }

  @Test (enabled=false)
  public void passwordBasedDecryptionTest() throws IOException {
    try(InputStream is = GPGFileDecryptor.decryptFile(
        FileUtils.openInputStream(new File(fileDir, passwdBasedFile)), passPhrase)) {
      Assert.assertEquals(IOUtils.toString(is, Charsets.UTF_8), expectedPasswdFileContent);
    }
  }

  /**
   * Decrypt a large (~1gb) password encrypted file and check that memory usage does not blow up
   * @throws IOException
   * @throws PGPException
   */
  @Test (enabled=true)
  public void decryptLargeFileSym() throws IOException, PGPException {
    System.gc();
    System.gc();

    long startHeapSize = Runtime.getRuntime().totalMemory();

    try(InputStream is = GPGFileDecryptor.decryptFile(
        getClass().getResourceAsStream("/crypto/gpg/passwordEncrypted.gpg"), "test")) {
      int value;
      long bytesRead = 0;

      // the file contains only the character 'a'
      while ((value = is.read()) != -1) {
        bytesRead++;
        Assert.assertTrue(value == 'a');
      }

      Assert.assertEquals(bytesRead, 1041981183L);

      // Make sure no error thrown if read again after reaching EOF
      Assert.assertEquals(is.read(), -1);

      System.gc();
      System.gc();
      long endHeapSize = Runtime.getRuntime().totalMemory();

      // make sure the heap doesn't grow too much
      Assert.assertTrue(endHeapSize - startHeapSize < 200 * 1024 * 1024,
          "start heap " + startHeapSize + " end heap " + endHeapSize);
    }
  }

  /**
   * Decrypt a large (~1gb) private key encrypted file and check that memory usage does not blow up
   * @throws IOException
   * @throws PGPException
   */
  @Test (enabled=true)
  public void decryptLargeFileAsym() throws IOException, PGPException {
    System.gc();
    System.gc();

    long startHeapSize = Runtime.getRuntime().totalMemory();

    try(InputStream is = GPGFileDecryptor.decryptFile(
        getClass().getResourceAsStream("/crypto/gpg/keyEncrypted.gpg"),
        getClass().getResourceAsStream("/crypto/gpg/testPrivate.key"), "gobblin")) {
      int value;
      long bytesRead = 0;

      // the file contains only the character 'a'
      while ((value = is.read()) != -1) {
        bytesRead++;
        Assert.assertTrue(value == 'a');
      }

      Assert.assertEquals(bytesRead, 1041981183L);

      System.gc();
      System.gc();
      long endHeapSize = Runtime.getRuntime().totalMemory();

      // make sure the heap doesn't grow too much
      Assert.assertTrue(endHeapSize - startHeapSize < 200 * 1024 * 1024,
          "start heap " + startHeapSize + " end heap " + endHeapSize);
    }
  }
}
