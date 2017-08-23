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

import com.google.common.base.Charsets;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


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

  @Test
  public void keyBasedDecryptionTest() throws IOException {
    try(InputStream is = GPGFileDecryptor.decryptFile(
        FileUtils.openInputStream(
            new File(fileDir, keyBasedFile)), FileUtils.openInputStream(new File(fileDir, privateKey)), passPhrase)) {
      Assert.assertEquals(IOUtils.toString(is, Charsets.UTF_8), expectedKeyFileContent);
    }
  }

  @Test
  public void passwordBasedDecryptionTest() throws IOException {
    try(InputStream is = GPGFileDecryptor.decryptFile(
        FileUtils.openInputStream(new File(fileDir, passwdBasedFile)), passPhrase)) {
      Assert.assertEquals(IOUtils.toString(is, Charsets.UTF_8), expectedPasswdFileContent);
    }
  }

}
