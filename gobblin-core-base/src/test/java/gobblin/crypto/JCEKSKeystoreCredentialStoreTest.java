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

package gobblin.crypto;

import java.io.File;
import java.io.IOException;
import java.security.KeyStoreException;
import java.util.EnumSet;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class JCEKSKeystoreCredentialStoreTest {
  private File tempFile;

  @BeforeTest
  public void generateTempPath() throws IOException {
    tempFile = File.createTempFile("keystore_unit_test", null);
    tempFile.delete();
  }

  @AfterTest
  public void deleteTempPath() throws IOException {
    tempFile.delete();
  }

  @Test
  public void testGenerateKeys() throws KeyStoreException, IOException {
    final String path = tempFile.getAbsolutePath();
    final String password = "abcd";

    try {
      JCEKSKeystoreCredentialStore cs = new JCEKSKeystoreCredentialStore(path, password);
      Assert.fail("Expected exception to be thrown because keystore doesn't exist");
    } catch (IllegalArgumentException e) {
      // pass
    }

    JCEKSKeystoreCredentialStore cs = new JCEKSKeystoreCredentialStore(path, password, EnumSet.of(
        JCEKSKeystoreCredentialStore.CreationOptions.CREATE_IF_MISSING));
    cs.generateAesKeys(20, 0);

    cs = new JCEKSKeystoreCredentialStore(path, password);
    Assert.assertEquals(cs.getAllEncodedKeys().size(), 20);
  }
}
