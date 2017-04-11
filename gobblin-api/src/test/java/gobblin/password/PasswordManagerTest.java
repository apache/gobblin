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

package gobblin.password;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

import org.jasypt.exceptions.EncryptionOperationNotPossibleException;
import org.jasypt.util.text.BasicTextEncryptor;
import org.jasypt.util.text.StrongTextEncryptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;

@Test(groups = {"disabledOnTravis"} )
public class PasswordManagerTest {

  @Test
  public void testReadNormalPassword() throws IOException {
    String password = UUID.randomUUID().toString();
    String masterPassword = UUID.randomUUID().toString();
    File masterPwdFile = getMasterPwdFile(masterPassword);
    State state = new State();
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, masterPwdFile.toString());
    Assert.assertEquals(PasswordManager.getInstance(state).readPassword(password), password);
    masterPwdFile.delete();
  }

  @Test
  public void testMasterPasswordNotExist() {
    String password = "ENC(" + UUID.randomUUID().toString() + ")";
    State state = new State();
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, UUID.randomUUID());
    Assert.assertEquals(PasswordManager.getInstance(state).readPassword(password), password);
  }

  @Test
  public void testBasicEncryptionAndDecryption() throws IOException {
    String password = UUID.randomUUID().toString();
    String masterPassword = UUID.randomUUID().toString();
    File masterPwdFile = getMasterPwdFile(masterPassword);
    State state = new State();
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, masterPwdFile.toString());
    BasicTextEncryptor encryptor = new BasicTextEncryptor();
    encryptor.setPassword(masterPassword);
    String encrypted = encryptor.encrypt(password);
    encrypted = "ENC(" + encrypted + ")";
    String decrypted = PasswordManager.getInstance(state).readPassword(encrypted);
    Assert.assertEquals(decrypted, password);
  }

  @Test
  public void testStrongEncryptionAndDecryption() throws IOException {
    String password = UUID.randomUUID().toString();
    String masterPassword = UUID.randomUUID().toString();
    File masterPwdFile = getMasterPwdFile(masterPassword);
    State state = new State();
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, masterPwdFile.toString());
    state.setProp(ConfigurationKeys.ENCRYPT_USE_STRONG_ENCRYPTOR, true);
    try{
      StrongTextEncryptor encryptor = new StrongTextEncryptor();
      encryptor.setPassword(masterPassword);
      String encrypted = encryptor.encrypt(password);
      encrypted = "ENC(" + encrypted + ")";
      String decrypted = PasswordManager.getInstance(state).readPassword(encrypted);
      Assert.assertEquals(decrypted, password);
    }
    catch (EncryptionOperationNotPossibleException e) {
      //no strong encryption is supported
    }
  }

  private File getMasterPwdFile(String masterPwd) throws IOException {
    File masterPwdFile = File.createTempFile("masterPassword", null);
    Files.write(masterPwd, masterPwdFile, Charset.defaultCharset());
    return masterPwdFile;
  }
}
