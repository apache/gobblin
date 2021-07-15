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

package org.apache.gobblin.password;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;

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

@Test(enabled=false, groups = {"disabledOnCI"} )
public class PasswordManagerTest {

  @Test (enabled=false)
  public void testReadNormalPassword() throws IOException {
    String password = UUID.randomUUID().toString();
    String masterPassword = UUID.randomUUID().toString();
    File masterPwdFile = getMasterPwdFile(masterPassword);
    State state = new State();
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, masterPwdFile.toString());
    Assert.assertEquals(PasswordManager.getInstance(state).readPassword(password), password);
    masterPwdFile.delete();
  }

  @Test (enabled=false)
  public void testMasterPasswordNotExist() {
    String password = "ENC(" + UUID.randomUUID().toString() + ")";
    State state = new State();
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, UUID.randomUUID());
    Assert.assertEquals(PasswordManager.getInstance(state).readPassword(password), password);
  }

  @Test (enabled=false)
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

  @Test (enabled=false)
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

  @Test (enabled=false)
  public void testMultipleMasterPasswords() throws IOException {
    String password = UUID.randomUUID().toString();

    String masterPassword = UUID.randomUUID().toString();
    String masterPassword1 = UUID.randomUUID().toString();
    String masterPassword2 = UUID.randomUUID().toString();
    String masterPassword3 = UUID.randomUUID().toString();

    File masterPasswordFile = File.createTempFile("masterPassword", null);
    Files.write(masterPassword, masterPasswordFile, Charset.defaultCharset());
    Files.write(masterPassword1, new File(masterPasswordFile.toString()+".1"), Charset.defaultCharset());
    Files.write(masterPassword2, new File(masterPasswordFile.toString()+".2"), Charset.defaultCharset());
    Files.write(masterPassword3, new File(masterPasswordFile.toString()+".3"), Charset.defaultCharset());

    State state = new State();
    BasicTextEncryptor encryptor = new BasicTextEncryptor();

    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, masterPasswordFile.toString());
    state.setProp(ConfigurationKeys.NUMBER_OF_ENCRYPT_KEYS, 3);
    PasswordManager passwordManager = PasswordManager.getInstance(state);

    // Test current master password
    encryptor.setPassword(masterPassword);
    String encrypted = "ENC(" + encryptor.encrypt(password) + ")";
    String decrypted = passwordManager.readPassword(encrypted);
    Assert.assertEquals(decrypted, password);

    // Test last master password using same passwordManager
    encryptor = new BasicTextEncryptor();
    encryptor.setPassword(masterPassword1);
    encrypted = "ENC(" + encryptor.encrypt(password) + ")";
    decrypted = passwordManager.readPassword(encrypted);
    Assert.assertEquals(decrypted, password);

    // Test second last master password using same passwordManager
    encryptor = new BasicTextEncryptor();
    encryptor.setPassword(masterPassword2);
    encrypted = "ENC(" + encryptor.encrypt(password) + ")";
    decrypted = passwordManager.readPassword(encrypted);
    Assert.assertEquals(decrypted, password);

    // Test third last master password using same passwordManager
    // This one is not accepted because ConfigurationKeys.NUMBER_OF_ENCRYPT_KEYS = 3
    encryptor = new BasicTextEncryptor();
    encryptor.setPassword(masterPassword3);
    encrypted = "ENC(" + encryptor.encrypt(password) + ")";
    try {
      passwordManager.readPassword(encrypted);
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().startsWith( "Failed to decrypt password"));
      return;
    }
    Assert.fail("Password Manager decrypted too old password.");
  }

  @Test (enabled=false)
  public void testMultipleMasterPasswordsWithoutPasswordFiles() throws IOException {
    String password = UUID.randomUUID().toString();

    String masterPassword = UUID.randomUUID().toString();
    String masterPassword1 = UUID.randomUUID().toString();

    File masterPasswordFile = File.createTempFile("masterPassword", null);
    Files.write(masterPassword, masterPasswordFile, Charset.defaultCharset());

    State state = new State();
    BasicTextEncryptor encryptor = new BasicTextEncryptor();

    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, masterPasswordFile.toString());
    PasswordManager passwordManager = PasswordManager.getInstance(state);

    // Test current master password
    encryptor.setPassword(masterPassword);
    String encrypted = "ENC(" + encryptor.encrypt(password) + ")";
    String decrypted = passwordManager.readPassword(encrypted);
    Assert.assertEquals(decrypted, password);

    // Test last master password using same passwordManager
    // This should throw FileNotFoundException as file for masterPassword1 is not created.
    encryptor = new BasicTextEncryptor();
    encryptor.setPassword(masterPassword1);
    encrypted = "ENC(" + encryptor.encrypt(password) + ")";
    try {
      passwordManager.readPassword(encrypted);
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().startsWith("Failed to decrypt password"));
      return;
    }
    Assert.fail("Password Manager decrypted password without correct master password.");
  }

  public static File getMasterPwdFile(String masterPwd) throws IOException {
    File masterPwdFile = File.createTempFile("masterPassword", null);
    Files.write(masterPwd, masterPwdFile, Charset.defaultCharset());
    return masterPwdFile;
  }
}