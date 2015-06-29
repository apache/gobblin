/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.password;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

import org.testng.annotations.Test;
import org.testng.Assert;

import com.google.common.io.Files;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;


public class PasswordManagerTest {

  @Test
  public void testReadNormalPassword() throws IOException {
    String password = UUID.randomUUID().toString();

    File masterPwdFile = getMasterPwdFile();
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
  public void testEncryptionAndDecryption() throws IOException {
    String password = UUID.randomUUID().toString();
    File masterPwdFile = getMasterPwdFile();
    State state = new State();
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, masterPwdFile.toString());
    String encrypted = PasswordManager.getInstance(state).encryptPassword(password);
    String decrypted = PasswordManager.getInstance(state).decryptPassword(encrypted);
    Assert.assertEquals(decrypted, password);
  }

  private File getMasterPwdFile() throws IOException {
    File masterPwdFile = File.createTempFile("masterPassword", null);
    Files.write(UUID.randomUUID().toString(), masterPwdFile, Charset.defaultCharset());
    return masterPwdFile;
  }
}
