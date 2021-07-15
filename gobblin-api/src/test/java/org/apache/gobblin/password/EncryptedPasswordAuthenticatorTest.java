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

import java.io.File;
import java.net.Authenticator;
import java.util.Properties;
import java.util.UUID;

import org.jasypt.util.text.BasicTextEncryptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;


/**
 * {@link Authenticator} that uses a username and password from the provided {@link Properties} to authenticate, and
 * also decrypts the password using {@link PasswordManager}
 */
public class EncryptedPasswordAuthenticatorTest extends Authenticator {
  @Test (enabled=false)
  public void testEncryptedPassword() throws Exception {
    String password = UUID.randomUUID().toString();
    String masterPassword = UUID.randomUUID().toString();
    File masterPwdFile = PasswordManagerTest.getMasterPwdFile(masterPassword);
    BasicTextEncryptor encryptor = new BasicTextEncryptor();
    encryptor.setPassword(masterPassword);

    Properties props = new Properties();
    props.put(EncryptedPasswordAuthenticator.AUTHENTICATOR_USERNAME, "testuser");
    props.put(EncryptedPasswordAuthenticator.AUTHENTICATOR_PASSWORD, "ENC(" + encryptor.encrypt(password) + ")");
    props.put(ConfigurationKeys.ENCRYPT_KEY_LOC, masterPwdFile.toString());

    EncryptedPasswordAuthenticator authenticator = new EncryptedPasswordAuthenticator(props);

    Assert.assertEquals(authenticator.getPasswordAuthentication().getUserName(), "testuser");
    Assert.assertEquals(authenticator.getPasswordAuthentication().getPassword(), password.toCharArray());

    masterPwdFile.delete();
  }

  @Test
  public void testUnencryptedPassword() {
    String password = UUID.randomUUID().toString();

    Properties props = new Properties();
    props.put(EncryptedPasswordAuthenticator.AUTHENTICATOR_USERNAME, "testuser");
    props.put(EncryptedPasswordAuthenticator.AUTHENTICATOR_PASSWORD, password);

    EncryptedPasswordAuthenticator authenticator = new EncryptedPasswordAuthenticator(props);

    Assert.assertEquals(authenticator.getPasswordAuthentication().getUserName(), "testuser");
    Assert.assertEquals(authenticator.getPasswordAuthentication().getPassword(), password.toCharArray());
  }
}
