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

package org.apache.gobblin.multistage.util;

import gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.password.PasswordManager;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


@PrepareForTest({PasswordManager.class})
public class EncryptionUtilsTest extends PowerMockTestCase {
  private final static String PLAIN_PASSWORD = "password";
  private final static String ENC_PASSWORD = "ENC(M6nV+j0lhqZ36RgvuF5TQMyNvBtXmkPl)";
  private SourceState state;
  @Mock
  private PasswordManager passwordManager;

  @BeforeMethod
  public void setUp() {
    String masterKeyLoc = this.getClass().getResource("/key/master_key").toString();
    state = new SourceState();
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, masterKeyLoc);
    PowerMockito.mockStatic(PasswordManager.class);
    PowerMockito.when(PasswordManager.getInstance(state)).thenReturn(passwordManager);
  }

  @Test
  void testDecryption() {
    when(passwordManager.readPassword(ENC_PASSWORD)).thenReturn(PLAIN_PASSWORD);
    Assert.assertEquals(EncryptionUtils.decryptGobblin(ENC_PASSWORD, state), PLAIN_PASSWORD);
    Assert.assertEquals(EncryptionUtils.decryptGobblin(PLAIN_PASSWORD, state), PLAIN_PASSWORD);
  }

  @Test
  void testEncryption() {
    when(passwordManager.encryptPassword(PLAIN_PASSWORD)).thenReturn(ENC_PASSWORD);
    when(passwordManager.readPassword(ENC_PASSWORD)).thenReturn(PLAIN_PASSWORD);
    Assert.assertEquals(EncryptionUtils.decryptGobblin(EncryptionUtils.encryptGobblin(PLAIN_PASSWORD, state), state),
        PLAIN_PASSWORD);

    when(passwordManager.encryptPassword(ENC_PASSWORD)).thenReturn(ENC_PASSWORD);
    Assert.assertEquals(EncryptionUtils.decryptGobblin(EncryptionUtils.encryptGobblin(ENC_PASSWORD, state), state),
        PLAIN_PASSWORD);
  }
}