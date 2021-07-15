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

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.Properties;

import com.google.common.base.Preconditions;


/**
 * {@link Authenticator} that uses a username and password from the provided {@link Properties} to authenticate, and
 * also decrypts the password using {@link PasswordManager}
 */
public class EncryptedPasswordAuthenticator extends Authenticator {
  public static final String AUTHENTICATOR_USERNAME = "authenticator.username";
  public static final String AUTHENTICATOR_PASSWORD = "authenticator.password";

  private final String username;
  private final String password;

  public EncryptedPasswordAuthenticator(Properties props) {
    this.username = props.getProperty(AUTHENTICATOR_USERNAME);
    this.password = PasswordManager.getInstance(props)
        .readPassword(props.getProperty(AUTHENTICATOR_PASSWORD));

    Preconditions.checkNotNull(this.username, AUTHENTICATOR_USERNAME + " must be set for EncryptedPasswordAuthenticator");
    Preconditions.checkNotNull(this.password, AUTHENTICATOR_PASSWORD + " must be set for EncryptedPasswordAuthenticator");
  }

  @Override
  protected PasswordAuthentication getPasswordAuthentication() {
    return new PasswordAuthentication(username, password.toCharArray());
  }
}
