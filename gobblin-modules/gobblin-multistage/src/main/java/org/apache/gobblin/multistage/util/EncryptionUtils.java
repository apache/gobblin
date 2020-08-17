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

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.password.PasswordManager;


/**
 * String encryption and decryption utilities
 */
public interface EncryptionUtils {
  String PATTERN = "^ENC\\(.*\\)$";
  /**
   * Decrypt the encrypted string using Gobblin utility
   * @param input the encrypted string
   * @param state Gobblin state object contains the master key location
   * @return decrypted string if the input string is enclosed inside ENC()
   */
  static String decryptGobblin(String input, State state) {
    if (input.matches(PATTERN)) {
      return PasswordManager.getInstance(state).readPassword(input);
    }
    return input;
  }

  /**
   * Encrypt the decrypted string using Gobblin utility
   * @param input the deccrypted string
   * @param state Gobblin state object contains the master key location
   * @return encrypted string which is enclosed within ENC() - as Gobblin utility doesn't do that explicitly
   */
  static String encryptGobblin(String input, State state) {
    String encryptedString = PasswordManager.getInstance(state).encryptPassword(input);
    if (encryptedString.matches(PATTERN)) {
      return encryptedString;
    }
    return "ENC(" + encryptedString + ")";
  }

}
