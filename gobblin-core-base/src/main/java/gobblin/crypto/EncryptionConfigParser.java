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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.password.PasswordManager;


/**
 * Extract encryption related information from taskState
 */
public class EncryptionConfigParser {
  /**
   * Encryption parameters for converters
   *  Algorithm: Encryption algorithm. Can be 'any' to let the system choose one.
   *  keystore_path: Location for the java keystore where encryption keys can be found
   *  keystore_password: Password the keystore is encrypted with
   */
  public static final String ENCRYPT_PREFIX = ConfigurationKeys.WRITER_PREFIX + ".encrypt";

  public static final String ENCRYPTION_ALGORITHM_KEY = "algorithm";
  public static final String ENCRYPTION_KEYSTORE_PATH_KEY = "keystore_path";
  public static final String ENCRYPTION_KEYSTORE_PASSWORD_KEY = "keystore_password";

  public static final String ENCRYPTION_TYPE_ANY = "any";

  public static Map<String, Object> getConfigForBranch(State taskState, int numBranches, int branch) {
    Map<String, Object> properties = extractPropertiesForBranch(taskState.getProperties(), ENCRYPT_PREFIX,
        numBranches, branch);
    if (properties.isEmpty()) {
      return null;
    }

    PasswordManager passwordManager = PasswordManager.getInstance(taskState);
    if (properties.containsKey(ENCRYPTION_KEYSTORE_PASSWORD_KEY)) {
      properties.put(ENCRYPTION_KEYSTORE_PASSWORD_KEY,
          passwordManager.readPassword((String)properties.get(ENCRYPTION_KEYSTORE_PASSWORD_KEY)));
    }

    return properties;
  }

  public static String getEncryptionType(Map<String, Object> properties) {
    return (String)properties.get(ENCRYPTION_ALGORITHM_KEY);
  }

  public static String getKeystorePath(Map<String, Object> properties) {
    return (String)properties.get(ENCRYPTION_KEYSTORE_PATH_KEY);
  }

  public static String getKeystorePassword(Map<String, Object> properties) {
    return (String)properties.get(ENCRYPTION_KEYSTORE_PASSWORD_KEY);
  }

  /**
   * Extract a set of properties for a given branch, stripping out the prefix and branch
   * suffix.
   *
   * Eg - original output:
   *  writer.encrypt.1 -> foo
   *  writer.encrypt.something.1 -> bar
   *
   *  will transform to
   *
   *  "" -> foo
   *  something - bar
   * this is very similar to ConfigUtils and typesafe config; need to figure out config story
   * @param properties Properties to extract data from
   * @param prefix Prefix to match; all other properties will be ignored
   * @param numBranches # of branches
   * @param branch Branch # to extract
   * @return Transformed properties as described above
   */
  private static Map<String, Object> extractPropertiesForBranch(
      Properties properties, String prefix, int numBranches, int branch) {

    Map<String, Object> ret = new HashMap<>();
    String branchSuffix = (numBranches > 1) ? String.format(".%d", branch) : "";

    for (Map.Entry<Object, Object> prop: properties.entrySet()) {
      String key = (String)prop.getKey();
      if (key.startsWith(prefix) && (branchSuffix.length() == 0 || key.endsWith(branchSuffix))) {
        int strippedKeyStart =  Math.min(key.length(), prefix.length() + 1);
        int strippedKeyEnd = Math.max(strippedKeyStart, key.length() - branchSuffix.length());
        String strippedKey = key.substring(strippedKeyStart, strippedKeyEnd);
        ret.put(strippedKey, prop.getValue());
      }
    }

    return ret;
  }
}
