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

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.password.PasswordManager;
import gobblin.util.ForkOperatorUtils;


/**
 * Extract encryption related information from taskState
 */
@Slf4j
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

  /**
   * Retrieve encryption configuration for the branch the WorKUnitState represents
   * @param workUnitState State for the object querying config
   * @return A list of encryption properties or null if encryption isn't configured
   */
  public static Map<String, Object> getConfigForBranch(WorkUnitState workUnitState) {
    return getConfigForBranch(workUnitState.getJobState(), ForkOperatorUtils.getPropertyNameForBranch(workUnitState, ""));
  }

  /**
   * Retrieve encryption config for a given branch of a task
   * @param taskState State of the task
   * @param numBranches Number of branches overall
   * @param branch Branch # of the current object
   * @return A list of encryption properties or null if encryption isn't configured
   */
  public static Map<String, Object> getConfigForBranch(State taskState, int numBranches, int branch) {
    return getConfigForBranch(taskState, ForkOperatorUtils.getPropertyNameForBranch("", numBranches, branch));
  }

  private static Map<String, Object> getConfigForBranch(State taskState, String branchSuffix) {
    Map<String, Object> properties =
        extractPropertiesForBranch(taskState.getProperties(), ENCRYPT_PREFIX, branchSuffix);
    if (properties.isEmpty()) {
      return null;
    }

    if (getEncryptionType(properties) == null) {
      log.warn("Encryption algorithm not specified; ignoring other encryption settings");
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
   * @param branchSuffix Suffix for all config properties
   * @return Transformed properties as described above
   */
  private static Map<String, Object> extractPropertiesForBranch(
      Properties properties, String prefix, String branchSuffix) {

    Map<String, Object> ret = new HashMap<>();

    for (Map.Entry<Object, Object> prop: properties.entrySet()) {
      String key = (String)prop.getKey();
      if (key.startsWith(prefix) && (branchSuffix.length() == 0 || key.endsWith(branchSuffix))) {
        int strippedKeyStart = Math.min(key.length(), prefix.length() + 1);

        // filter out subkeys that don't have a '.' -- eg writer.encrypted.foo shouldn't be returned
        // if prefix is writer.encrypt
        if (strippedKeyStart != key.length() && key.charAt(strippedKeyStart - 1) != '.') {
          continue;
        }

        int strippedKeyEnd = Math.max(strippedKeyStart, key.length() - branchSuffix.length());
        String strippedKey = key.substring(strippedKeyStart, strippedKeyEnd);
        ret.put(strippedKey, prop.getValue());
      }
    }

    return ret;
  }
}
