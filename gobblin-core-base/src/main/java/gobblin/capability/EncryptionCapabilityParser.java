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
package gobblin.capability;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import java.util.Collection;
import java.util.Map;


/**
 * Extract encryption related information from taskState
 */
public class EncryptionCapabilityParser extends CapabilityParser {
  private static final String ENCRYPT_PREFIX = ".encrypt";

  public static final String ENCRYPTION_TYPE_PROPERTY = "type";
  private static final String ENCRYPTION_KEYSTORE_PATH_KEY = "ks_path";
  private static final String ENCRYPTION_KEYSTORE_PASSWORD_KEY = "ks_password";

  public static final String ENCRYPTION_TYPE_ANY = "any";

  EncryptionCapabilityParser() {
    super(ImmutableSet.of(Capability.ENCRYPTION));
  }

  @Override
  public Collection<CapabilityRecord> parseForBranch(State taskState, int numBranches, int branch) {
    boolean capabilityExists = false;

    String prefixName = ConfigurationKeys.WRITER_PREFIX + ENCRYPT_PREFIX;
    Map<String, Object> properties = extractPropertiesForBranch(taskState.getProperties(), prefixName,
        numBranches, branch);

    // 'writer.encrypt' will get stripped down to the empty string by extractPropsFromBranch;
    // transform it into a type property
    String encryptionType = (String)properties.get("");
    if (encryptionType != null) {
      encryptionType = encryptionType.toLowerCase();

      if (encryptionType.equals("true")) {
        encryptionType = ENCRYPTION_TYPE_ANY;
      }

      if (!encryptionType.equals("false")) {
        capabilityExists = true;
        properties.put(ENCRYPTION_TYPE_PROPERTY, encryptionType);
      }

      properties.remove(prefixName);
    }

    return ImmutableList.of(new CapabilityRecord(Capability.ENCRYPTION, capabilityExists, properties));
  }

  public static String getEncryptionType(Map<String, Object> properties) {
    return (String)properties.get(ENCRYPTION_TYPE_PROPERTY);
  }

  public static String getKeystorePath(Map<String, Object> properties) {
    return (String)properties.get(ENCRYPTION_KEYSTORE_PATH_KEY);
  }

  public static String getKeystorePassword(Map<String, Object> properties) {
    return (String)properties.get(ENCRYPTION_KEYSTORE_PASSWORD_KEY);
  }
}
