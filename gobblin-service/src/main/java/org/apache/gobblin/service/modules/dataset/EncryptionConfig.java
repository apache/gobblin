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

package org.apache.gobblin.service.modules.dataset;

import java.io.IOException;

import com.google.common.base.Enums;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.ArrayList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.util.ConfigUtils;

@Slf4j
@ToString(exclude = {"rawConfig"})
@EqualsAndHashCode (exclude = {"rawConfig"})
public class EncryptionConfig {
  @Getter
  private final String encryptionAlgorithm;
  @Getter
  private final String encryptionLevel;
  @Getter
  private final String encryptedFields;
  @Getter
  private final String keystoreType;
  @Getter
  private final String keystoreEncoding;
  @Getter
  private final Config rawConfig;

  public enum  EncryptionLevel {
    FILE("file"),
    ROW("row"),
    FIELD("field"),
    NONE(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_NONE),
    ANY(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY);

    private final String level;

    EncryptionLevel(final String level) {
      this.level = level;
    }

    @Override
    public String toString() {
      return this.level;
    }

  }

  private static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(DatasetDescriptorConfigKeys.ENCRYPTION_ALGORITHM_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)
          .put(DatasetDescriptorConfigKeys.ENCRYPTION_KEYSTORE_TYPE_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)
          .put(DatasetDescriptorConfigKeys.ENCRYPTION_KEYSTORE_ENCODING_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)
          .put(DatasetDescriptorConfigKeys.ENCRYPTION_LEVEL_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)
          .put(DatasetDescriptorConfigKeys.ENCRYPTED_FIELDS, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)
          .build());

  public EncryptionConfig(Config encryptionConfig) throws IOException {
    this.encryptionAlgorithm = ConfigUtils.getString(encryptionConfig, DatasetDescriptorConfigKeys.ENCRYPTION_ALGORITHM_KEY,
        DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY).toLowerCase();
    if (this.encryptionAlgorithm.equalsIgnoreCase(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_NONE)) {
      this.keystoreType = DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_NONE;
      this.keystoreEncoding = DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_NONE;
      this.encryptionLevel = DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_NONE;
      this.encryptedFields = DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_NONE;
    } else {
      this.keystoreType = ConfigUtils.getString(encryptionConfig, DatasetDescriptorConfigKeys.ENCRYPTION_KEYSTORE_TYPE_KEY,
          DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY).toLowerCase();
      this.keystoreEncoding = ConfigUtils.getString(encryptionConfig, DatasetDescriptorConfigKeys.ENCRYPTION_KEYSTORE_ENCODING_KEY,
          DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY).toLowerCase();
      this.encryptionLevel = ConfigUtils.getString(encryptionConfig, DatasetDescriptorConfigKeys.ENCRYPTION_LEVEL_KEY,
          DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY).toLowerCase();
      this.encryptedFields = ConfigUtils.getString(encryptionConfig, DatasetDescriptorConfigKeys.ENCRYPTED_FIELDS,
          DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY).toLowerCase();
      validate(this.encryptionLevel, this.encryptedFields);
    }
    this.rawConfig = encryptionConfig.withFallback(DEFAULT_FALLBACK);
  }

  private void validate(String encryptionLevel, String encryptedFields) throws IOException {
    if (!Enums.getIfPresent(EncryptionLevel.class, encryptionLevel.toUpperCase()).isPresent()) {
      throw new IOException("Invalid encryption level " + encryptionLevel);
    }
    switch (EncryptionLevel.valueOf(encryptionLevel.toUpperCase())) {
      case FIELD:
        if ((encryptedFields.equalsIgnoreCase(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)) ||
            (encryptedFields.equalsIgnoreCase(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_NONE))) {
          log.error("Invalid input for encryptedFields {}", encryptedFields);
          throw new IOException("Invalid encryptedFields");
        }
        break;
      case NONE:
        if (!encryptedFields.equalsIgnoreCase(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_NONE)) {
          log.error("Invalid input for encryptedFields {}", encryptedFields);
          throw new IOException("Invalid encryptedFields");
        }
        break;
      default:
        break;
    }
    return;
  }

  public ArrayList<String> contains(EncryptionConfig userFlowConfig) {
    ArrayList<String> errors = new ArrayList<>();
    if (userFlowConfig == null) {
      errors.add("Empty EncryptionConfig");
      return errors;
    }

    String userFlowConfigEncryptionAlgorithm = userFlowConfig.getEncryptionAlgorithm();
    String userFlowotherKeystoreType = userFlowConfig.getKeystoreType();
    String userFlowotherKeystoreEncoding = userFlowConfig.getKeystoreEncoding();
    String userFlowotherEncryptionLevel = userFlowConfig.getEncryptionLevel();
    String userFlowotherEncryptedFields = userFlowConfig.getEncryptedFields();

    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getEncryptionAlgorithm())
        && !this.encryptionAlgorithm.equalsIgnoreCase(userFlowConfigEncryptionAlgorithm)) {
      errors.add("Mismatched encryption algorithm. Expected: " + this.getEncryptionAlgorithm() + " or any");
    }

    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getKeystoreType())
        && !this.keystoreType.equalsIgnoreCase(userFlowotherKeystoreType)) {
      errors.add("Mismatched keystore type. Expected: " + this.getKeystoreType() + " or any");
    }

    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getKeystoreEncoding())
        && !this.keystoreEncoding.equalsIgnoreCase(userFlowotherKeystoreEncoding)) {
      errors.add("Mismatched keystore encoding. Expected: " + this.getKeystoreEncoding() + " or any");
    }

    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getEncryptionLevel())
        && !this.encryptionLevel.equalsIgnoreCase(userFlowotherEncryptionLevel)) {
      errors.add("Mismatched encryption level. Expected: " + this.getEncryptionLevel() + " or any");
    }

    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getEncryptedFields())
        && !this.encryptedFields.equalsIgnoreCase(userFlowotherEncryptedFields)) {
      errors.add("Mismatched encrypted fields. Expected: " + this.getEncryptedFields() + " or any");
    }

    return errors;
  }
}
