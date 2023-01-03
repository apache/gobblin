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
@ToString(exclude = {"rawConfig", "isInputDataset"})
@EqualsAndHashCode (exclude = {"rawConfig", "isInputDataset"})
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
  @Getter
  protected Boolean isInputDataset;

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
    this.isInputDataset = ConfigUtils.getBoolean(encryptionConfig, DatasetDescriptorConfigKeys.IS_INPUT_DATASET, false);
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
    String datasetDescriptorPrefix = userFlowConfig.getIsInputDataset() ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    ArrayList<String> errors = new ArrayList<>();

    String userFlowEncryptionAlgorithm = userFlowConfig.getEncryptionAlgorithm();
    String userFlowKeystoreType = userFlowConfig.getKeystoreType();
    String userFlowKeystoreEncoding = userFlowConfig.getKeystoreEncoding();
    String userFlowEncryptionLevel = userFlowConfig.getEncryptionLevel();
    String userFlowEncryptedFields = userFlowConfig.getEncryptedFields();

    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getEncryptionAlgorithm())
        && !this.encryptionAlgorithm.equalsIgnoreCase(userFlowEncryptionAlgorithm)) {
      errors.add(datasetDescriptorPrefix + "." + DatasetDescriptorConfigKeys.ENCRYPTION_ALGORITHM_KEY + " is mismatched. User input: '" + userFlowEncryptionAlgorithm
          + "'. Expected value: '" + this.getEncryptionAlgorithm() + "'.");
    }

    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getKeystoreType())
        && !this.keystoreType.equalsIgnoreCase(userFlowKeystoreType)) {
      errors.add(datasetDescriptorPrefix + "." + DatasetDescriptorConfigKeys.ENCRYPTION_KEYSTORE_TYPE_KEY + " is mismatched. User input: '" + userFlowKeystoreType
          + "'. Expected value: '" + this.getKeystoreType() + "'.");
    }

    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getKeystoreEncoding())
        && !this.keystoreEncoding.equalsIgnoreCase(userFlowKeystoreEncoding)) {
      errors.add(datasetDescriptorPrefix + "." + DatasetDescriptorConfigKeys.ENCRYPTION_KEYSTORE_ENCODING_KEY + " is mismatched. User input: '" + userFlowKeystoreEncoding
          + "'. Expected value: " + this.getKeystoreEncoding() + "'.");
    }

    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getEncryptionLevel())
        && !this.encryptionLevel.equalsIgnoreCase(userFlowEncryptionLevel)) {
      errors.add(datasetDescriptorPrefix + "." + DatasetDescriptorConfigKeys.ENCRYPTION_LEVEL_KEY + " is mismatched. User input: '" + userFlowEncryptionLevel
          + "'. Expected value: '" + this.getEncryptionLevel()  + "'.");
    }

    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getEncryptedFields())
        && !this.encryptedFields.equalsIgnoreCase(userFlowEncryptedFields)) {
      errors.add(datasetDescriptorPrefix + "." + DatasetDescriptorConfigKeys.ENCRYPTED_FIELDS + " is mismatched. User input: '" + userFlowEncryptedFields
          + "'. Expected value: '" + this.getEncryptedFields() + ".");
    }

    return errors;
  }
}
