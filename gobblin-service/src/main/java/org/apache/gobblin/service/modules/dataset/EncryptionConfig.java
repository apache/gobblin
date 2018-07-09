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

import com.google.common.base.Joiner;
import com.typesafe.config.Config;

import lombok.Getter;

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


public class EncryptionConfig {
  @Getter
  private final String encryptionAlgorithm;
  @Getter
  private final String keystoreType;
  @Getter
  private final String keystoreEncoding;

  public EncryptionConfig(Config encryptionConfig) {
    this.encryptionAlgorithm = ConfigUtils.getString(encryptionConfig, DatasetDescriptorConfigKeys.ENCRYPTION_ALGORITHM_KEY,
        DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY);
    this.keystoreType = ConfigUtils.getString(encryptionConfig, DatasetDescriptorConfigKeys.ENCRYPTION_KEYSTORE_TYPE_KEY,
        DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY);
    this.keystoreEncoding = ConfigUtils.getString(encryptionConfig, DatasetDescriptorConfigKeys.ENCRYPTION_KEYSTORE_ENCODING_KEY,
        DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY);
  }

  public boolean isCompatibleWith(EncryptionConfig other) {
    if (other == null) {
      return false;
    }

    String otherEncryptionAlgorithm = other.getEncryptionAlgorithm();
    String otherKeystoreType = other.getKeystoreType();
    String otherKeystoreEncoding = other.getKeystoreEncoding();

    return (DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equals(otherEncryptionAlgorithm)
        || this.encryptionAlgorithm.equals(otherEncryptionAlgorithm))
        && (DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equals(otherKeystoreType)
        || this.keystoreType.equals(otherKeystoreType)) && (DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equals(otherKeystoreEncoding)
        || this.keystoreEncoding.equals(otherKeystoreEncoding));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof EncryptionConfig)) {
      return false;
    }
    EncryptionConfig other = (EncryptionConfig) o;
    return this.getEncryptionAlgorithm().equalsIgnoreCase(other.getEncryptionAlgorithm()) && this.keystoreEncoding.equalsIgnoreCase(other.getKeystoreEncoding())
        && this.getKeystoreType().equalsIgnoreCase(other.getKeystoreType());
  }

  @Override
  public String toString() {
    return "(" + Joiner.on(",").join(this.encryptionAlgorithm, this.keystoreType, this.keystoreEncoding) + ")";
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }

}
