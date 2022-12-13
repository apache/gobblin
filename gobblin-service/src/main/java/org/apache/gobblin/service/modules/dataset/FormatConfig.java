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

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.ArrayList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A location-independent descriptor of a dataset, which describes a dataset in terms of its physical attributes.
 * The physical attributes include:
 *  <ul>
 *    <p> Data format (e.g. Avro, CSV, JSON). </p>
 *    <p> Data encoding type (e.g. Gzip, Bzip2, Base64, Deflate). </p>
 *    <p> Encryption properties (e.g. aes_rotating, gpg). </p>
 *  </ul>
 */
@Alpha
@ToString (exclude = {"rawConfig", "isInputDataset"})
@EqualsAndHashCode (exclude = {"rawConfig", "isInputDataset"})
public class FormatConfig {
  @Getter
  private final String format;
  @Getter
  private final String codecType;
  @Getter
  private final EncryptionConfig encryptionConfig;
  @Getter
  private final Config rawConfig;
  @Getter
  protected Boolean isInputDataset;

  private static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(DatasetDescriptorConfigKeys.FORMAT_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)
          .put(DatasetDescriptorConfigKeys.CODEC_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)
          .build());

  public FormatConfig(Config config) throws IOException {
    this.format = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.FORMAT_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY).toLowerCase();
    this.codecType = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.CODEC_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY).toLowerCase();
    this.encryptionConfig = new EncryptionConfig(ConfigUtils.getConfig(config, DatasetDescriptorConfigKeys.ENCYPTION_PREFIX, ConfigFactory
        .empty()));
    this.rawConfig = config.withFallback(this.encryptionConfig.getRawConfig().atPath(DatasetDescriptorConfigKeys.ENCYPTION_PREFIX)).
        withFallback(DEFAULT_FALLBACK);
    this.isInputDataset = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_INPUT_DATASET, false);
  }

  public ArrayList<String> contains(FormatConfig other) {
    ArrayList<String> errors = new ArrayList<>();
    errors.addAll(containsFormat(other.getFormat(), other.getIsInputDataset()));
    errors.addAll(containsCodec(other.getCodecType(), other.getIsInputDataset()));
    errors.addAll(containsEncryptionConfig(other.getEncryptionConfig()));
    return errors;
  }

  private ArrayList<String> containsFormat(String otherFormat, Boolean inputDataset) {
    ArrayList<String> errors = new ArrayList<>();
    String datasetDescriptorPrefix = inputDataset ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getFormat())
        && (!this.getFormat().equalsIgnoreCase(otherFormat))) {
      errors.add(datasetDescriptorPrefix + "." + DatasetDescriptorConfigKeys.FORMAT_KEY + " is mismatched. User input: '" + otherFormat
          + "'. Expected value: '" + this.getFormat() + "'.");
    }
    return errors;
  }

  private ArrayList<String> containsCodec(String otherCodecType, Boolean inputDataset) {
    ArrayList<String> errors = new ArrayList<>();
    String datasetDescriptorPrefix = inputDataset ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getCodecType())
        && (!this.getCodecType().equalsIgnoreCase(otherCodecType))) {
      errors.add(datasetDescriptorPrefix + "." + DatasetDescriptorConfigKeys.CODEC_KEY + " is mismatched. User input: '" + otherCodecType
          + "'. Expected value: '" + this.getCodecType() + "'.");
    }
    return errors;
  }

  private ArrayList<String> containsEncryptionConfig(EncryptionConfig otherEncryptionConfig) {
    return this.getEncryptionConfig().contains(otherEncryptionConfig);
  }
}
