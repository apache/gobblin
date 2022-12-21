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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.google.common.base.Enums;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A class that is used to describe partition configuration of a filesystem-based dataset. Common partitioning
 * types include "datetime" and "regex". For each partition type, the corresponding partition pattern (e.g. date pattern or
 * the regex pattern) is validated.
 */
@Slf4j
@ToString (exclude = {"rawConfig", "isInputDataset"})
@EqualsAndHashCode (exclude = {"rawConfig", "isInputDataset"})
public class FSDatasetPartitionConfig {
  @Getter
  private final String partitionType;
  @Getter
  private final String partitionPattern;
  @Getter
  private final Config rawConfig;
  @Getter
  protected Boolean isInputDataset;

  public enum PartitionType {
    DATETIME("datetime"),
    REGEX("regex"),
    NONE(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_NONE),
    ANY(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY);

    private final String type;

    PartitionType(final String type) {
      this.type = type;
    }

    @Override
    public String toString() {
      return this.type;
    }
  }

  private static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(DatasetDescriptorConfigKeys.PARTITION_TYPE_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)
          .put(DatasetDescriptorConfigKeys.PARTITION_PATTERN_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)
          .build());

  public FSDatasetPartitionConfig(Config config) throws IOException {
    String partitionType = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.PARTITION_PREFIX + "." + DatasetDescriptorConfigKeys.PARTITION_TYPE_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY).toLowerCase();
    String partitionPattern = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.PARTITION_PREFIX + "." + DatasetDescriptorConfigKeys.PARTITION_PATTERN_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY);

    if (partitionType.equalsIgnoreCase(PartitionType.NONE.name())) {
      partitionPattern = DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_NONE;
    } else if(partitionType.equalsIgnoreCase(PartitionType.ANY.name())) {
      partitionPattern = DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY;
    }
    validatePartitionConfig(partitionType, partitionPattern);
    this.partitionType = partitionType;
    this.partitionPattern = partitionPattern;
    this.rawConfig = ConfigUtils.getConfig(config, DatasetDescriptorConfigKeys.PARTITION_PREFIX, DEFAULT_FALLBACK);
    this.isInputDataset = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_INPUT_DATASET, false);
  }

  private void validatePartitionConfig(String partitionType, String partitionPattern)
      throws IOException {
    if (!Enums.getIfPresent(PartitionType.class, partitionType.toUpperCase()).isPresent()) {
      log.error("Invalid partition type {}", partitionType);
      throw new IOException("Invalid partition type");
    }
    switch (PartitionType.valueOf(partitionType.toUpperCase())) {
      case DATETIME:
        try {
          new SimpleDateFormat(partitionPattern);
        } catch (Exception e) {
          log.error("Invalid datetime partition pattern {}", partitionPattern);
          throw new IOException(e);
        }
        break;
      case REGEX:
        try {
          Pattern.compile(partitionPattern);
        } catch (PatternSyntaxException e) {
          log.error("Invalid regex partition pattern {}", partitionPattern);
          throw new IOException(e);
        }
        break;
      case NONE:
        if (!partitionPattern.equalsIgnoreCase(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_NONE)) {
          log.error("Partition pattern {} incompatible with partition type {}", partitionPattern, partitionType);
          throw new IOException("Incompatible partition pattern/type");
        }
        break;
      case ANY:
        if (!partitionPattern.equalsIgnoreCase(DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)) {
          log.error("Partition pattern {} incompatible with partition type {}", partitionPattern, partitionType);
          throw new IOException("Incompatible partition pattern/type");
        }
        break;
    }
  }

  public ArrayList<String> contains(FSDatasetPartitionConfig userFlowConfig) {
    String datasetDescriptorPrefix = userFlowConfig.getIsInputDataset() ? DatasetDescriptorConfigKeys.FLOW_INPUT_DATASET_DESCRIPTOR_PREFIX : DatasetDescriptorConfigKeys.FLOW_OUTPUT_DATASET_DESCRIPTOR_PREFIX;
    ArrayList<String> errors = new ArrayList<>();
    if (userFlowConfig == null) {
      errors.add("Missing Dataset Partition Config");
      return errors;
    }

    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(this.getPartitionType())
        && !this.getPartitionType().equalsIgnoreCase(userFlowConfig.getPartitionType())) {
      errors.add(datasetDescriptorPrefix + "." + DatasetDescriptorConfigKeys.PARTITION_PREFIX + "." + DatasetDescriptorConfigKeys.PARTITION_TYPE_KEY + " is mismatched. User input: '" + userFlowConfig.getPartitionType()
          + "'. Expected value: '" + this.getPartitionType() + "'.");
    }

    if (!DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equalsIgnoreCase(getPartitionPattern())
        && !this.getPartitionPattern().equalsIgnoreCase(userFlowConfig.getPartitionPattern())) {
      errors.add(datasetDescriptorPrefix + "." + DatasetDescriptorConfigKeys.PARTITION_PREFIX + "." + DatasetDescriptorConfigKeys.PARTITION_PATTERN_KEY + " is mismatched. User input: '" + userFlowConfig.getPartitionPattern()
          + "'. Expected value: '" + this.getPartitionPattern() + "'.");
    }
    return errors;
  }
}
