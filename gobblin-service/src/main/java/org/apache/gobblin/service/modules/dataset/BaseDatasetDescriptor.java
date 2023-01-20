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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.ArrayList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorErrorUtils;
import org.apache.gobblin.util.ConfigUtils;

@EqualsAndHashCode (exclude = {"description", "rawConfig", "isInputDataset"})
@ToString (exclude = {"description", "rawConfig", "isInputDataset"})
public abstract class BaseDatasetDescriptor implements DatasetDescriptor {
  @Getter
  private final String platform;
  @Getter
  private final FormatConfig formatConfig;
  @Getter
  private final boolean isRetentionApplied;
  @Getter
  private final String description;
  @Getter
  private final Config rawConfig;
  @Getter
  protected Boolean isInputDataset;

  private static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(DatasetDescriptorConfigKeys.PATH_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)
          .put(DatasetDescriptorConfigKeys.IS_RETENTION_APPLIED_KEY, false)
          .build());

  public BaseDatasetDescriptor(Config config) throws IOException {
    Preconditions.checkArgument(config.hasPath(DatasetDescriptorConfigKeys.PLATFORM_KEY), "Dataset descriptor config must specify platform");
    this.platform = config.getString(DatasetDescriptorConfigKeys.PLATFORM_KEY).toLowerCase();
    this.formatConfig = new FormatConfig(config);
    this.isRetentionApplied = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_RETENTION_APPLIED_KEY, false);
    this.description = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.DESCRIPTION_KEY, "");
    this.rawConfig = config.withFallback(this.formatConfig.getRawConfig()).withFallback(DEFAULT_FALLBACK);
    this.isInputDataset = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_INPUT_DATASET, false);
  }

  /**
   * {@inheritDoc}
   */
  protected abstract ArrayList<String> isPathContaining(DatasetDescriptor other);

  /**
   * @return true if this {@link DatasetDescriptor} contains the other {@link DatasetDescriptor} i.e. the
   * datasets described by this {@link DatasetDescriptor} is a subset of the datasets described by the other
   * {@link DatasetDescriptor}. This operation is non-commutative.
   * @param inputDatasetDescriptorConfig This is the flow configuration that is sent in from user side and is compared against the flowgraph edges.
   */
  @Override
  public ArrayList<String> contains(DatasetDescriptor inputDatasetDescriptorConfig) {
    ArrayList<String> errors = new ArrayList<>();
    if (this == inputDatasetDescriptorConfig) {
      return errors;
    }

    DatasetDescriptorErrorUtils.populateErrorForDatasetDescriptorKey(errors, inputDatasetDescriptorConfig.getIsInputDataset(), DatasetDescriptorConfigKeys.CLASS_KEY, this.getClass().toString(), inputDatasetDescriptorConfig.getClass().toString(), false);
    DatasetDescriptorErrorUtils.populateErrorForDatasetDescriptorKey(errors, inputDatasetDescriptorConfig.getIsInputDataset(), DatasetDescriptorConfigKeys.PLATFORM_KEY, this.getPlatform(), inputDatasetDescriptorConfig.getPlatform(), false);
    DatasetDescriptorErrorUtils.populateErrorForDatasetDescriptorKey(errors, inputDatasetDescriptorConfig.getIsInputDataset(), DatasetDescriptorConfigKeys.IS_RETENTION_APPLIED_KEY, String.valueOf(this.isRetentionApplied()), String.valueOf(inputDatasetDescriptorConfig.isRetentionApplied()), false);

    errors.addAll(isPathContaining(inputDatasetDescriptorConfig));
    errors.addAll(getFormatConfig().contains(inputDatasetDescriptorConfig.getFormatConfig()));
    return errors;
  }
}
