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

import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;


/**
 * An implementation of {@link BaseFsDatasetDescriptor}.
 */
@Alpha
public abstract class BaseFsDatasetDescriptor implements DatasetDescriptor {
  @Getter
  private final String path;
  @Getter
  private final String format;
  @Getter
  private final String codecType;
  @Getter
  private final String description;
  @Getter
  private final EncryptionConfig encryptionConfig;
  @Getter
  private final Config rawConfig;

  public BaseFsDatasetDescriptor(Config config) {
    this.path = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.PATH_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY);
    this.format = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.FORMAT_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY);
    this.codecType = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.CODEC_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY);
    this.encryptionConfig = new EncryptionConfig(ConfigUtils.getConfig(config, DatasetDescriptorConfigKeys.ENCYPTION_PREFIX, ConfigFactory.empty()));
    this.description = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.DESCRIPTION_KEY, "");
    this.rawConfig = config;
  }

  @Override
  public boolean isPropertyCompatibleWith(DatasetDescriptor other) {
    return isFormatCompatible(other.getFormat()) && isCodecTypeCompatible(other.getCodecType()) && isEncryptionCompatible(other.getEncryptionConfig());
  }

  /**
   * A helper to determine if the path description of this {@link DatasetDescriptor} is a subset of paths
   * accepted by the other {@link DatasetDescriptor}. If the path description of this {@link DatasetDescriptor}
   * is a glob pattern, we return false.
   *
   * @param otherPath a glob pattern that describes a set of paths.
   * @return true if the glob pattern described by the otherPath matches the path in this {@link DatasetDescriptor}.
   */
  public boolean isPathCompatible(String otherPath) {
    if (otherPath == null) {
      return false;
    }
    if (DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equals(otherPath)) {
      return true;
    }
    if (PathUtils.isGlob(new Path(this.getPath()))) {
      return false;
    }
    GlobPattern globPattern = new GlobPattern(otherPath);
    return globPattern.matches(this.getPath());
  }

  private boolean isFormatCompatible(String otherFormat) {
    return DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equals(otherFormat) || (this.getFormat().equalsIgnoreCase(otherFormat));
  }

  private boolean isCodecTypeCompatible(String otherCodecType) {
    return DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equals(otherCodecType) || (this.getCodecType().equalsIgnoreCase(otherCodecType));
  }

  private boolean isEncryptionCompatible(EncryptionConfig otherEncryptionConfig) {
    return this.getEncryptionConfig().isCompatibleWith(otherEncryptionConfig);
  }

  @Override
  public String toString() {
     return "(" + Joiner.on(",").join(this.getPlatform(), this.getPath(), this.getFormat(), this.getCodecType(), this.getEncryptionConfig().toString()) + ")";
  }
}
