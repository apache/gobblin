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

import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;


/**
 * An implementation of {@link DatasetDescriptor} with FS-based storage.
 */
@Alpha
public class FSDatasetDescriptor implements DatasetDescriptor {
  @Getter
  private final String platform;
  @Getter
  private final String path;
  @Getter
  private final FormatConfig formatConfig;
  @Getter
  private final FsDatasetPartitionConfig partitionConfig;
  @Getter
  private final boolean isRetentionApplied;
  @Getter
  private final boolean isCompacted;
  @Getter
  private final boolean isCompactedAndDeduped;
  @Getter
  private final String description;
  @Getter
  private final Config rawConfig;

  private static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(DatasetDescriptorConfigKeys.PATH_KEY, DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY)
          .put(DatasetDescriptorConfigKeys.IS_RETENTION_APPLIED_KEY, false)
          .put(DatasetDescriptorConfigKeys.IS_COMPACTED_KEY, false)
          .put(DatasetDescriptorConfigKeys.IS_COMPACTED_AND_DEDUPED_KEY, false)
          .build());

  public FSDatasetDescriptor(Config config) throws IOException {
    Preconditions.checkArgument(config.hasPath(DatasetDescriptorConfigKeys.PLATFORM_KEY), "Dataset descriptor config must specify platform");
    this.platform = config.getString(DatasetDescriptorConfigKeys.PLATFORM_KEY);
    this.path = PathUtils.getPathWithoutSchemeAndAuthority(new Path(ConfigUtils.getString(config, DatasetDescriptorConfigKeys.PATH_KEY,
        DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY))).toString();
    this.formatConfig = new FormatConfig(config);
    this.partitionConfig = new FsDatasetPartitionConfig(ConfigUtils.getConfigOrEmpty(config, DatasetDescriptorConfigKeys.PARTITION_PREFIX));
    this.isRetentionApplied = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_RETENTION_APPLIED_KEY, false);
    this.isCompacted = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_COMPACTED_KEY, false);
    this.isCompactedAndDeduped = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_COMPACTED_AND_DEDUPED_KEY, false);
    this.description = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.DESCRIPTION_KEY, "");
    this.rawConfig = config.withFallback(this.formatConfig.getRawConfig()).withFallback(this.partitionConfig.getRawConfig()).withFallback(DEFAULT_FALLBACK);
  }

  /**
   * A helper to determine if the path description of this {@link DatasetDescriptor} is a superset of paths
   * accepted by the other {@link DatasetDescriptor}. If the path description of the other {@link DatasetDescriptor}
   * is a glob pattern, we return false.
   *
   * @param otherPath a glob pattern that describes a set of paths.
   * @return true if the glob pattern described by the otherPath matches the path in this {@link DatasetDescriptor}.
   */
  private boolean isPathContaining(String otherPath) {
    if (otherPath == null) {
      return false;
    }
    if (DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY.equals(this.getPath())) {
      return true;
    }
    if (PathUtils.isGlob(new Path(otherPath))) {
      return false;
    }
    GlobPattern globPattern = new GlobPattern(this.getPath());
    return globPattern.matches(otherPath);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean contains(DatasetDescriptor o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FSDatasetDescriptor)) {
      return false;
    }
    FSDatasetDescriptor other = (FSDatasetDescriptor) o;

    if (this.getPlatform() == null || other.getPlatform() == null || !this.getPlatform().equalsIgnoreCase(other.getPlatform())) {
      return false;
    }

    if ((this.isRetentionApplied() != other.isRetentionApplied()) || (this.isCompacted() != other.isCompacted()) ||
        (this.isCompactedAndDeduped() != other.isCompactedAndDeduped())) {
      return false;
    }

    return getFormatConfig().contains(other.getFormatConfig()) && getPartitionConfig().contains(other.getPartitionConfig())
        && isPathContaining(other.getPath());
  }

  /**
   *
   * @param o the other {@link FSDatasetDescriptor} to compare "this" {@link FSDatasetDescriptor} with.
   * @return true iff  "this" dataset descriptor is compatible with the "other" and the "other" dataset descriptor is
   * compatible with this dataset descriptor.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FSDatasetDescriptor)) {
      return false;
    }
    FSDatasetDescriptor other = (FSDatasetDescriptor) o;
    if (this.getPlatform() == null || other.getPlatform() == null || !this.getPlatform().equalsIgnoreCase(other.getPlatform())) {
      return false;
    }
    if ((this.isRetentionApplied() != other.isRetentionApplied()) || (this.isCompacted() != other.isCompacted()) ||
        (this.isCompactedAndDeduped() != other.isCompactedAndDeduped())) {
      return false;
    }
    return this.getPath().equals(other.getPath()) && this.getPartitionConfig().equals(other.getPartitionConfig()) &&
        this.getFormatConfig().equals(other.getFormatConfig());
  }

  @Override
  public String toString() {
     return "(" + Joiner.on(",").join(this.getPlatform(), this.getPath(), this.getFormatConfig().toString(), this.getPartitionConfig().toString(),
         String.valueOf(isRetentionApplied()), String.valueOf(isCompacted()), String.valueOf(isCompactedAndDeduped()))
         + ")";
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + platform.toLowerCase().hashCode();
    result = 31 * result + path.hashCode();
    result = 31 * result + Boolean.hashCode(isRetentionApplied);
    result = 31 * result + Boolean.hashCode(isCompacted);
    result = 31 * result + Boolean.hashCode(isCompactedAndDeduped);
    result = 31 * result + getFormatConfig().hashCode();
    result = 31 * result + getPartitionConfig().hashCode();
    return result;
  }

}
