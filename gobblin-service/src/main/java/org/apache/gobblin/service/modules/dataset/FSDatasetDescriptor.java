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
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.flowgraph.DatasetDescriptorConfigKeys;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PathUtils;


/**
 * An implementation of {@link DatasetDescriptor} with FS-based storage.
 */
@Alpha
@ToString (callSuper = true, exclude = {"rawConfig"})
@EqualsAndHashCode (callSuper = true, exclude = {"rawConfig"})
public class FSDatasetDescriptor extends BaseDatasetDescriptor implements DatasetDescriptor {
  @Getter
  private final String path;
  @Getter
  private final String subPaths;
  @Getter
  private final boolean isCompacted;
  @Getter
  private final boolean isCompactedAndDeduped;
  @Getter
  private final FSDatasetPartitionConfig partitionConfig;
  @Getter
  private final Config rawConfig;

  private static final Config DEFAULT_FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(DatasetDescriptorConfigKeys.IS_COMPACTED_KEY, false)
          .put(DatasetDescriptorConfigKeys.IS_COMPACTED_AND_DEDUPED_KEY, false)
          .build());

  public FSDatasetDescriptor(Config config) throws IOException {
    super(config);
    this.path = PathUtils
        .getPathWithoutSchemeAndAuthority(new Path(ConfigUtils.getString(config, DatasetDescriptorConfigKeys.PATH_KEY,
            DatasetDescriptorConfigKeys.DATASET_DESCRIPTOR_CONFIG_ANY))).toString();
    this.subPaths = ConfigUtils.getString(config, DatasetDescriptorConfigKeys.SUBPATHS_KEY, null);
    this.isCompacted = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_COMPACTED_KEY, false);
    this.isCompactedAndDeduped = ConfigUtils.getBoolean(config, DatasetDescriptorConfigKeys.IS_COMPACTED_AND_DEDUPED_KEY, false);
    this.partitionConfig = new FSDatasetPartitionConfig(ConfigUtils.getConfigOrEmpty(config, DatasetDescriptorConfigKeys.PARTITION_PREFIX));
    this.rawConfig = config.withFallback(getPartitionConfig().getRawConfig()).withFallback(DEFAULT_FALLBACK).withFallback(super.getRawConfig());
  }

  /**
   * If other descriptor has subpaths, this method checks that each concatenation of path + subpath is matched by this
   * path. Otherwise, it just checks the path.
   *
   * @param other descriptor whose path/subpaths to check
   * @return true if all subpaths are matched by this {@link DatasetDescriptor}'s path, or if subpaths is null and
   * the other's path matches this path.
   */
  @Override
  protected boolean isPathContaining(DatasetDescriptor other) {
    String otherPath = other.getPath();
    String otherSubPaths = ((FSDatasetDescriptor) other).getSubPaths();
    if (otherSubPaths != null) {
      List<String> subPaths = Splitter.on(",").splitToList(StringUtils.stripEnd(StringUtils.stripStart(otherSubPaths, "{"), "}"));
      for (String subPath : subPaths) {
        if (!isPathContaining(new Path(otherPath, subPath).toString())) {
          return false;
        }
      }
      return true;
    } else {
      return isPathContaining(otherPath);
    }
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
    if (!super.contains(o)) {
      return false;
    }

    FSDatasetDescriptor other = (FSDatasetDescriptor) o;

    if ((this.isCompacted() != other.isCompacted()) ||
        (this.isCompactedAndDeduped() != other.isCompactedAndDeduped())) {
      return false;
    }

    return this.getPartitionConfig().contains(other.getPartitionConfig());
  }
}
