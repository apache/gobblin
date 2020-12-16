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

package org.apache.gobblin.dataset;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.Getter;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;


/**
 * A {@link Descriptor} identifies and provides metadata to describe a dataset
 */
public class DatasetDescriptor extends Descriptor {
  private static final String PLATFORM_KEY = "platform";
  private static final String NAME_KEY = "name";
  private static final String CLUSTER_NAME_KEY = "clusterName";

  /**
   * which platform the dataset is stored, for example: local, hdfs, oracle, mysql, kafka
   */
  @Getter
  private final String platform;

  /**
   * Human-readeable cluster name.
   *
   * @see org.apache.gobblin.util.ClustersNames
   */
  @Getter
  @Nullable
  private final String clusterName;

  /**
   * metadata about the dataset
   */
  private final Map<String, String> metadata = Maps.newHashMap();

  public DatasetDescriptor(String platform, String name) {
    super(name);
    this.clusterName = null;
    this.platform = platform;
  }

  public DatasetDescriptor(String platform, String clusterName, String name) {
    super(name);
    this.clusterName = clusterName;
    this.platform = platform;
  }

  /**
   * @deprecated use {@link #copy()}
   */
  @Deprecated
  public DatasetDescriptor(DatasetDescriptor copy) {
    super(copy.getName());
    platform = copy.getPlatform();
    clusterName = copy.getClusterName();
    metadata.putAll(copy.getMetadata());
  }

  public ImmutableMap<String, String> getMetadata() {
    return ImmutableMap.<String, String>builder()
        .putAll(metadata)
        .build();
  }

  @Override
  public DatasetDescriptor copy() {
    return new DatasetDescriptor(this);
  }

  public void addMetadata(String key, String value) {
    metadata.put(key, value);
  }

  /**
   * Serialize to a string map
   *
   * @deprecated use {@link Descriptor#serialize(Descriptor)}
   */
  @Deprecated
  public Map<String, String> toDataMap() {
    Map<String, String> map = Maps.newHashMap();
    map.put(PLATFORM_KEY, platform);
    map.put(NAME_KEY, getName());
    if (getClusterName() != null) {
      map.put(CLUSTER_NAME_KEY, getClusterName());
    }
    map.putAll(metadata);
    return map;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DatasetDescriptor that = (DatasetDescriptor) o;
    return platform.equals(that.platform) &&
            Objects.equals(clusterName, that.clusterName) &&
            metadata.equals(that.metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(platform, clusterName, metadata);
  }

  /**
   * Deserialize a {@link DatasetDescriptor} from a string map
   *
   * @deprecated use {@link Descriptor#deserialize(String)}
   */
  @Deprecated
  public static DatasetDescriptor fromDataMap(Map<String, String> dataMap) {
    DatasetDescriptor descriptor = new DatasetDescriptor(dataMap.get(PLATFORM_KEY),
            dataMap.getOrDefault(CLUSTER_NAME_KEY, null), dataMap.get(NAME_KEY));
    dataMap.forEach((key, value) -> {
      if (!key.equals(PLATFORM_KEY) && !key.equals(NAME_KEY) && !key.equals(CLUSTER_NAME_KEY)) {
        descriptor.addMetadata(key, value);
      }
    });
    return descriptor;
  }
}
