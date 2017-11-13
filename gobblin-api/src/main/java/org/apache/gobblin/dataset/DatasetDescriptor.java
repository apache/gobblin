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


import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.RequiredArgsConstructor;


/**
 * A {@link DatasetDescriptor} identifies and provides metadata to describe a dataset
 */
@RequiredArgsConstructor
public final class DatasetDescriptor {
  private static final String PLATFORM_KEY = "platform";
  private static final String NAME_KEY = "name";

  /**
   * which platform the dataset is stored, for example: local, hdfs, oracle, mysql, kafka
   */
  @Getter
  private final String platform;
  /**
   * name of the dataset
   */
  @Getter
  private final String name;

  /**
   * metadata about the dataset
   */
  private final Map<String, String> metadata = Maps.newHashMap();

  public DatasetDescriptor(DatasetDescriptor copy) {
    platform = copy.getPlatform();
    name = copy.getName();
    metadata.putAll(copy.getMetadata());
  }

  public ImmutableMap<String, String> getMetadata() {
    return ImmutableMap.<String, String>builder()
        .putAll(metadata)
        .build();
  }

  public void addMetadata(String key, String value) {
    metadata.put(key, value);
  }

  /**
   * Serialize to a string map
   */
  public Map<String, String> toDataMap() {
    Map<String, String> map = Maps.newHashMap();
    map.put(PLATFORM_KEY, platform);
    map.put(NAME_KEY, name);
    map.putAll(metadata);
    return map;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DatasetDescriptor that = (DatasetDescriptor) o;
    return platform.equals(that.platform) && name.equals(that.name) && metadata.equals(that.metadata);
  }

  @Override
  public int hashCode() {
    int result = platform.hashCode();
    result = 31 * result + name.hashCode();
    result = 31 * result + metadata.hashCode();
    return result;
  }

  /**
   * Deserialize a {@link DatasetDescriptor} from a string map
   */
  public static DatasetDescriptor fromDataMap(Map<String, String> dataMap) {
    DatasetDescriptor descriptor = new DatasetDescriptor(dataMap.get(PLATFORM_KEY), dataMap.get(NAME_KEY));
    dataMap.forEach((key, value) -> {
      if (!key.equals(PLATFORM_KEY) && !key.equals(NAME_KEY)) {
        descriptor.addMetadata(key, value);
      }
    });
    return descriptor;
  }
}
