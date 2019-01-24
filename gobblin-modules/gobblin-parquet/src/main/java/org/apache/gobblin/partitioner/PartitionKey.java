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
package org.apache.gobblin.partitioner;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;

import static java.util.Arrays.asList;


/**
 * Used to represent partition keys. If partition key contains dots ex: repo.name
 * then it is considered as nested key.
 * Nested keys are flattened by joining them with _ (underscore).
 * @author tilakpatidar@gmail.com
 */
@Getter
@AllArgsConstructor
class PartitionKey {
  private final List<String> levels;

  PartitionKey(String key) {
    this.levels = asList(key.split("\\."));
  }

  String getFlattenedKey() {
    return String.join("_", levels);
  }

  boolean isNested() {
    return levels.size() > 1;
  }

  String[] getSubKeys() {
    return this.isNested() ? this.levels.toArray(new String[]{}) : new String[]{this.getLevels().get(0)};
  }

  static List<PartitionKey> partitionKeys(String property) {
    return Arrays.stream(property.replaceAll("\\s*,\\s*", ",").split(",")).map(PartitionKey::new)
        .collect(Collectors.toList());
  }
}
