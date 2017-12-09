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

package org.apache.gobblin.data.management.copy.replication;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import lombok.Data;


/**
 * Class used to represent the meta data of the replication
 * @author mitu
 *
 */

@Data
public class ReplicationMetaData {
  private final Optional<Map<String, String>> values;

  public static ReplicationMetaData buildMetaData(Config config) {
    if (!config.hasPath(ReplicationConfiguration.METADATA)) {
      return new ReplicationMetaData(Optional.<Map<String, String>> absent());
    }

    Config metaDataConfig = config.getConfig(ReplicationConfiguration.METADATA);
    Map<String, String> metaDataValues = new HashMap<>();
    Set<Map.Entry<String, ConfigValue>> meataDataEntry = metaDataConfig.entrySet();
    for (Map.Entry<String, ConfigValue> entry : meataDataEntry) {
      metaDataValues.put(entry.getKey(), metaDataConfig.getString(entry.getKey()));
    }

    ReplicationMetaData metaData = new ReplicationMetaData(Optional.of(metaDataValues));
    return metaData;
  }

  @Override
  public String toString() {
    Joiner.MapJoiner mapJoiner = Joiner.on(',').withKeyValueSeparator("=");

    return Objects.toStringHelper(this.getClass()).add("metadata", mapJoiner.join(this.values.get())).toString();
  }
}
