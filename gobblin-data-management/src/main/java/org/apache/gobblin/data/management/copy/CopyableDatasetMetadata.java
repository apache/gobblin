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

package gobblin.data.management.copy;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.google.gson.Gson;


/**
 * A {@link CopyableDataset} that is used to serialize into state objects. The class exists because custom
 * implementations of {@link CopyableDataset} may contain additional fields that should not be serialized.
 * The class is a data object and does not carry any functionality
 */
@EqualsAndHashCode(callSuper = false)
@ToString
public class CopyableDatasetMetadata {

  public CopyableDatasetMetadata(CopyableDatasetBase copyableDataset) {
    this.datasetURN = copyableDataset.datasetURN();
  }

  @Getter
  private final String datasetURN;
  private static final Gson GSON = new Gson();

  /**
   * Serialize an instance of {@link CopyableDatasetMetadata} into a {@link String}.
   *
   * @return serialized string
   */
  public String serialize() {
    return GSON.toJson(this);
  }

  /**
   * Deserializes the serialized {@link CopyableDatasetMetadata} string.
   *
   * @param serialized string
   * @return a new instance of {@link CopyableDatasetMetadata}
   */
  public static CopyableDatasetMetadata deserialize(String serialized) {
    return GSON.fromJson(serialized, CopyableDatasetMetadata.class);
  }

}
