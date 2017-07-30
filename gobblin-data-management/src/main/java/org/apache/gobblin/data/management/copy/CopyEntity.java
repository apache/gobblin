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

import gobblin.util.guid.Guid;
import gobblin.util.guid.HasGuid;
import gobblin.util.io.GsonInterfaceAdapter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


/**
 * Abstraction for a work unit for distcp.
 */
@Getter
@Setter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@EqualsAndHashCode
public class CopyEntity implements HasGuid {

  public static final Gson GSON = GsonInterfaceAdapter.getGson(Object.class);

  /**
   * File set this file belongs to. {@link CopyEntity}s in the same fileSet and originating from the same
   * {@link CopyableDataset} will be treated as a unit: they will be published nearly atomically, and a notification
   * will be emitted for each fileSet when it is published.
   */
  private String fileSet;
  /** Contains arbitrary metadata usable by converters and/or publisher. */
  @Singular(value = "metadata")
  private Map<String, String> additionalMetadata;

  @Override
  public Guid guid() throws IOException {
    return Guid.fromStrings(toString());
  }

  /**
   * Serialize an instance of {@link CopyEntity} into a {@link String}.
   *
   * @param copyEntity to be serialized
   * @return serialized string
   */
  public static String serialize(CopyEntity copyEntity) {
    return GSON.toJson(copyEntity);
  }

  /**
   * Serialize a {@link List} of {@link CopyEntity}s into a {@link String}.
   *
   * @param copyEntities to be serialized
   * @return serialized string
   */
  public static String serializeList(List<CopyEntity> copyEntities) {
    return GSON.toJson(copyEntities, new TypeToken<List<CopyEntity>>() {}.getType());
  }

  /**
   * Deserializes the serialized {@link CopyEntity} string.
   *
   * @param serialized string
   * @return a new instance of {@link CopyEntity}
   */
  public static CopyEntity deserialize(String serialized) {
    return GSON.fromJson(serialized, CopyEntity.class);
  }

  /**
   * Deserializes the serialized {@link List} of {@link CopyEntity} string.
   * Used together with {@link #serializeList(List)}
   *
   * @param serialized string
   * @return a new {@link List} of {@link CopyEntity}s
   */
  public static List<CopyEntity> deserializeList(String serialized) {
    return GSON.fromJson(serialized, new TypeToken<List<CopyEntity>>() {}.getType());
  }

  @Override
  public String toString() {
    return serialize(this);
  }

  /**
   * Get a {@link DatasetAndPartition} instance for the dataset and fileSet this {@link CopyEntity} belongs to.
   * @param metadata {@link CopyableDatasetMetadata} for the dataset this {@link CopyEntity} belongs to.
   * @return an instance of {@link DatasetAndPartition}
   */
  public DatasetAndPartition getDatasetAndPartition(CopyableDatasetMetadata metadata) {
    return new DatasetAndPartition(metadata, getFileSet());
  }

  /**
   * Used for simulate runs. Should explain what this copy entity will do.
   */
  public String explain() {
    return toString();
  }

  /**
   * Uniquely identifies a fileSet by also including the dataset metadata.
   */
  @Data
  @EqualsAndHashCode
  public static class DatasetAndPartition {
    private final CopyableDatasetMetadata dataset;
    private final String partition;

    /**
     * @return a unique string identifier for this {@link DatasetAndPartition}.
     */
    @SuppressWarnings("deprecation")
    public String identifier() {
      return Hex.encodeHexString(DigestUtils.sha(this.dataset.toString() + this.partition));
    }
  }
}
