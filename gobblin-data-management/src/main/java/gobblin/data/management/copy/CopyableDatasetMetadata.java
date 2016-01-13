/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy;

import java.io.IOException;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;


/**
 * A {@link CopyableDataset} that is used to serialize into state objects. The class exists because custom
 * implementations of {@link CopyableDataset} may contain additional fields that should not be serialized.
 * The class is a data object and does not carry any functionality
 */
@EqualsAndHashCode(callSuper=false)
@ToString
public class CopyableDatasetMetadata {

  public CopyableDatasetMetadata(CopyableDataset copyableDataset, Path datasetTargetRoot) {
    this.datasetRoot = copyableDataset.datasetRoot();
    this.datasetTargetRoot = datasetTargetRoot;
  }

  @Getter
  private final Path datasetRoot;
  @Getter
  private final Path datasetTargetRoot;
  private static final Gson GSON = new Gson();

  /**
   * Serialize an instance of {@link CopyableDatasetMetadata} into a {@link String}.
   *
   * @return serialized string
   */
  public String serialize() throws IOException {
    return GSON.toJson(this);
  }

  /**
   * Deserializes the serialized {@link CopyableDatasetMetadata} string.
   *
   * @param serialized string
   * @return a new instance of {@link CopyableDatasetMetadata}
   */
  public static CopyableDatasetMetadata deserialize(String serialized) throws IOException {
    return GSON.fromJson(serialized, CopyableDatasetMetadata.class);
  }

}
