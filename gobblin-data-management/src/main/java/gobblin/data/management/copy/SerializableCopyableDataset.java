/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;


/**
 * A {@link CopyableDataset} that is used to serialize into state objects. The class exists because custom
 * implementations of {@link CopyableDataset} may contain additional fields that should not be serialized.
 * The class is a data object and does not carry any functionality
 */
public class SerializableCopyableDataset extends SinglePartitionCopyableDataset {

  private SerializableCopyableDataset(CopyableDataset copyableDataset) {
    this.datasetRoot = copyableDataset.datasetRoot();
    this.datasetTargetRoot = copyableDataset.datasetTargetRoot();
  }

  private Path datasetRoot;
  private Path datasetTargetRoot;
  private static Gson gson = new Gson();

  @Override
  public List<CopyableFile> getCopyableFiles() throws IOException {
    // We may want to support this later by returning serialized CopyableFiles from the state
    throw new UnsupportedOperationException(
        "This implementation of CopyableDataset is ONLY meant for serialization and desrialization.");
  }

  @Override
  public Path datasetTargetRoot() {
    return this.datasetTargetRoot;
  }

  @Override
  public Path datasetRoot() {
    return this.datasetRoot;
  }

  /**
   * Serialize an instance of {@link SerializableCopyableDataset} into a {@link String}.
   *
   * @param SerializableCopyableDataset to be serialized
   * @return serialized string
   */
  public static String serialize(CopyableDataset copyableDataset) throws IOException {
    return gson.toJson(new SerializableCopyableDataset(copyableDataset));
  }

  /**
   * Deserializes the serialized {@link SerializableCopyableDataset} string.
   *
   * @param serialized string
   * @return a new instance of {@link SerializableCopyableDataset}
   */
  public static CopyableDataset deserialize(String serialized) throws IOException {
    return gson.fromJson(serialized, SerializableCopyableDataset.class);
  }
}
