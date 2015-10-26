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

import gobblin.data.management.dataset.Dataset;
import gobblin.data.management.partition.PartitionableDataset;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;


/**
 * {@link Dataset} that supports finding {@link CopyableFile}s.
 */
public interface CopyableDataset extends PartitionableDataset<CopyableFile> {

  /**
   * Find all {@link CopyableFile}s in this dataset.
   * @return List of {@link CopyableFile}s in this dataset.
   * @throws IOException
   * @param targetFileSystem
   */
  public List<CopyableFile> getCopyableFiles(FileSystem targetFileSystem) throws IOException;

}
