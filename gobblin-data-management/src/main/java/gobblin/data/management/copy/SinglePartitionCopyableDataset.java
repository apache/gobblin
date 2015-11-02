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

import gobblin.data.management.partition.Partition;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

/**
 * An implementation of {@link CopyableDataset} that partitions all files into a single partition
 */
public abstract class SinglePartitionCopyableDataset implements CopyableDataset {
  @SuppressWarnings("unchecked")
  @Override
  public Collection<Partition<CopyableFile>> partitionFiles(Collection<? extends CopyableFile> files) {

    List<CopyableFile> copyableFiles = Lists.newArrayListWithCapacity(files.size());
    for (CopyableFile file : files) {
      copyableFiles.add(file);
    }
    return Lists.newArrayList(new Partition.Builder<CopyableFile>(datasetRoot().toString()).add(copyableFiles).build());

  }

  @Override
  public Class<?> fileClass() {
    return CopyableFile.class;
  }
}
