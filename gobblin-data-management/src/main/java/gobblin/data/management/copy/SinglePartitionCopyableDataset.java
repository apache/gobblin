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
import gobblin.data.management.partition.PartitionableDataset;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * An implementation of {@link CopyableDataset} that partitions all files into a single partition
 */
public abstract class SinglePartitionCopyableDataset implements CopyableDataset, PartitionableDataset<CopyableFile> {

  public static Collection<Partition<CopyableFile>> singlePartition(Collection<? extends CopyableFile> files,
      String name) {
    List<CopyableFile> copyableFiles = Lists.newArrayListWithCapacity(files.size());
    for (CopyableFile file : files) {
      copyableFiles.add(file);
    }
    return ImmutableList.of(new Partition.Builder<CopyableFile>(name).add(copyableFiles).build());
  }

  @SuppressWarnings("unchecked")
  @Override
  public Collection<Partition<CopyableFile>> partitionFiles(Collection<? extends CopyableFile> files) {
    return singlePartition(files, datasetRoot().toString());
  }

  @Override
  public Class<?> fileClass() {
    return CopyableFile.class;
  }
}
