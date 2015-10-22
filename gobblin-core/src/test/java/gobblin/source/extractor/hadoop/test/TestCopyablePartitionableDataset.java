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

package gobblin.source.extractor.hadoop.test;

import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.partition.Partition;
import gobblin.data.management.partition.PartitionableDataset;

import java.util.Collection;

import com.beust.jcommander.internal.Lists;


/**
 * Partitionable dataset that partitions files depending on whether their name, parsed as an
 * integer, is above or below a threshold.
 */
public class TestCopyablePartitionableDataset extends TestCopyableDataset
    implements PartitionableDataset<CopyableFile> {

  public static final String BELOW = "below";
  public static final String ABOVE = "above";
  public static final int THRESHOLD = TestCopyableDataset.FILE_COUNT / 2;

  @Override public Collection<Partition<CopyableFile>> partitionFiles(Collection<? extends CopyableFile> files) {
    Partition.Builder<CopyableFile> belowThreshold = new Partition.Builder<CopyableFile>(BELOW);
    Partition.Builder<CopyableFile> aboveThreshold = new Partition.Builder<CopyableFile>(ABOVE);

    for(CopyableFile file : files) {
      if(Integer.parseInt(file.getOrigin().getPath().getName()) < THRESHOLD) {
        belowThreshold.add(file);
      } else {
        aboveThreshold.add(file);
      }
    }

    return Lists.newArrayList(belowThreshold.build(), aboveThreshold.build());
  }

  @Override public Class<?> fileClass() {
    return CopyableFile.class;
  }
}
