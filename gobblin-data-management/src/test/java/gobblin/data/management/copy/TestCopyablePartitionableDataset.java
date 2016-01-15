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

import org.apache.hadoop.fs.FileStatus;

/**
 * Partitionable dataset that partitions files depending on whether their name, parsed as an integer, is above or below
 * a threshold.
 */
public class TestCopyablePartitionableDataset extends TestCopyableDataset {

  public static final String BELOW = "below";
  public static final String ABOVE = "above";
  public static final int THRESHOLD = TestCopyableDataset.FILE_COUNT / 2;

  @Override protected void modifyCopyableFile(CopyableFile.Builder builder, FileStatus origin) {
    super.modifyCopyableFile(builder, origin);
    if (Integer.parseInt(origin.getPath().getName()) < THRESHOLD) {
      builder.fileSet(BELOW);
    } else {
      builder.fileSet(ABOVE);
    }
  }

}
