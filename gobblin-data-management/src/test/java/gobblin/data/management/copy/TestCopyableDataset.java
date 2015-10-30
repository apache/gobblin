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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import com.google.common.collect.Lists;


/**
 * Implementation of {@link CopyableDataset} for testing.
 */
public class TestCopyableDataset extends SinglePartitionCopyableDataset {

  public static final int FILE_COUNT = 10;
  public static final String ORIGIN_PREFIX = "/test";
  public static final String DESTINATION_PREFIX = "/destination";
  public static final String RELATIVE_PREFIX = "/relative";
  public static final OwnerAndPermission OWNER_AND_PERMISSION = new OwnerAndPermission("owner", "group",
      FsPermission.getDefault());

  @Override
  public List<CopyableFile> getCopyableFiles() throws IOException {
    List<CopyableFile> files = Lists.newArrayList();

    for (int i = 0; i < FILE_COUNT; i++) {
      files.add(new CopyableFile(new FileStatus(10, false, 0, 0, 0, new Path(ORIGIN_PREFIX, Integer.toString(i))),
          new Path(DESTINATION_PREFIX, Integer.toString(i)), new Path(RELATIVE_PREFIX, Integer.toString(i)),
          OWNER_AND_PERMISSION, Lists.newArrayList(OWNER_AND_PERMISSION), "checksum".getBytes()));
    }

    return files;
  }

  @Override
  public Path datasetRoot() {
    return new Path(ORIGIN_PREFIX);
  }

  @Override
  public Path datasetTargetRoot() {
    return new Path(DESTINATION_PREFIX);
  }
}
