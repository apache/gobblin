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

package gobblin.data.management.copy.predicates;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;

import gobblin.data.management.copy.CopyContext;
import gobblin.data.management.copy.hive.HiveCopyEntityHelper;
import gobblin.util.PathUtils;

import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * Use with {@link HiveCopyEntityHelper#FAST_PARTITION_SKIP_PREDICATE}.
 * Skips partitions whose data location exists in the target, and such that the target location has a newer mod time
 * than the source location.
 */
@AllArgsConstructor
@Slf4j
public class RootDirectoryModtimeSkipPredicate implements Predicate<Partition> {

  private HiveCopyEntityHelper helper;

  @Override
  public boolean apply(@Nullable Partition input) {

    if (input == null) {
      return true;
    }

    try {

      if (PathUtils.isGlob(input.getDataLocation())) {
        log.error(String.format("%s cannot be applied to globbed location %s. Will not skip.", this.getClass().getSimpleName(),
            input.getDataLocation()));
        return false;
      }

      Path targetPath = this.helper.getTargetFileSystem().makeQualified(
          this.helper.getTargetPath(input.getDataLocation(), this.helper.getTargetFs(), Optional.of(input), false));

      Optional<FileStatus> targetFileStatus =
          this.helper.getConfiguration().getCopyContext().getFileStatus(this.helper.getTargetFs(), targetPath);

      if (!targetFileStatus.isPresent()) {
        return false;
      }

      Optional<FileStatus> sourceFileStatus = this.helper.getConfiguration().getCopyContext().
          getFileStatus(this.helper.getDataset().getFs(), input.getDataLocation());


      if (!sourceFileStatus.isPresent()) {
        throw new RuntimeException(String.format("Source path %s does not exist!", input.getDataLocation()));
      }

      return targetFileStatus.get().getModificationTime() > sourceFileStatus.get().getModificationTime();

    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

  }

}
