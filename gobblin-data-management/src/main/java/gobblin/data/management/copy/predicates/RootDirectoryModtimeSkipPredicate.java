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

package gobblin.data.management.copy.predicates;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;

import gobblin.data.management.copy.hive.HiveCopyEntityHelper;
import gobblin.data.management.copy.hive.HivePartitionFileSet;
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
public class RootDirectoryModtimeSkipPredicate implements Predicate<HivePartitionFileSet> {

  private final HiveCopyEntityHelper helper;

  @Override
  public boolean apply(@Nullable HivePartitionFileSet input) {

    if (input == null) {
      return true;
    }

    if (!input.getExistingTargetPartition().isPresent()) {
      return false;
    }

    try {

      if (PathUtils.isGlob(input.getPartition().getDataLocation())) {
        log.error(String.format("%s cannot be applied to globbed location %s. Will not skip.",
            this.getClass().getSimpleName(), input.getPartition().getDataLocation()));
        return false;
      }

      Path targetPath = this.helper.getTargetFileSystem().makeQualified(this.helper.getTargetPathHelper().getTargetPath(
          input.getPartition().getDataLocation(), this.helper.getTargetFs(), Optional.of(input.getPartition()), false));

      Optional<FileStatus> targetFileStatus =
          this.helper.getConfiguration().getCopyContext().getFileStatus(this.helper.getTargetFs(), targetPath);

      if (!targetFileStatus.isPresent()) {
        return false;
      }

      Optional<FileStatus> sourceFileStatus = this.helper.getConfiguration().getCopyContext()
          .getFileStatus(this.helper.getDataset().getFs(), input.getPartition().getDataLocation());

      if (!sourceFileStatus.isPresent()) {
        throw new RuntimeException(
            String.format("Source path %s does not exist!", input.getPartition().getDataLocation()));
      }

      return targetFileStatus.get().getModificationTime() > sourceFileStatus.get().getModificationTime();

    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

  }

}
