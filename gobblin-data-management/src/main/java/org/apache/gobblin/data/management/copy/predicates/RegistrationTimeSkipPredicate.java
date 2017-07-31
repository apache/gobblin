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

package org.apache.gobblin.data.management.copy.predicates;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;

import org.apache.gobblin.data.management.copy.hive.HiveCopyEntityHelper;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.HivePartitionFileSet;
import org.apache.gobblin.util.PathUtils;

import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * A fast partition skip predicate that reads the parameter {@link HiveDataset#REGISTRATION_GENERATION_TIME_MILLIS} from
 * the Hive partition and skips the copy if it is newer than the modification time of the location of the source
 * partition.
 */
@AllArgsConstructor
@Slf4j
public class RegistrationTimeSkipPredicate implements Predicate<HivePartitionFileSet> {

  private final HiveCopyEntityHelper helper;

  @Override
  public boolean apply(@Nullable HivePartitionFileSet input) {

    if (input == null) {
      return true;
    }

    if (input.getExistingTargetPartition() == null) {
      throw new RuntimeException("Existing target partition has not been computed! This is an error in the code.");
    }

    if (PathUtils.isGlob(input.getPartition().getDataLocation())) {
      log.error(String.format("%s cannot be applied to globbed location %s. Will not skip.", this.getClass().getSimpleName(),
          input.getPartition().getDataLocation()));
      return false;
    }

    if (!input.getExistingTargetPartition().isPresent()) {
      // Target partition doesn't exit, don't skip
      return false;
    }

    if (!input.getExistingTargetPartition().get().getParameters().containsKey(HiveDataset.REGISTRATION_GENERATION_TIME_MILLIS)) {
      // Target partition is not annotated with registration time, don't skip
      return false;
    }

    try {
      long oldRegistrationTime = Long.parseLong(
          input.getExistingTargetPartition().get().getParameters().get(HiveDataset.REGISTRATION_GENERATION_TIME_MILLIS));

      Optional<FileStatus> sourceFileStatus = this.helper.getConfiguration().getCopyContext().
          getFileStatus(this.helper.getDataset().getFs(), input.getPartition().getDataLocation());

      if (!sourceFileStatus.isPresent()) {
        throw new RuntimeException(String.format("Source path %s does not exist!", input.getPartition().getDataLocation()));
      }

      // If the registration time of the partition is higher than the modification time, skip
      return oldRegistrationTime > sourceFileStatus.get().getModificationTime();

    } catch (NumberFormatException nfe) {
      // Cannot parse registration generation time, don't skip
      log.warn(String.format("Cannot parse %s in partition %s. Will not skip.",
          HiveDataset.REGISTRATION_GENERATION_TIME_MILLIS, input.getPartition().getCompleteName()));
      return false;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

  }

}
