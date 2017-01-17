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

import com.google.common.base.Optional;
import com.google.common.base.Predicate;

import gobblin.data.management.copy.hive.HiveCopyEntityHelper;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveLocationDescriptor;

import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * A fast table skip predicate that reads the parameter {@link HiveDataset#REGISTRATION_GENERATION_TIME_MILLIS} from
 * the Hive non partition table and skips the copy if it is newer than the modification time of the location of the source
 * table.
 */
@AllArgsConstructor
@Slf4j
public class NonPartitionTableRegistrationTimeSkipPredicate implements Predicate<HiveCopyEntityHelper> {


  @Override
  public boolean apply(@Nullable HiveCopyEntityHelper helper) {

    if (helper == null) {
      return true;
    }

    if (!helper.getExistingTargetTable().isPresent()) {
      // Target table doesn't exit, don't skip
      return false;
    }

    if (!helper.getExistingTargetTable().get().getParameters().containsKey(HiveDataset.REGISTRATION_GENERATION_TIME_MILLIS)) {
      // Target table is not annotated with registration time, don't skip
      return false;
    }

    try {
      long oldRegistrationTime = Long.parseLong(
          helper.getExistingTargetTable().get().getParameters().get(HiveDataset.REGISTRATION_GENERATION_TIME_MILLIS));

      HiveLocationDescriptor sourceHiveDescriptor = HiveLocationDescriptor.forTable(helper.getDataset().getTable(),
          helper.getDataset().getFs(), helper.getDataset().getProperties());

      Optional<FileStatus> sourceFileStatus = helper.getConfiguration().getCopyContext().
          getFileStatus(helper.getDataset().getFs(), sourceHiveDescriptor.getLocation());

      if (!sourceFileStatus.isPresent()) {
        throw new RuntimeException(String.format("Source path %s does not exist!", sourceHiveDescriptor.getLocation()));
      }

      // If the registration time of the table is higher than the modification time, skip
      return oldRegistrationTime > sourceFileStatus.get().getModificationTime();

    } catch (NumberFormatException nfe) {
      // Cannot parse registration generation time, don't skip
      log.warn(String.format("Cannot parse %s in table %s. Will not skip.",
          HiveDataset.REGISTRATION_GENERATION_TIME_MILLIS, helper.getDataset().getTable().getDbName()+"."+helper.getDataset().getTable().getTableName()));
      return false;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

  }

}
