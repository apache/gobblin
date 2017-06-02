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

package gobblin.data.management.copy.hive;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.entities.PrePublishStep;
import gobblin.metrics.event.MultiTimingEvent;
import gobblin.util.commit.DeleteFileCommitStep;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link HiveFileSet} that generates {@link CopyEntity}s for an unpartitioned Hive table.
 */
@Slf4j
public class UnpartitionedTableFileSet extends HiveFileSet {

  private final HiveCopyEntityHelper helper;

  public UnpartitionedTableFileSet(String name, HiveDataset dataset, HiveCopyEntityHelper helper) {
    super(name, dataset);
    this.helper = helper;
  }

  // Suppress warnings for "stepPriority++" in the PrePublishStep constructor, as stepPriority may be used later
  @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE")
  @Override
  protected Collection<CopyEntity> generateCopyEntities()
      throws IOException {
    MultiTimingEvent multiTimer = new MultiTimingEvent(this.helper.getEventSubmitter(), "TableCopy", true);

    int stepPriority = 0;
    String fileSet = getTable().getTableName();
    List<CopyEntity> copyEntities = Lists.newArrayList();

    Optional<Table> existingTargetTable = this.helper.getExistingTargetTable();
    if (existingTargetTable.isPresent()) {
      if (!this.helper.getTargetTable().getDataLocation().equals(existingTargetTable.get().getDataLocation())) {
        switch (this.helper.getExistingEntityPolicy()){
          case UPDATE_TABLE:
            // Update the location of files while keep the existing table entity.
            log.warn("Source table will not be deregistered while file locaiton has been changed, update source table's"
                + " file location to" + this.helper.getTargetTable().getDataLocation());
            break ;
          case REPLACE_TABLE:
            // Required to de-register the original table.
            log.warn("Source and target table are not compatible. Will override target table " + existingTargetTable.get()
                .getDataLocation());
            stepPriority = this.helper.addTableDeregisterSteps(copyEntities, fileSet, stepPriority, this.helper.getTargetTable());
            existingTargetTable = Optional.absent();
            break ;
          default:
            log.error("Source and target table are not compatible. Aborting copy of table " + this.helper.getTargetTable(),
                new HiveTableLocationNotMatchException(this.helper.getTargetTable().getDataLocation(),
                    existingTargetTable.get().getDataLocation()));
            multiTimer.close();

            return Lists.newArrayList();
        }
      }
    }

    stepPriority = this.helper.addSharedSteps(copyEntities, fileSet, stepPriority);

    HiveLocationDescriptor sourceLocation =
        HiveLocationDescriptor.forTable(getTable(), getHiveDataset().getFs(), getHiveDataset().getProperties());
    HiveLocationDescriptor desiredTargetLocation =
        HiveLocationDescriptor.forTable(this.helper.getTargetTable(), this.helper.getTargetFs(), getHiveDataset().getProperties());

    Optional<HiveLocationDescriptor> existingTargetLocation = existingTargetTable.isPresent() ? Optional.of(
        HiveLocationDescriptor.forTable(existingTargetTable.get(), this.helper.getTargetFs(), getHiveDataset().getProperties()))
        : Optional.<HiveLocationDescriptor> absent();

    if (this.helper.getFastTableSkip().isPresent() && this.helper.getFastTableSkip().get().apply(this.helper)) {
      log.info(String.format("Skipping copy of table %s due to fast table skip predicate.", getTable().getDbName()+"." + getTable().getTableName()));
      multiTimer.close();
      return Lists.newArrayList();
    }

    HiveCopyEntityHelper.DiffPathSet
        diffPathSet = HiveCopyEntityHelper.fullPathDiff(sourceLocation, desiredTargetLocation, existingTargetLocation,
        Optional.<Partition> absent(), multiTimer, this.helper);

    multiTimer.nextStage(HiveCopyEntityHelper.Stages.FULL_PATH_DIFF);

    // Could used to delete files for the existing snapshot
    DeleteFileCommitStep deleteStep =
        DeleteFileCommitStep.fromPaths(this.helper.getTargetFs(), diffPathSet.pathsToDelete, getHiveDataset().getProperties());
    copyEntities.add(new PrePublishStep(fileSet, Maps.<String, String> newHashMap(), deleteStep, stepPriority++));

    for (CopyableFile.Builder builder : this.helper.getCopyableFilesFromPaths(diffPathSet.filesToCopy, this.helper.getConfiguration(),
        Optional.<Partition> absent())) {
      copyEntities.add(builder.fileSet(fileSet).build());
    }

    multiTimer.close();
    return copyEntities;
  }
}
