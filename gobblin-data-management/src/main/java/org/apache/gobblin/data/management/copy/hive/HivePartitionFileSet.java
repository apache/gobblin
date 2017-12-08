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

package org.apache.gobblin.data.management.copy.hive;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.entities.PostPublishStep;
import org.apache.gobblin.data.management.copy.entities.PrePublishStep;
import org.apache.gobblin.hive.HiveRegisterStep;
import org.apache.gobblin.hive.metastore.HiveMetaStoreUtils;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.hive.spec.SimpleHiveSpec;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.MultiTimingEvent;
import org.apache.gobblin.util.commit.DeleteFileCommitStep;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link HiveFileSet} for Hive partitions. Creates {@link CopyEntity}s for a single Hive partition.
 */
@Getter
@Slf4j
public class HivePartitionFileSet extends HiveFileSet {

  private HiveCopyEntityHelper hiveCopyEntityHelper;
  private final Partition partition;
  private final Properties properties;
  private Optional<Partition> existingTargetPartition;
  private final EventSubmitter eventSubmitter;

  public HivePartitionFileSet(HiveCopyEntityHelper hiveCopyEntityHelper, Partition partition, Properties properties) {
    super(partition.getCompleteName(), hiveCopyEntityHelper.getDataset());
    this.hiveCopyEntityHelper = hiveCopyEntityHelper;
    this.partition = partition;
    this.properties = properties;
    this.existingTargetPartition =
        Optional.fromNullable(this.hiveCopyEntityHelper.getTargetPartitions().get(this.partition.getValues()));
    this.eventSubmitter =
        new EventSubmitter.Builder(this.hiveCopyEntityHelper.getDataset().getMetricContext(), "hive.dataset.copy")
            .addMetadata("Partition", this.partition.getName()).build();
  }

  @Override
  protected Collection<CopyEntity> generateCopyEntities() throws IOException {

    try (Closer closer = Closer.create()) {
      MultiTimingEvent multiTimer = closer.register(new MultiTimingEvent(this.eventSubmitter, "PartitionCopy", true));

      int stepPriority = 0;
      String fileSet = HiveCopyEntityHelper.gson.toJson(this.partition.getValues());

      List<CopyEntity> copyEntities = Lists.newArrayList();

      stepPriority = hiveCopyEntityHelper.addSharedSteps(copyEntities, fileSet, stepPriority);

      multiTimer.nextStage(HiveCopyEntityHelper.Stages.COMPUTE_TARGETS);
      Path targetPath = hiveCopyEntityHelper.getTargetLocation(hiveCopyEntityHelper.getDataset().fs, hiveCopyEntityHelper.getTargetFs(),
          this.partition.getDataLocation(), Optional.of(this.partition));
      Partition targetPartition = getTargetPartition(this.partition, targetPath);

      multiTimer.nextStage(HiveCopyEntityHelper.Stages.EXISTING_PARTITION);
      if (this.existingTargetPartition.isPresent()) {
        hiveCopyEntityHelper.getTargetPartitions().remove(this.partition.getValues());
        try {
          checkPartitionCompatibility(targetPartition, this.existingTargetPartition.get());
        } catch (IOException ioe) {
          if (hiveCopyEntityHelper.getExistingEntityPolicy() != HiveCopyEntityHelper.ExistingEntityPolicy.REPLACE_PARTITIONS) {
            log.error("Source and target partitions are not compatible. Aborting copy of partition " + this.partition,
                ioe);
            return Lists.newArrayList();
          }
          log.warn("Source and target partitions are not compatible. Will override target partition: " + ioe.getMessage());
          log.debug("Incompatibility details: ", ioe);
          stepPriority = hiveCopyEntityHelper.addPartitionDeregisterSteps(copyEntities, fileSet, stepPriority,
              hiveCopyEntityHelper.getTargetTable(), this.existingTargetPartition.get());
          this.existingTargetPartition = Optional.absent();
        }
      }

      multiTimer.nextStage(HiveCopyEntityHelper.Stages.PARTITION_SKIP_PREDICATE);
      if (hiveCopyEntityHelper.getFastPartitionSkip().isPresent()
          && hiveCopyEntityHelper.getFastPartitionSkip().get().apply(this)) {
        log.info(String.format("Skipping copy of partition %s due to fast partition skip predicate.",
            this.partition.getCompleteName()));
        return Lists.newArrayList();
      }

      HiveSpec partitionHiveSpec = new SimpleHiveSpec.Builder<>(targetPath)
          .withTable(HiveMetaStoreUtils.getHiveTable(hiveCopyEntityHelper.getTargetTable().getTTable()))
          .withPartition(Optional.of(HiveMetaStoreUtils.getHivePartition(targetPartition.getTPartition()))).build();
      HiveRegisterStep register = new HiveRegisterStep(hiveCopyEntityHelper.getTargetURI(), partitionHiveSpec,
          hiveCopyEntityHelper.getHiveRegProps());
      copyEntities.add(new PostPublishStep(fileSet, Maps.<String, String> newHashMap(), register, stepPriority++));

      multiTimer.nextStage(HiveCopyEntityHelper.Stages.CREATE_LOCATIONS);
      HiveLocationDescriptor sourceLocation =
          HiveLocationDescriptor.forPartition(this.partition, hiveCopyEntityHelper.getDataset().fs, this.properties);
      HiveLocationDescriptor desiredTargetLocation =
          HiveLocationDescriptor.forPartition(targetPartition, hiveCopyEntityHelper.getTargetFs(), this.properties);
      Optional<HiveLocationDescriptor> existingTargetLocation = this.existingTargetPartition.isPresent()
          ? Optional.of(HiveLocationDescriptor.forPartition(this.existingTargetPartition.get(),
              hiveCopyEntityHelper.getTargetFs(), this.properties))
          : Optional.<HiveLocationDescriptor> absent();

      multiTimer.nextStage(HiveCopyEntityHelper.Stages.FULL_PATH_DIFF);
      HiveCopyEntityHelper.DiffPathSet
          diffPathSet = HiveCopyEntityHelper.fullPathDiff(sourceLocation, desiredTargetLocation, existingTargetLocation,
          Optional.<Partition> absent(), multiTimer, hiveCopyEntityHelper);

      multiTimer.nextStage(HiveCopyEntityHelper.Stages.CREATE_DELETE_UNITS);
      if (diffPathSet.pathsToDelete.size() > 0) {
        DeleteFileCommitStep deleteStep = DeleteFileCommitStep.fromPaths(hiveCopyEntityHelper.getTargetFs(),
            diffPathSet.pathsToDelete, hiveCopyEntityHelper.getDataset().properties);
        copyEntities.add(new PrePublishStep(fileSet, Maps.<String, String> newHashMap(), deleteStep, stepPriority++));
      }

      multiTimer.nextStage(HiveCopyEntityHelper.Stages.CREATE_COPY_UNITS);
      for (CopyableFile.Builder builder : hiveCopyEntityHelper.getCopyableFilesFromPaths(diffPathSet.filesToCopy,
          hiveCopyEntityHelper.getConfiguration(), Optional.of(this.partition))) {
        CopyableFile fileEntity =
            builder.fileSet(fileSet).checksum(new byte[0]).datasetOutputPath(desiredTargetLocation.location.toString())
                .build();
        this.hiveCopyEntityHelper.setCopyableFileDatasets(fileEntity);
        copyEntities.add(fileEntity);
      }

      log.info("Created {} copy entities for partition {}", copyEntities.size(), this.partition.getCompleteName());

      return copyEntities;
    }
  }

  private Partition getTargetPartition(Partition originPartition, Path targetLocation) throws IOException {
    try {
      Partition targetPartition = new Partition(this.hiveCopyEntityHelper.getTargetTable(), originPartition.getTPartition().deepCopy());
      targetPartition.getTable().setDbName(this.hiveCopyEntityHelper.getTargetDatabase());
      targetPartition.getTPartition().setDbName(this.hiveCopyEntityHelper.getTargetDatabase());
      targetPartition.getTPartition().putToParameters(HiveDataset.REGISTERER, HiveCopyEntityHelper.GOBBLIN_DISTCP);
      targetPartition.getTPartition().putToParameters(HiveDataset.REGISTRATION_GENERATION_TIME_MILLIS,
          Long.toString(this.hiveCopyEntityHelper.getStartTime()));
      targetPartition.setLocation(targetLocation.toString());
      targetPartition.getTPartition().unsetCreateTime();
      return targetPartition;
    } catch (HiveException he) {
      throw new IOException(he);
    }
  }

  private static void checkPartitionCompatibility(Partition desiredTargetPartition, Partition existingTargetPartition)
      throws IOException {
    if (!desiredTargetPartition.getDataLocation().equals(existingTargetPartition.getDataLocation())) {
      throw new IOException(
          String.format("Desired target location %s and already registered target location %s do not agree.",
              desiredTargetPartition.getDataLocation(), existingTargetPartition.getDataLocation()));
    }
  }
}
