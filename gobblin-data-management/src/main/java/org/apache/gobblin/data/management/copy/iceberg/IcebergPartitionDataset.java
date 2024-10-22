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

package org.apache.gobblin.data.management.copy.iceberg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.util.SerializationUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.entities.PostPublishStep;
import org.apache.gobblin.data.management.copy.CopyableDataset;
import org.apache.gobblin.util.measurement.GrowthMilestoneTracker;
import org.apache.gobblin.data.management.copy.iceberg.predicates.IcebergMatchesAnyPropNamePartitionFilterPredicate;
import org.apache.gobblin.data.management.copy.iceberg.predicates.IcebergPartitionFilterPredicateUtil;

/**
 * Iceberg Partition dataset implementing {@link CopyableDataset}
 * <p>
 * This class extends {@link IcebergDataset} and provides functionality to filter partitions
 * and generate copy entities for partition based data movement.
 * </p>
 */
@Slf4j
public class IcebergPartitionDataset extends IcebergDataset {
  // Currently hardcoded these transforms here but eventually it will depend on filter predicate implementation and can
  // be moved to a common place or inside each filter predicate.
  private static final List<String> supportedTransforms = ImmutableList.of("identity", "truncate");
  private final Predicate<StructLike> partitionFilterPredicate;
  private final String partitionColumnName;
  private final String partitionColValue;

  public IcebergPartitionDataset(IcebergTable srcIcebergTable, IcebergTable destIcebergTable, Properties properties,
      FileSystem sourceFs, boolean shouldIncludeMetadataPath, String partitionColumnName, String partitionColValue)
      throws IOException {
    super(srcIcebergTable, destIcebergTable, properties, sourceFs, shouldIncludeMetadataPath);
    this.partitionColumnName = partitionColumnName;
    this.partitionColValue = partitionColValue;
    this.partitionFilterPredicate = createPartitionFilterPredicate();
  }

  /**
   * Generates copy entities for partition based data movement.
   * It finds files specific to the partition and create destination data files based on the source data files.
   * Also updates the destination data files with destination table write data location and add UUID to the file path
   * to avoid conflicts.
   *
   * @param targetFs the target file system
   * @param copyConfig the copy configuration
   * @return a collection of copy entities
   * @throws IOException if an I/O error occurs
   */
  @Override
  Collection<CopyEntity> generateCopyEntities(FileSystem targetFs, CopyConfiguration copyConfig) throws IOException {
    // TODO: Refactor the IcebergDataset::generateCopyEntities to avoid code duplication
    //  Differences are getting data files, copying ancestor permission and adding post publish steps
    String fileSet = this.getFileSetId();
    List<CopyEntity> copyEntities = Lists.newArrayList();
    IcebergTable srcIcebergTable = getSrcIcebergTable();
    List<DataFile> srcDataFiles = srcIcebergTable.getPartitionSpecificDataFiles(this.partitionFilterPredicate);
    Map<Path, DataFile> destDataFileBySrcPath = calcDestDataFileBySrcPath(srcDataFiles);
    Configuration defaultHadoopConfiguration = new Configuration();

    for (Map.Entry<Path, FileStatus> entry : calcSrcFileStatusByDestFilePath(destDataFileBySrcPath).entrySet()) {
      Path destPath = entry.getKey();
      FileStatus srcFileStatus = entry.getValue();
      // TODO: should be the same FS each time; try creating once, reusing thereafter, to not recreate wastefully
      FileSystem actualSourceFs = getSourceFileSystemFromFileStatus(srcFileStatus, defaultHadoopConfiguration);

      CopyableFile fileEntity = CopyableFile.fromOriginAndDestination(
              actualSourceFs, srcFileStatus, targetFs.makeQualified(destPath), copyConfig)
          .fileSet(fileSet)
          .datasetOutputPath(targetFs.getUri().getPath())
          .build();

      fileEntity.setSourceData(getSourceDataset(this.sourceFs));
      fileEntity.setDestinationData(getDestinationDataset(targetFs));
      copyEntities.add(fileEntity);
    }

    // Adding this check to avoid adding post publish step when there are no files to copy.
    List<DataFile> destDataFiles = new ArrayList<>(destDataFileBySrcPath.values());
    if (CollectionUtils.isNotEmpty(destDataFiles)) {
      copyEntities.add(createOverwritePostPublishStep(destDataFiles));
    }

    log.info("~{}~ generated {} copy--entities", fileSet, copyEntities.size());
    return copyEntities;
  }

  private Map<Path, DataFile> calcDestDataFileBySrcPath(List<DataFile> srcDataFiles)
      throws IcebergTable.TableNotFoundException {
    String fileSet = this.getFileSetId();
    Map<Path, DataFile> destDataFileBySrcPath = Maps.newHashMap();
    if (srcDataFiles.isEmpty()) {
      log.warn("~{}~ found no data files for partition col : {} with partition value : {} to copy", fileSet,
          this.partitionColumnName, this.partitionColValue);
      return destDataFileBySrcPath;
    }
    TableMetadata srcTableMetadata = getSrcIcebergTable().accessTableMetadata();
    TableMetadata destTableMetadata = getDestIcebergTable().accessTableMetadata();
    PartitionSpec partitionSpec = destTableMetadata.spec();
    // tableMetadata.property(TableProperties.WRITE_DATA_LOCATION, "") returns null if the property is not set and
    // doesn't respect passed default value, so to avoid NPE in .replace() we are setting it to empty string.
    String srcWriteDataLocation = Optional.ofNullable(srcTableMetadata.property(TableProperties.WRITE_DATA_LOCATION,
        "")).orElse("");
    String destWriteDataLocation = Optional.ofNullable(destTableMetadata.property(TableProperties.WRITE_DATA_LOCATION,
        "")).orElse("");
    if (StringUtils.isEmpty(srcWriteDataLocation) || StringUtils.isEmpty(destWriteDataLocation)) {
      log.warn(
          "~{}~ Either source or destination table does not have write data location : source table write data location : {} , destination table write data location : {}",
          fileSet,
          srcWriteDataLocation,
          destWriteDataLocation
      );
    }
    GrowthMilestoneTracker growthMilestoneTracker = new GrowthMilestoneTracker();
    srcDataFiles.forEach(dataFile -> {
      String srcFilePath = dataFile.path().toString();
      Path updatedDestFilePath = relocateDestPath(srcFilePath, srcWriteDataLocation, destWriteDataLocation);
      log.debug("~{}~ Path changed from Src : {} to Dest : {}", fileSet, srcFilePath, updatedDestFilePath);
      destDataFileBySrcPath.put(new Path(srcFilePath), DataFiles.builder(partitionSpec)
          .copy(dataFile)
          .withPath(updatedDestFilePath.toString())
          .build());
      if (growthMilestoneTracker.isAnotherMilestone(destDataFileBySrcPath.size())) {
        log.info("~{}~ created {} destination data files", fileSet, destDataFileBySrcPath.size());
      }
    });
    log.info("~{}~ created {} destination data files", fileSet, destDataFileBySrcPath.size());
    return destDataFileBySrcPath;
  }

  private Path relocateDestPath(String curPathStr, String prefixToBeReplaced, String prefixToReplaceWith) {
    String updPathStr = curPathStr.replace(prefixToBeReplaced, prefixToReplaceWith);
    return addUUIDToPath(updPathStr);
  }

  private Path addUUIDToPath(String filePathStr) {
    Path filePath = new Path(filePathStr);
    String fileDir = filePath.getParent().toString();
    String fileName = filePath.getName();
    String newFileName = String.join("-",UUID.randomUUID().toString(), fileName);
    return new Path(fileDir, newFileName);
  }

  private Map<Path, FileStatus> calcSrcFileStatusByDestFilePath(Map<Path, DataFile> destDataFileBySrcPath) {
    Map<Path, FileStatus> srcFileStatusByDestFilePath = Maps.newHashMap();
    destDataFileBySrcPath.forEach((srcPath, destDataFile) -> {
      FileStatus srcFileStatus;
      try {
        srcFileStatus = this.sourceFs.getFileStatus(srcPath);
      } catch (IOException e) {
        String errMsg = String.format("~%s~ Failed to get file status for path : %s", this.getFileSetId(), srcPath);
        log.error(errMsg);
        throw new RuntimeException(errMsg, e);
      }
      srcFileStatusByDestFilePath.put(new Path(destDataFile.path().toString()), srcFileStatus);
    });
    return srcFileStatusByDestFilePath;
  }

  private PostPublishStep createOverwritePostPublishStep(List<DataFile> destDataFiles) {
    byte[] serializedDataFiles = SerializationUtil.serializeToBytes(destDataFiles);

    IcebergOverwritePartitionsStep icebergOverwritePartitionStep = new IcebergOverwritePartitionsStep(
        this.getDestIcebergTable().getTableId().toString(),
        this.partitionColumnName,
        this.partitionColValue,
        serializedDataFiles,
        this.properties
    );

    return new PostPublishStep(this.getFileSetId(), Maps.newHashMap(), icebergOverwritePartitionStep, 0);
  }

  private Predicate<StructLike> createPartitionFilterPredicate() throws IOException {
    //TODO: Refactor it later using factory or other way to support different types of filter predicate
    // Also take into consideration creation of Expression Filter to be used in overwrite api
    TableMetadata srcTableMetadata = getSrcIcebergTable().accessTableMetadata();
    Optional<Integer> partitionColumnIndexOpt = IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex(
        this.partitionColumnName,
        srcTableMetadata,
        supportedTransforms
    );
    Preconditions.checkArgument(partitionColumnIndexOpt.isPresent(), String.format(
        "Partition column %s not found in table %s",
        this.partitionColumnName, this.getFileSetId()));
    int partitionColumnIndex = partitionColumnIndexOpt.get();
    return new IcebergMatchesAnyPropNamePartitionFilterPredicate(partitionColumnIndex, this.partitionColValue);
  }

}