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
import com.google.common.base.Preconditions;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.entities.PostPublishStep;
import org.apache.gobblin.data.management.copy.CopyableDataset;
import org.apache.gobblin.data.management.copy.iceberg.predicates.IcebergPartitionFilterPredicateFactory;

/**
 * Iceberg Partition dataset implementing {@link CopyableDataset}
 * <p>
 * This class extends {@link IcebergDataset} and provides functionality to filter partitions
 * and generate copy entities for partition based data movement.
 * </p>
 */
@Slf4j
public class IcebergPartitionDataset extends IcebergDataset {

  private static final String ICEBERG_PARTITION_NAME_KEY = "partition.name";
  private final Predicate<StructLike> partitionFilterPredicate;

  public IcebergPartitionDataset(IcebergTable srcIcebergTable, IcebergTable destIcebergTable, Properties properties,
      FileSystem sourceFs, boolean shouldIncludeMetadataPath) throws IcebergTable.TableNotFoundException {
    super(srcIcebergTable, destIcebergTable, properties, sourceFs, shouldIncludeMetadataPath);

    String partitionColumnName =
        IcebergDatasetFinder.getLocationQualifiedProperty(properties, IcebergDatasetFinder.CatalogLocation.SOURCE,
            ICEBERG_PARTITION_NAME_KEY);
    Preconditions.checkArgument(StringUtils.isNotEmpty(partitionColumnName),
        "Partition column name cannot be empty");

    TableMetadata srcTableMetadata = getSrcIcebergTable().accessTableMetadata();
    this.partitionFilterPredicate = IcebergPartitionFilterPredicateFactory.getFilterPredicate(partitionColumnName,
        srcTableMetadata, properties);
  }

  /**
   * Represents the destination file paths and the corresponding file status in source file system.
   * These both properties are used in creating {@link CopyEntity}
   */
  @Data
  protected static final class FilePathsWithStatus {
    private final Path destPath;
    private final FileStatus srcFileStatus;
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
    String fileSet = this.getFileSetId();
    List<CopyEntity> copyEntities = Lists.newArrayList();
    IcebergTable srcIcebergTable = getSrcIcebergTable();
    List<DataFile> srcDataFiles = srcIcebergTable.getPartitionSpecificDataFiles(this.partitionFilterPredicate);
    List<DataFile> destDataFiles = getDestDataFiles(srcDataFiles);
    Configuration defaultHadoopConfiguration = new Configuration();

    for (FilePathsWithStatus filePathsWithStatus : getFilePathsStatus(srcDataFiles, destDataFiles, this.sourceFs)) {
      Path destPath = filePathsWithStatus.getDestPath();
      FileStatus srcFileStatus = filePathsWithStatus.getSrcFileStatus();
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
    if (CollectionUtils.isNotEmpty(destDataFiles)) {
      copyEntities.add(createPostPublishStep(destDataFiles));
    }

    log.info("~{}~ generated {} copy--entities", fileSet, copyEntities.size());
    return copyEntities;
  }

  private List<DataFile> getDestDataFiles(List<DataFile> srcDataFiles) throws IcebergTable.TableNotFoundException {
    List<DataFile> destDataFiles = new ArrayList<>();
    if (srcDataFiles.isEmpty()) {
      return destDataFiles;
    }
    TableMetadata srcTableMetadata = getSrcIcebergTable().accessTableMetadata();
    TableMetadata destTableMetadata = getDestIcebergTable().accessTableMetadata();
    PartitionSpec partitionSpec = destTableMetadata.spec();
    String srcWriteDataLocation = srcTableMetadata.property(TableProperties.WRITE_DATA_LOCATION, "");
    String destWriteDataLocation = destTableMetadata.property(TableProperties.WRITE_DATA_LOCATION, "");
    if (StringUtils.isEmpty(srcWriteDataLocation) || StringUtils.isEmpty(destWriteDataLocation)) {
      log.warn(
          String.format("Either source or destination table does not have write data location : source table write data location : {%s} , destination table write data location : {%s}",
              srcWriteDataLocation,
              destWriteDataLocation
          )
      );
    }
    // tableMetadata.property(TableProperties.WRITE_DATA_LOCATION, "") returns null if the property is not set and
    // doesn't respect passed default value, so to avoid NPE in .replace() we are setting it to empty string.
    String prefixToBeReplaced = (srcWriteDataLocation != null) ? srcWriteDataLocation : "";
    String prefixToReplaceWith = (destWriteDataLocation != null) ? destWriteDataLocation : "";
    srcDataFiles.forEach(dataFile -> {
      String curDestFilePath = dataFile.path().toString();
      String newDestFilePath = curDestFilePath.replace(prefixToBeReplaced, prefixToReplaceWith);
      String updatedDestFilePath = addUUIDToPath(newDestFilePath);
      destDataFiles.add(DataFiles.builder(partitionSpec)
          .copy(dataFile)
          .withPath(updatedDestFilePath)
          .build());
    });
    return destDataFiles;
  }

  private String addUUIDToPath(String filePathStr) {
    Path filePath = new Path(filePathStr);
    String fileDir = filePath.getParent().toString();
    String fileName = filePath.getName();
    String newFileName = UUID.randomUUID() + "-" + fileName;
    return String.join("/", fileDir, newFileName);
  }

  private List<FilePathsWithStatus> getFilePathsStatus(List<DataFile> srcDataFiles, List<DataFile> destDataFiles, FileSystem fs) throws IOException {
    List<FilePathsWithStatus> filePathsStatus = new ArrayList<>();
    for (int i = 0; i < srcDataFiles.size(); i++) {
      Path srcPath = new Path(srcDataFiles.get(i).path().toString());
      Path destPath = new Path(destDataFiles.get(i).path().toString());
      FileStatus srcFileStatus = fs.getFileStatus(srcPath);
      filePathsStatus.add(new FilePathsWithStatus(destPath, srcFileStatus));
    }
    return filePathsStatus;
  }

  private PostPublishStep createPostPublishStep(List<DataFile> destDataFiles) {

    byte[] serializedDataFiles = SerializationUtil.serializeToBytes(destDataFiles);

    IcebergReplacePartitionsStep icebergReplacePartitionsStep = new IcebergReplacePartitionsStep(
        this.getDestIcebergTable().getTableId().toString(),
        serializedDataFiles,
        this.properties);

    return new PostPublishStep(this.getFileSetId(), Maps.newHashMap(), icebergReplacePartitionsStep, 0);
  }

}