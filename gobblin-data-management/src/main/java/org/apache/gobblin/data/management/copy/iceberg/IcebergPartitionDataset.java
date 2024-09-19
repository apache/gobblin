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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
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
import org.apache.gobblin.data.management.copy.iceberg.predicates.IcebergPartitionFilterPredicate;
import org.apache.gobblin.data.management.copy.iceberg.predicates.IcebergDateTimePartitionFilterPredicate;


@Slf4j
public class IcebergPartitionDataset extends IcebergDataset {

  private static final String ICEBERG_PARTITION_NAME_KEY = "partition.name";
  private static final String ICEBERG_PARTITION_TYPE_KEY = "partition.type";
  private static final String DATETIME_PARTITION_TYPE = "datetime";
  private Predicate<StructLike> partitionFilterPredicate;

  public IcebergPartitionDataset(IcebergTable srcIcebergTable, IcebergTable destIcebergTable, Properties properties,
      FileSystem sourceFs, boolean shouldIncludeMetadataPath) throws IcebergTable.TableNotFoundException {
    super(srcIcebergTable, destIcebergTable, properties, sourceFs, shouldIncludeMetadataPath);

    initializePartitionFilterPredicate();
  }

  private void initializePartitionFilterPredicate() throws IcebergTable.TableNotFoundException {
    //TODO: Move this to a factory class of some sort
    String partitionColumnName =
        IcebergDatasetFinder.getLocationQualifiedProperty(properties, IcebergDatasetFinder.CatalogLocation.SOURCE,
            ICEBERG_PARTITION_NAME_KEY);
    Preconditions.checkArgument(StringUtils.isNotEmpty(partitionColumnName),
        "Partition column name cannot be empty");

    TableMetadata srcTableMetadata = getSrcIcebergTable().accessTableMetadata();

    if (DATETIME_PARTITION_TYPE.equals(IcebergDatasetFinder.getLocationQualifiedProperty(properties,
        IcebergDatasetFinder.CatalogLocation.SOURCE, ICEBERG_PARTITION_TYPE_KEY))) {
      this.partitionFilterPredicate = new IcebergDateTimePartitionFilterPredicate(partitionColumnName,
          srcTableMetadata, properties);
    } else {
      this.partitionFilterPredicate = new IcebergPartitionFilterPredicate(partitionColumnName,
          srcTableMetadata, properties);
    }
  }

  @Data
  protected static final class FilePathsWithStatus {
    private final Path srcPath;
    private final Path destPath;
    private final FileStatus srcFileStatus;
  }

  @Override
  Collection<CopyEntity> generateCopyEntities(FileSystem targetFs, CopyConfiguration copyConfig) throws IOException {
    String fileSet = this.getFileSetId();
    List<CopyEntity> copyEntities = Lists.newArrayList();
    IcebergTable srcIcebergTable = getSrcIcebergTable();
//    List<IcebergDataFileInfo> dataFileInfos = srcIcebergTable.getPartitionSpecificDataFiles(this.partitionFilterPredicate);
    List<DataFile> srcDataFiles = srcIcebergTable.getPartitionSpecificDataFiles(this.partitionFilterPredicate);
    List<DataFile> destDataFiles = getDestDataFiles(srcDataFiles);
//    log.info("Data File Infos - 0 : {}", dataFileInfos);
//    fixDestFilePaths(dataFileInfos);
//    log.info("Data File Infos - 1 : {}", dataFileInfos);
    Configuration defaultHadoopConfiguration = new Configuration();

    int cnt = 0;

    for (FilePathsWithStatus filePathsWithStatus : getFilePathsStatus(srcDataFiles, destDataFiles, this.sourceFs)) {
      Path srcPath = filePathsWithStatus.getSrcPath();
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

      //TODO: Remove this logging later
      log.info("Iteration : {}", cnt++);
      log.info("Source Path : {}", srcPath);
      log.info("Destination Path : {}", destPath);
      log.info("Actual Source FileSystem : {}", actualSourceFs.toString());
      log.info("Src Path Parent : {}", srcPath.getParent());
      log.info("Src File Status : {}", srcFileStatus);
      log.info("Destination : {}", targetFs.makeQualified(destPath));
      log.info("Dataset Output Path : {}", targetFs.getUri().getPath());
      log.info("Source Dataset : {}", getSourceDataset(this.sourceFs).toString());
      log.info("Destination Dataset : {}", getDestinationDataset(targetFs).toString());

    }

    copyEntities.add(createPostPublishStep(destDataFiles));

    log.info("~{}~ generated {} copy--entities", fileSet, copyEntities.size());
    log.info("Copy Entities : {}", copyEntities);

    return copyEntities;
  }

//  private void fixDestFilePaths(List<IcebergDataFileInfo> dataFileInfos) throws IOException {
//    String prefixToBeReplaced = getSrcIcebergTable().accessTableMetadata().property(TableProperties.WRITE_DATA_LOCATION, "");
//    String prefixToReplaceWith = getDestIcebergTable().accessTableMetadata().property(TableProperties.WRITE_DATA_LOCATION, "");
//    if (StringUtils.isEmpty(prefixToBeReplaced) || StringUtils.isEmpty(prefixToReplaceWith)) {
//      log.warn(
//          String.format("Cannot fix dest file paths as either source or destination table does not have write data location : "
//              + "source table write data location : {%s} , destination table write data location : {%s}",
//              prefixToBeReplaced,
//              prefixToReplaceWith
//          )
//      );
//      return;
//    }
//    for (IcebergDataFileInfo dataFileInfo : dataFileInfos) {
//      String curDestFilePath = dataFileInfo.getDestFilePath();
//      String newDestFilePath = curDestFilePath.replace(prefixToBeReplaced, prefixToReplaceWith);
//      dataFileInfo.setDestFilePath(newDestFilePath);
//    }
//  }

  private List<DataFile> getDestDataFiles(List<DataFile> srcDataFiles) throws IcebergTable.TableNotFoundException {
    TableMetadata srcTableMetadata = getSrcIcebergTable().accessTableMetadata();
    TableMetadata destTableMetadata = getDestIcebergTable().accessTableMetadata();
    String prefixToBeReplaced = srcTableMetadata.property(TableProperties.WRITE_DATA_LOCATION, "");
    String prefixToReplaceWith = destTableMetadata.property(TableProperties.WRITE_DATA_LOCATION, "");
    if (StringUtils.isEmpty(prefixToBeReplaced) || StringUtils.isEmpty(prefixToReplaceWith)) {
      log.warn(
          String.format("Either source or destination table does not have write data location : source table write data location : {%s} , destination table write data location : {%s}",
              prefixToBeReplaced,
              prefixToReplaceWith
          )
      );
    }
    List<DataFile> destDataFiles = new ArrayList<>();
    srcDataFiles.forEach(dataFile -> {
      String curDestFilePath = dataFile.path().toString();
      String newDestFilePath = curDestFilePath.replace(prefixToBeReplaced, prefixToReplaceWith);
      String updatedDestFilePath = addUUIDToPath(newDestFilePath);
      destDataFiles.add(DataFiles.builder(destTableMetadata.spec())
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
    Path newFilePath = new Path(fileDir, newFileName);
    return newFilePath.toString();
  }

  private List<FilePathsWithStatus> getFilePathsStatus(List<DataFile> srcDataFiles, List<DataFile> destDataFiles, FileSystem fs) throws IOException {
    List<FilePathsWithStatus> filePathsStatus = new ArrayList<>();
    for (int i = 0; i < srcDataFiles.size(); i++) {
      Path srcPath = new Path(srcDataFiles.get(i).path().toString());
      Path destPath = new Path(destDataFiles.get(i).toString());
      FileStatus srcFileStatus = fs.getFileStatus(srcPath);
      filePathsStatus.add(new FilePathsWithStatus(srcPath, destPath, srcFileStatus));
    }
    return filePathsStatus;
  }

//  private List<FilePathsWithStatus> getFilePathsStatus(List<IcebergDataFileInfo> dataFileInfos, FileSystem fs) throws IOException {
//    List<FilePathsWithStatus> filePathsStatus = new ArrayList<>();
//    for (IcebergDataFileInfo dataFileInfo : dataFileInfos) {
//      Path srcPath = new Path(dataFileInfo.getSrcFilePath());
//      Path destPath = new Path(dataFileInfo.getDestFilePath());
//      FileStatus arcFileStatus = fs.getFileStatus(srcPath);
//      filePathsStatus.add(new FilePathsWithStatus(srcPath, destPath, arcFileStatus));
//    }
//    return filePathsStatus;
//  }

  private PostPublishStep createPostPublishStep(List<DataFile> destDataFiles) {
//    IcebergTable destIcebergTable = getDestIcebergTable();
//    PartitionSpec partitionSpec = destIcebergTable.accessTableMetadata().spec();
//    List<DataFile> dataFiles = fileInfos.stream()
//        .map(dataFileInfo -> dataFileInfo.getDataFile(partitionSpec))
//        .collect(Collectors.toList());

    byte[] serializedDataFiles = SerializationUtil.serializeToBytes(destDataFiles);

    IcebergReplacePartitionsStep icebergReplacePartitionsStep = new IcebergReplacePartitionsStep(
        this.getDestIcebergTable().getTableId().toString(),
        serializedDataFiles,
        this.properties);

    return new PostPublishStep(this.getFileSetId(), Maps.newHashMap(), icebergReplacePartitionsStep, 0);
  }

}
