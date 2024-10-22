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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopyContext;
import org.apache.gobblin.data.management.copy.PreserveAttributes;
import org.apache.gobblin.data.management.copy.iceberg.predicates.IcebergPartitionFilterPredicateUtil;
import org.apache.gobblin.dataset.DatasetDescriptor;

import static org.mockito.ArgumentMatchers.any;


/** Tests for {@link org.apache.gobblin.data.management.copy.iceberg.IcebergPartitionDataset} */
public class IcebergPartitionDatasetTest {
  private IcebergTable srcIcebergTable;
  private IcebergTable destIcebergTable;
  private TableMetadata srcTableMetadata;
  private TableMetadata destTableMetadata;
  private static FileSystem sourceFs;
  private static FileSystem targetFs;
  private IcebergPartitionDataset icebergPartitionDataset;
  private MockedStatic<IcebergPartitionFilterPredicateUtil> icebergPartitionFilterPredicateUtil;
  private static final String SRC_TEST_DB = "srcTestDB";
  private static final String SRC_TEST_TABLE = "srcTestTable";
  private static final String SRC_WRITE_LOCATION = SRC_TEST_DB + "/" + SRC_TEST_TABLE + "/data";
  private static final String DEST_TEST_DB = "destTestDB";
  private static final String DEST_TEST_TABLE = "destTestTable";
  private static final String DEST_WRITE_LOCATION = DEST_TEST_DB + "/" + DEST_TEST_TABLE + "/data";
  private static final String TEST_ICEBERG_PARTITION_COLUMN_NAME = "testPartition";
  private static final String TEST_ICEBERG_PARTITION_COLUMN_VALUE = "testValue";
  private static final String OVERWRITE_COMMIT_STEP = "org.apache.gobblin.data.management.copy.iceberg.IcebergOverwritePartitionsStep";
  private final Properties copyConfigProperties = new Properties();
  private final Properties properties = new Properties();
  private static final List<String> srcFilePaths = new ArrayList<>();

  private static final URI SRC_FS_URI;
  private static final URI DEST_FS_URI;

  static {
    try {
      SRC_FS_URI = new URI("abc", "the.source.org", "/", null);
      DEST_FS_URI = new URI("xyz", "the.dest.org", "/", null);
    } catch (URISyntaxException e) {
      throw new RuntimeException("should not occur!", e);
    }
  }

  @BeforeMethod
  public void setUp() throws Exception {
    setupSrcFileSystem();
    setupDestFileSystem();

    TableIdentifier tableIdentifier = TableIdentifier.of(SRC_TEST_DB, SRC_TEST_TABLE);

    srcIcebergTable = Mockito.mock(IcebergTable.class);
    destIcebergTable = Mockito.mock(IcebergTable.class);

    srcTableMetadata = Mockito.mock(TableMetadata.class);
    destTableMetadata = Mockito.mock(TableMetadata.class);
    Mockito.when(destTableMetadata.spec()).thenReturn(Mockito.mock(PartitionSpec.class));

    Mockito.when(srcIcebergTable.getTableId()).thenReturn(tableIdentifier);
    Mockito.when(destIcebergTable.getTableId()).thenReturn(tableIdentifier);
    Mockito.when(srcIcebergTable.accessTableMetadata()).thenReturn(srcTableMetadata);
    Mockito.when(destIcebergTable.accessTableMetadata()).thenReturn(destTableMetadata);
    Mockito.when(srcIcebergTable.getDatasetDescriptor(Mockito.any())).thenReturn(Mockito.mock(DatasetDescriptor.class));
    Mockito.when(destIcebergTable.getDatasetDescriptor(Mockito.any())).thenReturn(Mockito.mock(DatasetDescriptor.class));

    icebergPartitionFilterPredicateUtil = Mockito.mockStatic(IcebergPartitionFilterPredicateUtil.class);
    icebergPartitionFilterPredicateUtil
        .when(() -> IcebergPartitionFilterPredicateUtil.getPartitionColumnIndex(Mockito.anyString(), Mockito.any(), Mockito.any()))
        .thenReturn(Optional.of(0));

    copyConfigProperties.setProperty("data.publisher.final.dir", "/test");
  }

  @AfterMethod
  public void cleanUp() {
    srcFilePaths.clear();
    icebergPartitionFilterPredicateUtil.close();
  }

  @Test
  public void testGenerateCopyEntities() throws IOException {
    srcFilePaths.add(SRC_WRITE_LOCATION + "/file1.orc");
    List<DataFile> mockSrcDataFiles = createDataFileMocks();
    Mockito.when(srcIcebergTable.getPartitionSpecificDataFiles(Mockito.any())).thenReturn(mockSrcDataFiles);

    icebergPartitionDataset = new TestIcebergPartitionDataset(srcIcebergTable, destIcebergTable, properties, sourceFs,
        true);

    CopyConfiguration copyConfiguration =
        CopyConfiguration.builder(targetFs, copyConfigProperties).preserve(PreserveAttributes.fromMnemonicString(""))
            .copyContext(new CopyContext()).build();

    Collection<CopyEntity> copyEntities = icebergPartitionDataset.generateCopyEntities(targetFs, copyConfiguration);

    verifyCopyEntities(copyEntities, 2, true);
  }

  @Test
  public void testGenerateCopyEntitiesWithEmptyDataFiles() throws IOException {
    List<DataFile> srcDataFiles = Lists.newArrayList();
    Mockito.when(srcIcebergTable.getPartitionSpecificDataFiles(Mockito.any())).thenReturn(srcDataFiles);

    icebergPartitionDataset = new IcebergPartitionDataset(srcIcebergTable, destIcebergTable, properties, sourceFs,
        true, TEST_ICEBERG_PARTITION_COLUMN_NAME, TEST_ICEBERG_PARTITION_COLUMN_VALUE);
    Collection<CopyEntity> copyEntities = icebergPartitionDataset.generateCopyEntities(targetFs,
        Mockito.mock(CopyConfiguration.class));

    // Since No data files are present, no copy entities should be generated
    verifyCopyEntities(copyEntities, 0, true);
  }

  @Test
  public void testMultipleCopyEntitiesGenerated() throws IOException {
    srcFilePaths.add(SRC_WRITE_LOCATION + "/file1.orc");
    srcFilePaths.add(SRC_WRITE_LOCATION + "/file2.orc");
    srcFilePaths.add(SRC_WRITE_LOCATION + "/file3.orc");
    srcFilePaths.add(SRC_WRITE_LOCATION + "/file4.orc");
    srcFilePaths.add(SRC_WRITE_LOCATION + "/file5.orc");

    List<DataFile> mockSrcDataFiles = createDataFileMocks();
    Mockito.when(srcIcebergTable.getPartitionSpecificDataFiles(Mockito.any())).thenReturn(mockSrcDataFiles);

    icebergPartitionDataset = new TestIcebergPartitionDataset(srcIcebergTable, destIcebergTable, properties, sourceFs,
        true);

    CopyConfiguration copyConfiguration =
        CopyConfiguration.builder(targetFs, copyConfigProperties).preserve(PreserveAttributes.fromMnemonicString(""))
            .copyContext(new CopyContext()).build();

    Collection<CopyEntity> copyEntities = icebergPartitionDataset.generateCopyEntities(targetFs, copyConfiguration);

    verifyCopyEntities(copyEntities, 6, true);
  }

  @Test
  public void testWithDifferentSrcAndDestTableWriteLocation() throws IOException {
    srcFilePaths.add(SRC_WRITE_LOCATION + "/randomFile--Name.orc");
    Mockito.when(srcTableMetadata.property(TableProperties.WRITE_DATA_LOCATION, "")).thenReturn(SRC_WRITE_LOCATION);
    Mockito.when(destTableMetadata.property(TableProperties.WRITE_DATA_LOCATION, "")).thenReturn(DEST_WRITE_LOCATION);

    List<DataFile> mockSrcDataFiles = createDataFileMocks();
    Mockito.when(srcIcebergTable.getPartitionSpecificDataFiles(Mockito.any())).thenReturn(mockSrcDataFiles);

    icebergPartitionDataset = new TestIcebergPartitionDataset(srcIcebergTable, destIcebergTable, properties, sourceFs,
        true);

    CopyConfiguration copyConfiguration =
        CopyConfiguration.builder(targetFs, copyConfigProperties).preserve(PreserveAttributes.fromMnemonicString(""))
            .copyContext(new CopyContext()).build();

    List<CopyEntity> copyEntities =
        (List<CopyEntity>) icebergPartitionDataset.generateCopyEntities(targetFs, copyConfiguration);

    verifyCopyEntities(copyEntities, 2, false);
  }

  private static void setupSrcFileSystem() throws IOException {
    sourceFs = Mockito.mock(FileSystem.class);
    Mockito.when(sourceFs.getUri()).thenReturn(SRC_FS_URI);
    Mockito.when(sourceFs.makeQualified(any(Path.class)))
        .thenAnswer(invocation -> invocation.getArgument(0, Path.class).makeQualified(SRC_FS_URI, new Path("/")));
    Mockito.when(sourceFs.getFileStatus(any(Path.class))).thenAnswer(invocation -> {
      Path path = invocation.getArgument(0, Path.class);
      Path qualifiedPath = sourceFs.makeQualified(path);
      return IcebergDatasetTest.MockFileSystemBuilder.createEmptyFileStatus(qualifiedPath.toString());
    });
  }

  private static void setupDestFileSystem() throws IOException {
    targetFs = Mockito.mock(FileSystem.class);
    Mockito.when(targetFs.getUri()).thenReturn(DEST_FS_URI);
    Mockito.when(targetFs.makeQualified(any(Path.class)))
        .thenAnswer(invocation -> invocation.getArgument(0, Path.class).makeQualified(DEST_FS_URI, new Path("/")));
    // Since we are adding UUID to the file name for every file while creating destination path,
    // so return file not found exception if trying to find file status on destination file system
    Mockito.when(targetFs.getFileStatus(any(Path.class))).thenThrow(new FileNotFoundException());
  }

  private static List<DataFile> createDataFileMocks() throws IOException {
    List<DataFile> dataFiles = new ArrayList<>();
    for (String srcFilePath : srcFilePaths) {
      DataFile dataFile = Mockito.mock(DataFile.class);
      Path dataFilePath = new Path(srcFilePath);
      Path qualifiedPath = sourceFs.makeQualified(dataFilePath);
      Mockito.when(dataFile.path()).thenReturn(dataFilePath.toString());
      Mockito.when(sourceFs.getFileStatus(Mockito.eq(dataFilePath))).thenReturn(
          IcebergDatasetTest.MockFileSystemBuilder.createEmptyFileStatus(qualifiedPath.toString()));
      dataFiles.add(dataFile);
    }
    return dataFiles;
  }

  private static void verifyCopyEntities(Collection<CopyEntity> copyEntities, int expectedCopyEntitiesSize,
      boolean sameSrcAndDestWriteLocation) {
    Assert.assertEquals(copyEntities.size(), expectedCopyEntitiesSize);
    String srcWriteLocationStart = SRC_FS_URI + SRC_WRITE_LOCATION;
    String destWriteLocationStart = DEST_FS_URI + (sameSrcAndDestWriteLocation ? SRC_WRITE_LOCATION : DEST_WRITE_LOCATION);
    String srcErrorMsg = String.format("Source Location should start with %s", srcWriteLocationStart);
    String destErrorMsg = String.format("Destination Location should start with %s", destWriteLocationStart);
    for (CopyEntity copyEntity : copyEntities) {
      String json = copyEntity.toString();
      if (IcebergDatasetTest.isCopyableFile(json)) {
        String originFilepath = IcebergDatasetTest.CopyEntityDeserializer.getOriginFilePathAsStringFromJson(json);
        String destFilepath = IcebergDatasetTest.CopyEntityDeserializer.getDestinationFilePathAsStringFromJson(json);
        Assert.assertTrue(originFilepath.startsWith(srcWriteLocationStart), srcErrorMsg);
        Assert.assertTrue(destFilepath.startsWith(destWriteLocationStart), destErrorMsg);
        String originFileName = originFilepath.substring(srcWriteLocationStart.length() + 1);
        String destFileName = destFilepath.substring(destWriteLocationStart.length() + 1);
        Assert.assertTrue(destFileName.endsWith(originFileName), "Incorrect file name in destination path");
        Assert.assertTrue(destFileName.length() > originFileName.length() + 1,
            "Destination file name should be longer than source file name as UUID is appended");
      } else{
        IcebergDatasetTest.verifyPostPublishStep(json, OVERWRITE_COMMIT_STEP);
      }
    }
  }

  /**
   * See {@link org.apache.gobblin.data.management.copy.iceberg.IcebergDatasetTest.TrickIcebergDataset}
   * */
  protected static class TestIcebergPartitionDataset extends IcebergPartitionDataset {

    public TestIcebergPartitionDataset(IcebergTable srcIcebergTable, IcebergTable destIcebergTable,
        Properties properties, FileSystem sourceFs, boolean shouldIncludeMetadataPath) throws IOException {
      super(srcIcebergTable, destIcebergTable, properties, sourceFs, shouldIncludeMetadataPath,
          TEST_ICEBERG_PARTITION_COLUMN_NAME, TEST_ICEBERG_PARTITION_COLUMN_VALUE);
    }

    @Override
    protected FileSystem getSourceFileSystemFromFileStatus(FileStatus fileStatus, Configuration hadoopConfig) {
      return this.sourceFs;
    }
  }
}
