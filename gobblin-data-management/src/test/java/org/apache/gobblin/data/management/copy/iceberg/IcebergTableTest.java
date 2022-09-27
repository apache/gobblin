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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.apache.iceberg.shaded.org.apache.avro.SchemaBuilder;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


/** Test {@link org.apache.gobblin.data.management.copy.iceberg.IcebergTable} */
public class IcebergTableTest extends HiveMetastoreTest {

  protected static final org.apache.iceberg.shaded.org.apache.avro.Schema avroDataSchema =
      SchemaBuilder.record("test")
          .fields()
          .name("id")
          .type()
          .longType()
          .noDefault()
          .endRecord();
  protected static final Schema icebergSchema = AvroSchemaUtil.toIceberg(avroDataSchema);
  protected static final PartitionSpec icebergPartitionSpec = PartitionSpec.builderFor(icebergSchema)
      .identity("id")
      .build();

  private final String dbName = "myicebergdb";
  private final String tableName = "justtesting";
  private TableIdentifier tableId;
  private Table table;
  private String metadataBasePath;

  @BeforeClass
  public void setUp() throws Exception {
    startMetastore();
    catalog.createNamespace(Namespace.of(dbName));
  }

  @BeforeMethod
  public void setUpEachTest() {
    tableId = TableIdentifier.of(dbName, tableName);
    table = catalog.createTable(tableId, icebergSchema);
    metadataBasePath = calcMetadataBasePath(tableId);
  }

  @AfterMethod
  public void cleanUpEachTest() {
    catalog.dropTable(tableId);
  }

  /** Verify info about the current snapshot only */
  @Test
  public void testGetCurrentSnapshotInfo() throws IOException {
    List<List<String>> perSnapshotFilesets = Lists.newArrayList(
        Lists.newArrayList("/path/to/data-a0.orc"),
        Lists.newArrayList("/path/to/data-b0.orc", "/path/to/data-b1.orc"),
        Lists.newArrayList("/path/to/data-c0.orc", "/path/to/data-c1.orc", "/path/to/data-c2.orc"),
        Lists.newArrayList("/path/to/data-d0.orc")
    );

    initializeSnapshots(table, perSnapshotFilesets);
    IcebergSnapshotInfo snapshotInfo = new IcebergTable(tableId, catalog.newTableOps(tableId)).getCurrentSnapshotInfo();
    verifySnapshotInfo(snapshotInfo, perSnapshotFilesets, perSnapshotFilesets.size());
  }

  /** Verify failure when attempting to get current snapshot info for non-existent table */
  @Test(expectedExceptions = IcebergTable.TableNotFoundException.class)
  public void testGetCurrentSnapshotInfoOnBogusTable() throws IOException {
    TableIdentifier bogusTableId = TableIdentifier.of(dbName, tableName + "_BOGUS");
    IcebergSnapshotInfo snapshotInfo = new IcebergTable(bogusTableId, catalog.newTableOps(bogusTableId)).getCurrentSnapshotInfo();
    Assert.fail("expected an exception when using table ID '" + bogusTableId + "'");
  }

  /** Verify info about all (full) snapshots */
  @Test
  public void testGetAllSnapshotInfosIterator() throws IOException {
    List<List<String>> perSnapshotFilesets = Lists.newArrayList(
        Lists.newArrayList("/path/to/data-a0.orc"),
        Lists.newArrayList("/path/to/data-b0.orc", "/path/to/data-b1.orc"),
        Lists.newArrayList("/path/to/data-c0.orc", "/path/to/data-c1.orc", "/path/to/data-c2.orc"),
        Lists.newArrayList("/path/to/data-d0.orc")
    );

    initializeSnapshots(table, perSnapshotFilesets);
    List<IcebergSnapshotInfo> snapshotInfos = Lists.newArrayList(new IcebergTable(tableId, catalog.newTableOps(tableId)).getAllSnapshotInfosIterator());
    Assert.assertEquals(snapshotInfos.size(), perSnapshotFilesets.size(), "num snapshots");

    for (int i = 0; i < snapshotInfos.size(); ++i) {
      System.err.println("verifying snapshotInfo[" + i + "]");
      verifySnapshotInfo(snapshotInfos.get(i), perSnapshotFilesets.subList(0, i + 1), snapshotInfos.size());
    }
  }

  /** Verify info about all snapshots (incremental deltas) */
  @Test
  public void testGetIncrementalSnapshotInfosIterator() throws IOException {
    List<List<String>> perSnapshotFilesets = Lists.newArrayList(
        Lists.newArrayList("/path/to/data-a0.orc"),
        Lists.newArrayList("/path/to/data-b0.orc", "/path/to/data-b1.orc"),
        Lists.newArrayList("/path/to/data-c0.orc", "/path/to/data-c1.orc", "/path/to/data-c2.orc"),
        Lists.newArrayList("/path/to/data-d0.orc")
    );

    initializeSnapshots(table, perSnapshotFilesets);
    List<IcebergSnapshotInfo> snapshotInfos = Lists.newArrayList(new IcebergTable(tableId, catalog.newTableOps(tableId)).getIncrementalSnapshotInfosIterator());
    Assert.assertEquals(snapshotInfos.size(), perSnapshotFilesets.size(), "num snapshots");

    for (int i = 0; i < snapshotInfos.size(); ++i) {
      System.err.println("verifying snapshotInfo[" + i + "]");
      verifySnapshotInfo(snapshotInfos.get(i), perSnapshotFilesets.subList(i, i + 1), snapshotInfos.size());
    }
  }

  /** Verify info about all snapshots (incremental deltas) correctly eliminates repeated data files */
  @Test
  public void testGetIncrementalSnapshotInfosIteratorRepeatedFiles() throws IOException {
    List<List<String>> perSnapshotFilesets = Lists.newArrayList(
        Lists.newArrayList("/path/to/data-a0.orc"),
        Lists.newArrayList("/path/to/data-b0.orc", "/path/to/data-b1.orc", "/path/to/data-a0.orc"),
        Lists.newArrayList("/path/to/data-a0.orc","/path/to/data-c0.orc", "/path/to/data-b1.orc", "/path/to/data-c1.orc", "/path/to/data-c2.orc"),
        Lists.newArrayList("/path/to/data-d0.orc")
    );

    initializeSnapshots(table, perSnapshotFilesets);
    List<IcebergSnapshotInfo> snapshotInfos = Lists.newArrayList(new IcebergTable(tableId, catalog.newTableOps(tableId)).getIncrementalSnapshotInfosIterator());
    Assert.assertEquals(snapshotInfos.size(), perSnapshotFilesets.size(), "num snapshots");

    for (int i = 0; i < snapshotInfos.size(); ++i) {
      System.err.println("verifying snapshotInfo[" + i + "] - " + snapshotInfos.get(i));
      char initialChar = (char) ((int) 'a' + i);
      // adjust expectations to eliminate duplicate entries (i.e. those bearing letter not aligned with ordinal fileset)
      List<String> fileset = perSnapshotFilesets.get(i).stream().filter(name -> {
        String uniquePortion = name.substring("/path/to/data-".length());
        return uniquePortion.startsWith(Character.toString(initialChar));
      }).collect(Collectors.toList());
      verifySnapshotInfo(snapshotInfos.get(i), Arrays.asList(fileset), snapshotInfos.size());
    }
  }

  /** full validation for a particular {@link IcebergSnapshotInfo} */
  protected void verifySnapshotInfo(IcebergSnapshotInfo snapshotInfo, List<List<String>> perSnapshotFilesets, int overallNumSnapshots) {
    // verify metadata file
    snapshotInfo.getMetadataPath().ifPresent(metadataPath -> {
          Optional<File> optMetadataFile = extractSomeMetadataFilepath(metadataPath, metadataBasePath, IcebergTableTest::doesResembleMetadataFilename);
          Assert.assertTrue(optMetadataFile.isPresent(), "has metadata filepath");
          verifyMetadataFile(optMetadataFile.get(), Optional.of(overallNumSnapshots));
        }
    );
    // verify manifest list file
    Optional<File> optManifestListFile = extractSomeMetadataFilepath(snapshotInfo.getManifestListPath(), metadataBasePath, IcebergTableTest::doesResembleManifestListFilename);
    Assert.assertTrue(optManifestListFile.isPresent(), "has manifest list filepath");
    verifyManifestListFile(optManifestListFile.get(), Optional.of(snapshotInfo.getSnapshotId()));
    // verify manifest files and their listed data files
    List<IcebergSnapshotInfo.ManifestFileInfo> manifestFileInfos = snapshotInfo.getManifestFiles();
    verifyManifestFiles(manifestFileInfos, snapshotInfo.getManifestFilePaths(), perSnapshotFilesets);
    verifyAnyOrder(snapshotInfo.getAllDataFilePaths(), flatten(perSnapshotFilesets), "data filepaths");
    // verify all aforementioned paths collectively equal `getAllPaths()`
    List<String> allPathsExpected = Lists.newArrayList(snapshotInfo.getManifestListPath());
    snapshotInfo.getMetadataPath().ifPresent(allPathsExpected::add);
    allPathsExpected.addAll(snapshotInfo.getManifestFilePaths());
    allPathsExpected.addAll(snapshotInfo.getAllDataFilePaths());
    verifyAnyOrder(snapshotInfo.getAllPaths(), allPathsExpected, "all paths, metadata and data");
  }

  protected String calcMetadataBasePath(TableIdentifier tableId) {
    return calcMetadataBasePath(tableId.namespace().toString(), tableId.name());
  }

  protected String calcMetadataBasePath(String theDbName, String theTableName) {
    String basePath = String.format("%s/%s/metadata", metastore.getDatabasePath(theDbName), theTableName);
    System.err.println("calculated metadata base path: '" + basePath + "'");
    return basePath;
  }

  /** Add one snapshot per sub-list of `perSnapshotFilesets`, in order, with the sub-list contents as the data files */
  protected static void initializeSnapshots(Table table, List<List<String>> perSnapshotFilesets) {
    for (List<String> snapshotFileset : perSnapshotFilesets) {
      AppendFiles append = table.newAppend();
      for (String fpath : snapshotFileset) {
        append.appendFile(createDataFile(fpath, 0, 1));
      }
      append.commit();
    }
  }

  /** Extract whatever kind of iceberg metadata file, iff recognized by `doesResemble` */
  protected static Optional<File> extractSomeMetadataFilepath(String candidatePath, String basePath, Predicate<String> doesResemble) {
    try {
      URI candidateUri = new URI(candidatePath);
      File file = new File(candidateUri.getPath());
      Assert.assertEquals(file.getParent(), basePath, "metadata base dirpath");
      return Optional.ofNullable(doesResemble.test(file.getName()) ? file : null);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e); // should not happen!
    }
  }

  protected void verifyMetadataFile(File file, Optional<Integer> optSnapshotSeqNum) {
    Assert.assertTrue(doesResembleMetadataFilename(file.getName()), "metadata filename resemblance");
    if (optSnapshotSeqNum.isPresent()) {
      Assert.assertEquals(Integer.valueOf(file.getName().split("-")[0]), optSnapshotSeqNum.get(),
          "snapshot sequence num");
    }
  }

  protected void verifyManifestListFile(File file, Optional<Long> optSnapshotId) {
    Assert.assertTrue(doesResembleManifestListFilename(file.getName()),"manifest list filename resemblance");
    if (optSnapshotId.isPresent()) {
      Assert.assertEquals(Long.valueOf(file.getName().split("-")[1]), optSnapshotId.get(), "snapshot id");
    }
  }

  protected void verifyManifestFiles(List<IcebergSnapshotInfo.ManifestFileInfo> manifestFileInfos,
      List<String> manifestFilePaths,
      List<List<String>> perSnapshotFilesets) {
    Assert.assertEquals(manifestFileInfos.size(), manifestFilePaths.size());
    Assert.assertEquals(manifestFileInfos.size(), perSnapshotFilesets.size());
    int numManifests = manifestFileInfos.size();
    for (int i = 0; i < numManifests; ++i) {
      IcebergSnapshotInfo.ManifestFileInfo mfi = manifestFileInfos.get(i);
      Assert.assertTrue(doesResembleManifestFilename(mfi.getManifestFilePath()), "manifest filename resemblance");
      Assert.assertEquals(mfi.getManifestFilePath(), manifestFilePaths.get(i));
      verifyAnyOrder(mfi.getListedFilePaths(), perSnapshotFilesets.get(numManifests - i - 1),
          "manifest contents of '" + mfi.getManifestFilePath() + "'");
    }
  }

  protected static boolean doesResembleMetadataFilename(String name) {
    return name.endsWith(".metadata.json");
  }

  protected static boolean doesResembleManifestListFilename(String name) {
    return name.startsWith("snap-") && name.endsWith(".avro");
  }

  protected static boolean doesResembleManifestFilename(String name) {
    return !name.startsWith("snap-") && name.endsWith(".avro");
  }

  /** doesn't actually create a physical file (on disk), merely a {@link org.apache.iceberg.DataFile} */
  protected static DataFile createDataFile(String path, long sizeBytes, long numRecords) {
    return DataFiles.builder(icebergPartitionSpec)
        .withPath(path)
        .withFileSizeInBytes(sizeBytes)
        .withRecordCount(numRecords)
        .build();
  }

  /** general utility: order-independent/set equality between collections */
  protected static <T> void verifyAnyOrder(Collection<T> actual, Collection<T> expected, String message) {
    Assert.assertEquals(Sets.newHashSet(actual), Sets.newHashSet(expected), message);
  }

  /** general utility: flatten a collection of collections into a single-level {@link List} */
  protected static <T, C extends Collection<T>> List<T> flatten(Collection<C> cc) {
    return cc.stream().flatMap(x -> x.stream()).collect(Collectors.toList());
  }
}
