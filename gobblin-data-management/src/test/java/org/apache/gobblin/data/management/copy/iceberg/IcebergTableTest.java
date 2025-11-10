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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.shaded.org.apache.avro.SchemaBuilder;
import org.apache.iceberg.types.Types;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


/** Test {@link org.apache.gobblin.data.management.copy.iceberg.IcebergTable} */
public class IcebergTableTest extends HiveMetastoreTest {

  protected static final org.apache.iceberg.shaded.org.apache.avro.Schema avroDataSchema =
      SchemaBuilder.record("test")
          .fields()
          .name("id")
          .type()
          .stringType()
          .noDefault()
          .endRecord();
  protected static final Schema icebergSchema = AvroSchemaUtil.toIceberg(avroDataSchema);
  protected static final PartitionSpec icebergPartitionSpec = PartitionSpec.builderFor(icebergSchema)
      .identity("id")
      .build();
  protected static final List<List<String>> perSnapshotDataFilesets = Lists.newArrayList(
      Lists.newArrayList("path/to/data-a0.orc"),
      Lists.newArrayList("path/to/data-b0.orc", "path/to/data-b1.orc"),
      Lists.newArrayList("path/to/data-c0.orc", "path/to/data-c1.orc", "path/to/data-c2.orc"),
      Lists.newArrayList("path/to/data-d0.orc")
  );
  protected static final List<List<String>> perSnapshotDeleteFilesets = Lists.newArrayList(
      Lists.newArrayList("path/to/delete-a0.orc"),
      Lists.newArrayList("path/to/delete-b0.orc", "path/to/delete-b1.orc"),
      Lists.newArrayList("path/to/delete-c0.orc", "path/to/delete-c1.orc", "path/to/delete-c2.orc"),
      Lists.newArrayList("path/to/delete-d0.orc")
  );

  private final String dbName = "myicebergdb";
  private final String tableName = "justtesting";
  private final String destTableName = "destTable";
  private TableIdentifier tableId;
  private TableIdentifier sourceTableId;
  private TableIdentifier destTableId;
  private Table table;
  private String catalogUri;
  private String metadataBasePath;

  @BeforeClass
  public void setUp() throws Exception {
    try {
      startMetastore();
    } catch (Exception e) {
      // Metastore may already be started if another test class ran first
      // The startMetastore() method creates a default 'hivedb' which will fail if already exists
    }
    catalog.createNamespace(Namespace.of(dbName));
  }

  @BeforeMethod
  public void setUpEachTest() {
    tableId = TableIdentifier.of(dbName, tableName);
    table = catalog.createTable(tableId, icebergSchema, icebergPartitionSpec, Collections.singletonMap("format-version", "2"));
    catalogUri = catalog.getConf().get(CatalogProperties.URI);
    metadataBasePath = calcMetadataBasePath(tableId);
  }

  @AfterMethod
  public void cleanUpEachTest() {
    catalog.dropTable(tableId);
    catalog.dropTable(sourceTableId);
    catalog.dropTable(destTableId);
  }

  /** Test to verify getCurrentSnapshotInfo, getAllSnapshotInfosIterator, getIncrementalSnapshotInfosIterator for iceberg table containing only data files.*/
  @Test
  public void testGetSnapshotInfosForDataFilesOnlyTable() throws IOException {
    initializeSnapshots(table, perSnapshotDataFilesets);

    IcebergSnapshotInfo snapshotInfo = new IcebergTable(tableId, catalog.newTableOps(tableId), catalogUri,
        catalog.loadTable(tableId)).getCurrentSnapshotInfo();
    verifySnapshotInfo(snapshotInfo, perSnapshotDataFilesets, perSnapshotDataFilesets.size());

    List<IcebergSnapshotInfo> snapshotInfos = Lists.newArrayList(new IcebergTable(tableId, catalog.newTableOps(tableId),
        catalogUri, catalog.loadTable(tableId)).getAllSnapshotInfosIterator());
    Assert.assertEquals(snapshotInfos.size(), perSnapshotDataFilesets.size(), "num snapshots");
    for (int i = 0; i < perSnapshotDataFilesets.size(); ++i) {
      System.err.println("verifying snapshotInfo[" + i + "]");
      verifySnapshotInfo(snapshotInfos.get(i), perSnapshotDataFilesets.subList(0, i + 1), snapshotInfos.size());
    }

    List<IcebergSnapshotInfo> incrementalSnapshotInfos = Lists.newArrayList(new IcebergTable(tableId, catalog.newTableOps(tableId),
        catalogUri, catalog.loadTable(tableId)).getIncrementalSnapshotInfosIterator());
    Assert.assertEquals(incrementalSnapshotInfos.size(), perSnapshotDataFilesets.size(), "num snapshots");
    for (int i = 0; i < incrementalSnapshotInfos.size(); ++i) {
      System.err.println("verifying snapshotInfo[" + i + "]");
      verifySnapshotInfo(incrementalSnapshotInfos.get(i), perSnapshotDataFilesets.subList(i, i + 1), incrementalSnapshotInfos.size());
    }
  }

  @Test
  public void schemaUpdateSuccessTest() throws IcebergTable.TableNotFoundException {
    // create source iceberg table with this schema
    Schema sourceIcebergSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "product_name", Types.StringType.get()),
        Types.NestedField.required(3, "details",
            Types.StructType.of(Types.NestedField.required(4, "description", Types.StringType.get()),
                Types.NestedField.required(6, "category", Types.StringType.get()),
                Types.NestedField.required(7, "remarks", Types.StringType.get()))),
        Types.NestedField.required(5, "price", Types.DecimalType.of(10, 2)));

    PartitionSpec sourceIcebergPartitionSpec = PartitionSpec.builderFor(sourceIcebergSchema)
        .identity("id")
        .build();

    sourceTableId = TableIdentifier.of(dbName, tableName + "_source");
    catalog.createTable(sourceTableId, sourceIcebergSchema, sourceIcebergPartitionSpec, Collections.singletonMap("format-version", "2"));

    // create destination iceberg table with this schema
    Schema destIcebergSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    PartitionSpec destIcebergPartitionSpec = PartitionSpec.builderFor(destIcebergSchema)
        .identity("id")
        .build();

    destTableId = TableIdentifier.of(dbName, tableName + "_dest");
    catalog.createTable(destTableId, destIcebergSchema, destIcebergPartitionSpec, Collections.singletonMap("format-version", "2"));

    TableOperations destTableOps = Mockito.spy(catalog.newTableOps(destTableId));
    TableOperations sourceTableOps = catalog.newTableOps(sourceTableId);

    IcebergTable destIcebergTable = new IcebergTable(destTableId, destTableOps, catalogUri, catalog.loadTable(tableId));

    TableMetadata srcTableMetadata = sourceTableOps.current();
    Schema srcTableSchema = srcTableMetadata.schema();

    // update schema to verify is schema update succeeds
    destIcebergTable.updateSchema(srcTableSchema, false);
    Mockito.verify(destTableOps, Mockito.times(1)).commit(Mockito.any(), Mockito.any());
  }

  @Test
  public void schemaUpdateTest_divergentSchema() throws IcebergTable.TableNotFoundException {
    // create source iceberg table with this schema
    Schema sourceIcebergSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "product_name", Types.StringType.get()),
        Types.NestedField.required(3, "details",
            Types.StructType.of(Types.NestedField.required(4, "description", Types.StringType.get()),
                Types.NestedField.required(6, "category", Types.StringType.get()),
                Types.NestedField.required(7, "remarks", Types.StringType.get()))),
        Types.NestedField.required(5, "price", Types.DecimalType.of(10, 2)));

    PartitionSpec sourceIcebergPartitionSpec = PartitionSpec.builderFor(sourceIcebergSchema)
        .identity("id")
        .build();

    TableIdentifier sourceTableId = TableIdentifier.of(dbName, tableName + "_source");
    catalog.createTable(sourceTableId, sourceIcebergSchema, sourceIcebergPartitionSpec, Collections.singletonMap("format-version", "2"));

    // create destination iceberg table with this schema
    Schema destIcebergSchema = new Schema(Types.NestedField.required(1, "randomField", Types.LongType.get()));

    PartitionSpec destIcebergPartitionSpec = PartitionSpec.builderFor(destIcebergSchema)
        .identity("randomField")
        .build();

    TableIdentifier destTableId = TableIdentifier.of(dbName, tableName + "_dest");
    catalog.createTable(destTableId, destIcebergSchema, destIcebergPartitionSpec, Collections.singletonMap("format-version", "2"));

    TableOperations destTableOps = Mockito.spy(catalog.newTableOps(destTableId));
    TableOperations sourceTableOps = catalog.newTableOps(sourceTableId);

    IcebergTable destIcebergTable = new IcebergTable(destTableId, destTableOps, catalogUri, catalog.loadTable(destTableId));

    TableMetadata srcTableMetadata = sourceTableOps.current();
    Schema srcTableSchema = srcTableMetadata.schema();

    // update schema to verify is schema update succeeds
    destIcebergTable.updateSchema(srcTableSchema, false);
    Mockito.verify(destTableOps, Mockito.times(1)).commit(Mockito.any(), Mockito.any());
  }

  @Test
  public void schemaUpdateTest_sameSchema_noOpTest() throws IcebergTable.TableNotFoundException {
    // create source iceberg table with this schema
    Schema sourceIcebergSchema = new Schema(Types.NestedField.required(1, "randomField", Types.LongType.get()));

    PartitionSpec sourceIcebergPartitionSpec = PartitionSpec.builderFor(sourceIcebergSchema)
        .identity("randomField")
        .build();

    TableIdentifier sourceTableId = TableIdentifier.of(dbName, tableName + "_source");
    catalog.createTable(sourceTableId, sourceIcebergSchema, sourceIcebergPartitionSpec, Collections.singletonMap("format-version", "2"));

    // create destination iceberg table with this schema
    Schema destIcebergSchema = new Schema(Types.NestedField.required(1, "randomField", Types.LongType.get()));

    PartitionSpec destIcebergPartitionSpec = PartitionSpec.builderFor(destIcebergSchema)
        .identity("randomField")
        .build();

    TableIdentifier destTableId = TableIdentifier.of(dbName, tableName + "_dest");
    catalog.createTable(destTableId, destIcebergSchema, destIcebergPartitionSpec, Collections.singletonMap("format-version", "2"));

    TableOperations destTableOps = Mockito.spy(catalog.newTableOps(destTableId));
    TableOperations sourceTableOps = catalog.newTableOps(sourceTableId);

    IcebergTable destIcebergTable = new IcebergTable(destTableId, destTableOps, catalogUri, catalog.loadTable(destTableId));

    TableMetadata srcTableMetadata = sourceTableOps.current();
    Schema srcTableSchema = srcTableMetadata.schema();

    // update schema to verify is schema update succeeds
    destIcebergTable.updateSchema(srcTableSchema, false);
    Mockito.verify(destTableOps, Mockito.times(0)).commit(Mockito.any(), Mockito.any());
  }

  @Test
  public void schemaUpdate_onlyValidationTest() throws IcebergTable.TableNotFoundException {
    // create source iceberg table with this schema
    Schema sourceIcebergSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.required(2, "product_name", Types.StringType.get()),
        Types.NestedField.required(3, "details",
            Types.StructType.of(Types.NestedField.required(4, "description", Types.StringType.get()),
                Types.NestedField.required(6, "category", Types.StringType.get()),
                Types.NestedField.required(7, "remarks", Types.StringType.get()))),
        Types.NestedField.required(5, "price", Types.DecimalType.of(10, 2)));

    PartitionSpec sourceIcebergPartitionSpec = PartitionSpec.builderFor(sourceIcebergSchema)
        .identity("id")
        .build();

    sourceTableId = TableIdentifier.of(dbName, tableName + "_source");
    catalog.createTable(sourceTableId, sourceIcebergSchema, sourceIcebergPartitionSpec, Collections.singletonMap("format-version", "2"));

    // create destination iceberg table with this schema
    Schema destIcebergSchema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    PartitionSpec destIcebergPartitionSpec = PartitionSpec.builderFor(destIcebergSchema)
        .identity("id")
        .build();

    destTableId = TableIdentifier.of(dbName, tableName + "_dest");
    catalog.createTable(destTableId, destIcebergSchema, destIcebergPartitionSpec, Collections.singletonMap("format-version", "2"));

    TableOperations destTableOps = Mockito.spy(catalog.newTableOps(destTableId));
    TableOperations sourceTableOps = catalog.newTableOps(sourceTableId);

    IcebergTable destIcebergTable = new IcebergTable(destTableId, destTableOps, catalogUri, catalog.loadTable(tableId));

    TableMetadata srcTableMetadata = sourceTableOps.current();
    Schema srcTableSchema = srcTableMetadata.schema();

    // update schema to verify is schema update succeeds
    destIcebergTable.updateSchema(srcTableSchema, true);
    Mockito.verify(destTableOps, Mockito.times(0)).commit(Mockito.any(), Mockito.any());
  }

  @DataProvider(name = "isPosDeleteProvider")
  public Object[][] isPosDeleteProvider() {
    return new Object[][] {{true}, {false}};
  }

  /** Verify info about the current snapshot only */
  @Test(dataProvider = "isPosDeleteProvider")
  public void testGetCurrentSnapshotInfo(boolean isPosDelete) throws IOException {
    initializeSnapshots(table, perSnapshotDataFilesets);
    initializeSnapshotsWithDeleteFiles(table, catalog.newTableOps(tableId).io(), perSnapshotDeleteFilesets, isPosDelete);
    List<List<String>> perSnapshotFilesets = Stream.concat(perSnapshotDeleteFilesets.stream(), perSnapshotDataFilesets.stream())
        .collect(Collectors.toList());
    IcebergSnapshotInfo snapshotInfo = new IcebergTable(tableId, catalog.newTableOps(tableId), catalogUri,
        catalog.loadTable(tableId)).getCurrentSnapshotInfo();
    verifySnapshotInfo(snapshotInfo, perSnapshotFilesets, perSnapshotFilesets.size());
  }

  /** Verify failure when attempting to get current snapshot info for non-existent table */
  @Test(expectedExceptions = IcebergTable.TableNotFoundException.class)
  public void testGetCurrentSnapshotInfoOnBogusTable() throws IOException {
    TableIdentifier bogusTableId = TableIdentifier.of(dbName, tableName + "_BOGUS");
    IcebergSnapshotInfo snapshotInfo = new IcebergTable(bogusTableId, catalog.newTableOps(bogusTableId), catalogUri,
        null).getCurrentSnapshotInfo();
    Assert.fail("expected an exception when using table ID '" + bogusTableId + "'");
  }

  /** Verify info about all (full) snapshots */
  @Test(dataProvider = "isPosDeleteProvider")
  public void testGetAllSnapshotInfosIterator(boolean isPosDelete) throws IOException {
    int numDataSnapshots = perSnapshotDataFilesets.size();
    int numDeleteSnapshots = perSnapshotDeleteFilesets.size();

    initializeSnapshots(table, perSnapshotDataFilesets);
    initializeSnapshotsWithDeleteFiles(table, catalog.newTableOps(tableId).io(), perSnapshotDeleteFilesets, isPosDelete);
    List<IcebergSnapshotInfo> snapshotInfos = Lists.newArrayList(new IcebergTable(tableId, catalog.newTableOps(tableId),
        catalogUri, catalog.loadTable(tableId)).getAllSnapshotInfosIterator());
    Assert.assertEquals(snapshotInfos.size(), numDataSnapshots + numDeleteSnapshots, "num snapshots");

    for (int i = 0; i < numDataSnapshots; ++i) {
      System.err.println("verifying snapshotInfo[" + i + "]");
      verifySnapshotInfo(snapshotInfos.get(i), perSnapshotDataFilesets.subList(0, i + 1), snapshotInfos.size());
    }

    for (int i = 0 ; i < numDeleteSnapshots ; i++) {
      System.err.println("verifying snapshotInfo[" + i + "]");
      List<List<String>> curSnapshotFileSets = Stream.concat(perSnapshotDeleteFilesets.subList(0, i + 1).stream(), perSnapshotDataFilesets.stream())
          .collect(Collectors.toList());
      verifySnapshotInfo(snapshotInfos.get(i + numDataSnapshots), curSnapshotFileSets, snapshotInfos.size());
    }
  }

  /** Verify info about all snapshots (incremental deltas) */
  @Test(dataProvider = "isPosDeleteProvider")
  public void testGetIncrementalSnapshotInfosIterator(boolean isPosDelete) throws IOException {
    initializeSnapshots(table, perSnapshotDataFilesets);
    initializeSnapshotsWithDeleteFiles(table, catalog.newTableOps(tableId).io(), perSnapshotDeleteFilesets, isPosDelete);
    List<List<String>> perSnapshotFilesets = Stream.concat(perSnapshotDataFilesets.stream(), perSnapshotDeleteFilesets.stream())
        .collect(Collectors.toList());
    List<IcebergSnapshotInfo> snapshotInfos = Lists.newArrayList(new IcebergTable(tableId, catalog.newTableOps(tableId),
        catalogUri, catalog.loadTable(tableId)).getIncrementalSnapshotInfosIterator());
    Assert.assertEquals(snapshotInfos.size(), perSnapshotFilesets.size(), "num snapshots");

    for (int i = 0; i < snapshotInfos.size(); ++i) {
      System.err.println("verifying snapshotInfo[" + i + "]");
      verifySnapshotInfo(snapshotInfos.get(i), perSnapshotFilesets.subList(i, i + 1), snapshotInfos.size());
    }
  }

  /** Verify info about all snapshots (incremental deltas) correctly eliminates repeated data files */
  @Test(dataProvider = "isPosDeleteProvider")
  public void testGetIncrementalSnapshotInfosIteratorRepeatedFiles(boolean isPosDelete) throws IOException {
    List<List<String>> perSnapshotFilesets1 = Lists.newArrayList(
        Lists.newArrayList("path/to/data-a0.orc"),
        Lists.newArrayList("path/to/data-b0.orc", "path/to/data-b1.orc", "path/to/data-a0.orc"),
        Lists.newArrayList("path/to/data-a0.orc","path/to/data-c0.orc", "path/to/data-b1.orc", "path/to/data-c1.orc", "path/to/data-c2.orc"),
        Lists.newArrayList("path/to/data-d0.orc")
    );

    // Note : Keeping the name as data- only to test the functionality without changing below validation code
    List<List<String>> perSnapshotFilesets2 = Lists.newArrayList(
        Lists.newArrayList("path/to/data-e0.orc"),
        Lists.newArrayList("path/to/data-f0.orc", "path/to/data-f1.orc", "path/to/data-e0.orc"),
        Lists.newArrayList("path/to/data-e0.orc","path/to/data-g0.orc", "path/to/data-f1.orc", "path/to/data-g1.orc", "path/to/data-g2.orc"),
        Lists.newArrayList("path/to/data-h0.orc")
    );

    List<List<String>> perSnapshotFileSets = new ArrayList<>(perSnapshotFilesets1);
    perSnapshotFileSets.addAll(perSnapshotFilesets2);

    initializeSnapshots(table, perSnapshotFilesets1);
    initializeSnapshotsWithDeleteFiles(table, catalog.newTableOps(tableId).io(), perSnapshotFilesets2, isPosDelete);
    List<IcebergSnapshotInfo> snapshotInfos = Lists.newArrayList(new IcebergTable(tableId, catalog.newTableOps(tableId),
        catalogUri, catalog.loadTable(tableId)).getIncrementalSnapshotInfosIterator());
    Assert.assertEquals(snapshotInfos.size(), perSnapshotFileSets.size(), "num snapshots");

    for (int i = 0; i < snapshotInfos.size(); ++i) {
      System.err.println("verifying snapshotInfo[" + i + "] - " + snapshotInfos.get(i));
      char initialChar = (char) ((int) 'a' + i);
      // adjust expectations to eliminate duplicate entries (i.e. those bearing letter not aligned with ordinal fileset)
      List<String> fileset = perSnapshotFileSets.get(i).stream().filter(name -> {
        String uniquePortion = name.substring("path/to/data-".length());
        return uniquePortion.startsWith(Character.toString(initialChar));
      }).collect(Collectors.toList());
      verifySnapshotInfo(snapshotInfos.get(i), Arrays.asList(fileset), snapshotInfos.size());
    }
  }

  /** Verify that registerIcebergTable will update existing table properties */
  @Test
  public void testNewTablePropertiesAreRegistered() throws Exception {
    Map<String, String> srcTableProperties = Maps.newHashMap();
    Map<String, String> destTableProperties = Maps.newHashMap();

    srcTableProperties.put("newKey", "newValue");
    // Expect the old value to be overwritten by the new value
    srcTableProperties.put("testKey", "testValueNew");
    destTableProperties.put("testKey", "testValueOld");
    // Expect existing property values to be deleted if it does not exist on the source
    destTableProperties.put("deletedTableProperty", "deletedTablePropertyValue");

    TableIdentifier destTableId = TableIdentifier.of(dbName, destTableName);
    catalog.createTable(destTableId, icebergSchema, null, destTableProperties);

    IcebergTable destIcebergTable = new IcebergTable(destTableId, catalog.newTableOps(destTableId), catalogUri,
        catalog.loadTable(destTableId));
    // Mock a source table with the same table UUID copying new properties
    TableMetadata newSourceTableProperties = destIcebergTable.accessTableMetadata().replaceProperties(srcTableProperties);

    destIcebergTable.registerIcebergTable(newSourceTableProperties, destIcebergTable.accessTableMetadata());
    Assert.assertEquals(destIcebergTable.accessTableMetadata().properties().size(), 2);
    Assert.assertEquals(destIcebergTable.accessTableMetadata().properties().get("newKey"), "newValue");
    Assert.assertEquals(destIcebergTable.accessTableMetadata().properties().get("testKey"), "testValueNew");
    Assert.assertNull(destIcebergTable.accessTableMetadata().properties().get("deletedTableProperty"));

    catalog.dropTable(destTableId);
  }

  /** Verify that getPartitionSpecificDataFiles return datafiles belonging to the partition defined by predicate */
  @Test
  public void testGetPartitionSpecificDataFiles() throws IOException {
    // Note - any specific file path format is not mandatory to be mapped to specific partition
    List<String> paths = Arrays.asList(
        "/path/tableName/data/id=1/file1.orc",
        "/path/tableName/data/file3.orc",
        "/path/tableName/data/id=2/file5.orc",
        "/path/tableName/data/file4.orc",
        "/path/tableName/data/id=3/file2.orc"
    );
    // Using the schema defined in start of this class
    PartitionData partitionData = new PartitionData(icebergPartitionSpec.partitionType());
    partitionData.set(0, "1");

    addPartitionDataFiles(table, createDataFiles(paths.stream().collect(Collectors.toMap(Function.identity(), v -> partitionData))));

    IcebergTable icebergTable = new IcebergTable(tableId,
        catalog.newTableOps(tableId),
        catalogUri,
        catalog.loadTable(tableId));
    // Using AlwaysTrue & AlwaysFalse Predicate to avoid mocking of predicate class
    Predicate<StructLike> alwaysTruePredicate = partition -> true;
    Predicate<StructLike> alwaysFalsePredicate = partition -> false;
    Assert.assertEquals(icebergTable.getPartitionSpecificDataFiles(alwaysTruePredicate).size(), 5);
    Assert.assertEquals(icebergTable.getPartitionSpecificDataFiles(alwaysFalsePredicate).size(), 0);
  }

  /** Verify that overwritePartition replace data files belonging to given partition col and value */
  @Test
  public void testOverwritePartition() throws IOException {
    // Note - any specific file path format is not mandatory to be mapped to specific partition
    List<String> paths = Arrays.asList(
        "/path/tableName/data/id=1/file1.orc",
        "/path/tableName/data/file2.orc"
    );
    // Using the schema defined in start of this class
    PartitionData partition1Data = new PartitionData(icebergPartitionSpec.partitionType());
    partition1Data.set(0, "1");

    addPartitionDataFiles(table, createDataFiles(paths.stream().collect(Collectors.toMap(Function.identity(), v -> partition1Data))));

    IcebergTable icebergTable = new IcebergTable(tableId,
        catalog.newTableOps(tableId),
        catalogUri,
        catalog.loadTable(tableId));

    verifyAnyOrder(paths, icebergTable.getCurrentSnapshotInfo().getAllDataFilePaths(), "data filepaths should match");

    List<String> paths2 = Arrays.asList(
        "/path/tableName/data/file3.orc",
        "/path/tableName/data/id=2/file4.orc"
    );
    // Using the schema defined in start of this class
    PartitionData partition2Data = new PartitionData(icebergPartitionSpec.partitionType());
    partition2Data.set(0, "2");

    List<DataFile> partition2DataFiles = createDataFiles(paths2.stream().collect(Collectors.toMap(Function.identity(), v -> partition2Data)));
    // here, since partition data with value 2 doesn't exist yet,
    // we expect it to get added to the table, w/o changing or deleting any other partitions
    icebergTable.overwritePartition(partition2DataFiles, "id", "2");
    List<String> expectedPaths2 = new ArrayList<>(paths);
    expectedPaths2.addAll(paths2);
    verifyAnyOrder(expectedPaths2, icebergTable.getCurrentSnapshotInfo().getAllDataFilePaths(), "data filepaths should match");

    List<String> paths3 = Arrays.asList(
        "/path/tableName/data/id=2/file5.orc",
        "/path/tableName/data/file6.orc"
    );
    // Reusing same partition data to create data file with different paths
    List<DataFile> partition1NewDataFiles = createDataFiles(paths3.stream().collect(Collectors.toMap(Function.identity(), v -> partition1Data)));
    // here, since partition data with value 1 already exists, we expect it to get updated in the table with newer path
    icebergTable.overwritePartition(partition1NewDataFiles, "id", "1");
    List<String> expectedPaths3 = new ArrayList<>(paths2);
    expectedPaths3.addAll(paths3);
    verifyAnyOrder(expectedPaths3, icebergTable.getCurrentSnapshotInfo().getAllDataFilePaths(), "data filepaths should match");
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
    boolean shouldIncludeMetadataPath = false;
    List<String> allPathsExpected = Lists.newArrayList(snapshotInfo.getManifestListPath());
    allPathsExpected.addAll(snapshotInfo.getManifestFilePaths());
    allPathsExpected.addAll(snapshotInfo.getAllDataFilePaths());
    verifyAnyOrder(snapshotInfo.getAllPaths(shouldIncludeMetadataPath), allPathsExpected, "all paths, metadata and data, except metadataPath itself");

    boolean shouldIncludeMetadataPathIfAvailable = true;
    snapshotInfo.getMetadataPath().ifPresent(allPathsExpected::add);
    verifyAnyOrder(snapshotInfo.getAllPaths(shouldIncludeMetadataPathIfAvailable), allPathsExpected, "all paths, metadata and data, including metadataPath");
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

  protected static void initializeSnapshotsWithDeleteFiles(Table table, FileIO fileIO,
      List<List<String>> perSnapshotFilesets, boolean isPosDelete) {
    Schema deleteSchema = icebergSchema.select("id");
    int[] equalityFieldIds = {0};
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(
            icebergSchema,
            icebergPartitionSpec,
            equalityFieldIds,
            deleteSchema,
            deleteSchema);
    EncryptionManager encryptionManager = table.encryption();
    Record deleteRecord = GenericRecord.create(deleteSchema).copy(ImmutableMap.of("id", "testVal"));
    PartitionData partitionData = new PartitionData(icebergPartitionSpec.partitionType());
    partitionData.set(0, "testVal");

    for (List<String> snapshotFileset : perSnapshotFilesets) {
      RowDelta rowDelta = table.newRowDelta();
      for (String filePath : snapshotFileset) {
        EncryptedOutputFile encryptedOutputFile = encryptionManager.encrypt(fileIO.newOutputFile(filePath));
        if (isPosDelete) {
          rowDelta.addDeletes(createPosDeleteFile(appenderFactory, encryptedOutputFile, partitionData, deleteRecord));
        } else {
          rowDelta.addDeletes(createEqDeleteFile(appenderFactory, encryptedOutputFile, partitionData, deleteRecord));
        }
      }
      rowDelta.commit();
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

  protected static DeleteFile createEqDeleteFile(GenericAppenderFactory appenderFactory,
      EncryptedOutputFile encryptedOutputFile, StructLike partitionData, Record record) {
    EqualityDeleteWriter<Record> eqDeleteWriter = appenderFactory.newEqDeleteWriter(encryptedOutputFile, FileFormat.ORC, partitionData);
    try (EqualityDeleteWriter<Record> clsEqDeleteWriter = eqDeleteWriter) {
      clsEqDeleteWriter.write(record);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return eqDeleteWriter.toDeleteFile();
  }

  protected static DeleteFile createPosDeleteFile(GenericAppenderFactory appenderFactory,
      EncryptedOutputFile encryptedOutputFile, StructLike partitionData, Record record) {
    PositionDelete<Record> posDelRecord = PositionDelete.create();
    posDelRecord.set("dummyFilePath", 0, record);
    PositionDeleteWriter<Record> posDeleteWriter = appenderFactory.newPosDeleteWriter(encryptedOutputFile, FileFormat.ORC, partitionData);
    try (PositionDeleteWriter<Record> clsPosDeleteWriter = posDeleteWriter) {
      clsPosDeleteWriter.write(posDelRecord);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return posDeleteWriter.toDeleteFile();
  }

  /** general utility: order-independent/set equality between collections */
  protected static <T> void verifyAnyOrder(Collection<T> actual, Collection<T> expected, String message) {
    Assert.assertEquals(Sets.newHashSet(actual), Sets.newHashSet(expected), message);
  }

  /** general utility: flatten a collection of collections into a single-level {@link List} */
  protected static <T, C extends Collection<T>> List<T> flatten(Collection<C> cc) {
    return cc.stream().flatMap(x -> x.stream()).collect(Collectors.toList());
  }

  private static void addPartitionDataFiles(Table table, List<DataFile> dataFiles) {
    dataFiles.forEach(dataFile -> table.newAppend().appendFile(dataFile).commit());
  }

  private static List<DataFile> createDataFiles(Map<String, PartitionData> pathWithPartitionData) {
    return pathWithPartitionData.entrySet().stream()
        .map(e -> createDataFileWithPartition(e.getKey(), e.getValue()))
        .collect(Collectors.toList());
  }

  private static DataFile createDataFileWithPartition(String path, PartitionData partitionData) {
    return DataFiles.builder(icebergPartitionSpec)
        .withPath(path)
        .withFileSizeInBytes(8)
        .withRecordCount(1)
        .withPartition(partitionData)
        .withFormat(FileFormat.ORC)
        .build();
  }

}
