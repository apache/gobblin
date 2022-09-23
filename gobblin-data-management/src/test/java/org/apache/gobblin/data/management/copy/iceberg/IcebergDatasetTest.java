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
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import lombok.Data;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.api.client.util.Maps;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyContext;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.PreserveAttributes;


public class IcebergDatasetTest {

  private static final String ROOT_PATH = "/root/iceberg/test/";
  private static final String METADATA_PATH = ROOT_PATH + "metadata/metadata.json";
  private static final String MANIFEST_LIST_PATH_0 = ROOT_PATH + "metadata/manifest_list.x";
  private static final String MANIFEST_PATH_0 = ROOT_PATH + "metadata/manifest.a";
  private static final String MANIFEST_DATA_PATH_0A = ROOT_PATH + "data/p0/a";
  private static final String MANIFEST_DATA_PATH_0B = ROOT_PATH + "data/p0/b";
  private static final MockedIcebergTable.SnapshotPaths SNAPSHOT_PATHS_0 = new MockedIcebergTable.SnapshotPaths(
      Optional.of(METADATA_PATH), MANIFEST_LIST_PATH_0,
      Arrays.asList(new IcebergSnapshotInfo.ManifestFileInfo(
          MANIFEST_PATH_0, Arrays.asList(MANIFEST_DATA_PATH_0A, MANIFEST_DATA_PATH_0B)))
  );
  private static final String MANIFEST_LIST_PATH_1 = MANIFEST_LIST_PATH_0.replaceAll("\\.x$", ".y");
  private static final String MANIFEST_PATH_1 = MANIFEST_PATH_0.replaceAll("\\.a$", ".b");
  private static final String MANIFEST_DATA_PATH_1A = MANIFEST_DATA_PATH_0A.replaceAll("/p0/", "/p1/");
  private static final String MANIFEST_DATA_PATH_1B = MANIFEST_DATA_PATH_0B.replaceAll("/p0/", "/p1/");
  private static final MockedIcebergTable.SnapshotPaths SNAPSHOT_PATHS_1 = new MockedIcebergTable.SnapshotPaths(
      Optional.empty(), MANIFEST_LIST_PATH_1,
      Arrays.asList(new IcebergSnapshotInfo.ManifestFileInfo(
          MANIFEST_PATH_1, Arrays.asList(MANIFEST_DATA_PATH_1A, MANIFEST_DATA_PATH_1B)))
  );

  private final String test_db_name = "test_db_name";
  private final String test_table_name = "test_tbl_name";
  private final String test_qualified_path = "/root/iceberg/test/destination/sub_path_destination";
  private final String test_uri_path = "/root/iceberg/test/output";
  private final Properties properties = new Properties();

  @BeforeClass
  public void setUp() throws Exception {
    properties.setProperty("data.publisher.final.dir", "/test");
  }

  @Test
  public void testGetFilePaths() throws IOException {

    List<String> pathsToCopy = Lists.newArrayList(MANIFEST_DATA_PATH_0A, MANIFEST_DATA_PATH_0B);
    Map<Path, FileStatus> expected = Maps.newHashMap();
    expected.put(new Path(MANIFEST_DATA_PATH_0A), null);
    expected.put(new Path(MANIFEST_DATA_PATH_0B), null);

    IcebergTable icebergTable = Mockito.mock(IcebergTable.class);
    FileSystem fs = Mockito.mock(FileSystem.class);
    IcebergSnapshotInfo icebergSnapshotInfo = Mockito.mock(IcebergSnapshotInfo.class);

    Mockito.when(icebergTable.getIncrementalSnapshotInfosIterator()).thenReturn(Arrays.asList(icebergSnapshotInfo).iterator());
    Mockito.when(icebergSnapshotInfo.getAllPaths()).thenReturn(pathsToCopy);
    Mockito.when(icebergSnapshotInfo.getSnapshotId()).thenReturn(98765L);
    Mockito.when(icebergSnapshotInfo.getMetadataPath()).thenReturn(Optional.of("path for log message"));

    IcebergDataset icebergDataset = new IcebergDataset("test_db_name", "test_tbl_name", icebergTable, new Properties(), fs);

    Map<Path, FileStatus> actual = icebergDataset.getFilePathsToFileStatus();
    Assert.assertEquals(actual, expected);
  }

  /**
   * Test case to generate copy entities for all the file paths for a mocked iceberg table.
   * The assumption here is that we create copy entities for all the matching file paths,
   * without calculating any difference between the source and destination
   */
  @Test
  public void testGenerateCopyEntitiesWhenDestEmpty() throws IOException, URISyntaxException {
    List<String> expectedPaths = Arrays.asList(METADATA_PATH, MANIFEST_LIST_PATH_0,
        MANIFEST_PATH_0, MANIFEST_DATA_PATH_0A, MANIFEST_DATA_PATH_0B);

    FileSystem fs = Mockito.mock(FileSystem.class);
    IcebergTable icebergTable = new MockedIcebergTable(Arrays.asList(SNAPSHOT_PATHS_0));
    IcebergDataset icebergDataset = new IcebergDataset(test_db_name, test_table_name, icebergTable, new Properties(), fs);
    DestinationFileSystem destinationFileSystem = new DestinationFileSystem();
    destinationFileSystem.addPaths(expectedPaths);

    mockFileSystemMethodCalls(fs, destinationFileSystem.pathToFileStatus, test_qualified_path, test_uri_path);

    CopyConfiguration copyConfiguration = CopyConfiguration.builder(null, properties)
        .preserve(PreserveAttributes.fromMnemonicString(""))
        .copyContext(new CopyContext())
        .build();
    Collection<CopyEntity> copyEntities = icebergDataset.generateCopyEntities(fs, copyConfiguration);
    verifyCopyEntities(copyEntities, expectedPaths);
  }

  /** Test generating copy entities for a multi-snapshot iceberg; given empty dest, src-dest delta will be entirety */
  @Test
  public void testGenerateCopyEntitiesMultiSnapshotWhenDestEmpty() throws IOException, URISyntaxException {
    List<String> expectedPaths = Arrays.asList(METADATA_PATH,
        MANIFEST_LIST_PATH_0, MANIFEST_PATH_0, MANIFEST_DATA_PATH_0A, MANIFEST_DATA_PATH_0B,
        MANIFEST_LIST_PATH_1, MANIFEST_PATH_1, MANIFEST_DATA_PATH_1A, MANIFEST_DATA_PATH_1B);

    FileSystem fs = Mockito.mock(FileSystem.class);
    IcebergTable icebergTable = new MockedIcebergTable(Arrays.asList(SNAPSHOT_PATHS_1, SNAPSHOT_PATHS_0));
    IcebergDataset icebergDataset = new IcebergDataset(test_db_name, test_table_name, icebergTable, new Properties(), fs);
    DestinationFileSystem destinationFileSystem = new DestinationFileSystem();
    destinationFileSystem.addPaths(expectedPaths);

    mockFileSystemMethodCalls(fs, destinationFileSystem.pathToFileStatus, test_qualified_path, test_uri_path);

    CopyConfiguration copyConfiguration = CopyConfiguration.builder(null, properties)
        .preserve(PreserveAttributes.fromMnemonicString(""))
        .copyContext(new CopyContext())
        .build();
    Collection<CopyEntity> copyEntities = icebergDataset.generateCopyEntities(fs, copyConfiguration);
    verifyCopyEntities(copyEntities, expectedPaths);
  }

  private void verifyCopyEntities(Collection<CopyEntity> copyEntities, List<String> expected) {
    List<String> actual = new ArrayList<>();
    for (CopyEntity copyEntity : copyEntities) {
      String json = copyEntity.toString();
      String filepath = new Gson().fromJson(json, JsonObject.class)
          .getAsJsonObject("object-data").getAsJsonObject("origin")
          .getAsJsonObject("object-data").getAsJsonObject("path")
          .getAsJsonObject("object-data").getAsJsonObject("uri")
          .getAsJsonPrimitive("object-data").getAsString();
      actual.add(filepath);
    }
    Assert.assertEquals(actual.size(), expected.size(),
        "Set" + actual.toString() + " vs Set" + expected.toString());
    Assert.assertEqualsNoOrder(actual.toArray(), expected.toArray());
  }

  private void mockFileSystemMethodCalls(FileSystem fs, Map<Path, FileStatus> pathToFileStatus, String qualifiedPath, String uriPath)
      throws URISyntaxException, IOException {

    Mockito.when(fs.getUri()).thenReturn(new URI(null, null, uriPath, null));
    for (Map.Entry<Path, FileStatus> entry : pathToFileStatus.entrySet()) {
      Path path = entry.getKey();
      FileStatus fileStatus = entry.getValue();
      Mockito.when(fs.getFileStatus(path)).thenReturn(fileStatus);
      Mockito.when(fs.makeQualified(path)).thenReturn(new Path(qualifiedPath));
    }
  }

  private static class MockedIcebergTable extends IcebergTable {

    @Data
    public static class SnapshotPaths {
      private final Optional<String> metadataPath;
      private final String manifestListPath;
      private final List<IcebergSnapshotInfo.ManifestFileInfo> manifestFiles;
    }

    private final List<SnapshotPaths> snapshotPathsList;

    public MockedIcebergTable(List<SnapshotPaths> snapshotPathsList) {
      super(null);
      this.snapshotPathsList = Lists.newCopyOnWriteArrayList(snapshotPathsList);
    }

    @Override
    public Iterator<IcebergSnapshotInfo> getAllSnapshotInfosIterator() {
      Long snapshotId = 0L;
      Instant timestamp  = Instant.ofEpochMilli(0L);
      List<IcebergSnapshotInfo> snapshotInfos = snapshotPathsList.stream()
          .map(snapshotPaths -> createSnapshotInfo(snapshotPaths, snapshotId, timestamp))
          .collect(Collectors.toList());
      return snapshotInfos.iterator();
    }

    private IcebergSnapshotInfo createSnapshotInfo(SnapshotPaths snapshotPaths, Long snapshotId, Instant timestamp) {
      return new IcebergSnapshotInfo(snapshotId, timestamp, snapshotPaths.metadataPath, snapshotPaths.manifestListPath, snapshotPaths.manifestFiles);
    }
  }

  private static class DestinationFileSystem {
    Map<Path, FileStatus> pathToFileStatus;

    public DestinationFileSystem() {
      this.pathToFileStatus = Maps.newHashMap();
    }

    public void addPaths(List<String> pathStrings) {
      for (String pathString : pathStrings) {
        addPath(pathString);
      }
    }

    public void addPath(String pathString) {
      Path path  = new Path(pathString);
      FileStatus fileStatus = new FileStatus();
      fileStatus.setPath(path);
      this.pathToFileStatus.put(path, fileStatus);
    }

    public void addPath(String pathString, FileStatus fileStatus) {
      Path path = new Path(pathString);
      this.pathToFileStatus.put(path, fileStatus);
    }
  }
}

