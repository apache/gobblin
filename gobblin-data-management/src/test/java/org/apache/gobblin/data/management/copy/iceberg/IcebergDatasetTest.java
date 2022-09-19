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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.api.client.util.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyContext;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.PreserveAttributes;


public class
IcebergDatasetTest {

  static final String METADATA_PATH = "/root/iceberg/test/metadata";
  static final String MANIFEST_PATH = "/root/iceberg/test/metadata/test_manifest";
  static final String MANIFEST_LIST_PATH = "/root/iceberg/test/metadata/test_manifest/data";
  static final String MANIFEST_FILE_PATH1 = "/root/iceberg/test/metadata/test_manifest/data/a";
  static final String MANIFEST_FILE_PATH2 = "/root/iceberg/test/metadata/test_manifest/data/b";

  @Test
  public void testGetFilePaths() throws IOException {

    List<String> pathsToCopy = new ArrayList<>();
    pathsToCopy.add(MANIFEST_FILE_PATH1);
    pathsToCopy.add(MANIFEST_FILE_PATH2);
    Map<Path, FileStatus> expected = Maps.newHashMap();
    expected.put(new Path(MANIFEST_FILE_PATH1), null);
    expected.put(new Path(MANIFEST_FILE_PATH2), null);

    IcebergTable icebergTable = Mockito.mock(IcebergTable.class);
    FileSystem fs = Mockito.mock(FileSystem.class);
    IcebergSnapshotInfo icebergSnapshotInfo = Mockito.mock(IcebergSnapshotInfo.class);

    Mockito.when(icebergTable.getCurrentSnapshotInfo()).thenReturn(icebergSnapshotInfo);
    Mockito.when(icebergSnapshotInfo.getAllPaths()).thenReturn(pathsToCopy);
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

    FileSystem fs = Mockito.mock(FileSystem.class);
    String test_db_name = "test_db_name";
    String test_table_name = "test_tbl_name";
    String test_qualified_path = "/root/iceberg/test/destination/sub_path_destination";
    String test_uri_path = "/root/iceberg/test/output";
    Properties properties = new Properties();
    properties.setProperty("data.publisher.final.dir", "/test");
    List<String> expected = new ArrayList<>(Arrays.asList(METADATA_PATH, MANIFEST_PATH, MANIFEST_LIST_PATH, MANIFEST_FILE_PATH1, MANIFEST_FILE_PATH2));

    CopyConfiguration copyConfiguration = CopyConfiguration.builder(null, properties)
        .preserve(PreserveAttributes.fromMnemonicString(""))
        .copyContext(new CopyContext())
        .build();

    List<String> listedManifestFilePaths = Arrays.asList(MANIFEST_FILE_PATH1, MANIFEST_FILE_PATH2);
    IcebergSnapshotInfo.ManifestFileInfo manifestFileInfo = new IcebergSnapshotInfo.ManifestFileInfo(MANIFEST_LIST_PATH, listedManifestFilePaths);
    List<IcebergSnapshotInfo.ManifestFileInfo> manifestFiles = Arrays.asList(manifestFileInfo);
    IcebergTable icebergTable = new MockedIcebergTable(METADATA_PATH, MANIFEST_PATH, manifestFiles);
    IcebergDataset icebergDataset = new IcebergDataset(test_db_name, test_table_name, icebergTable, new Properties(), fs);
    DestinationFileSystem destinationFileSystem = new DestinationFileSystem();
    destinationFileSystem.addPath(METADATA_PATH);
    destinationFileSystem.addPath(MANIFEST_PATH);
    destinationFileSystem.addPath(MANIFEST_LIST_PATH);
    destinationFileSystem.addPath(MANIFEST_FILE_PATH1);
    destinationFileSystem.addPath(MANIFEST_FILE_PATH2);

    mockFileSystemMethodCalls(fs, destinationFileSystem.pathToFileStatus, test_qualified_path, test_uri_path);

    Collection<CopyEntity> copyEntities = icebergDataset.generateCopyEntities(copyConfiguration);
    verifyCopyEntities(copyEntities, expected);

  }

  private void verifyCopyEntities(Collection<CopyEntity> copyEntities, List<String> expected) {
    List<String> actual = new ArrayList<>();
    for (CopyEntity copyEntity : copyEntities) {
      String json = copyEntity.toString();
      JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
      JsonObject objectData =
          jsonObject.getAsJsonObject("object-data").getAsJsonObject("origin").getAsJsonObject("object-data");
      JsonObject pathObject = objectData.getAsJsonObject("path").getAsJsonObject("object-data").getAsJsonObject("uri");
      String filepath = pathObject.getAsJsonPrimitive("object-data").getAsString();
      actual.add(filepath);
    }
    Assert.assertEquals(actual.size(), expected.size());
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

    String metadataPath;
    String manifestListPath;
    List<IcebergSnapshotInfo.ManifestFileInfo> manifestFiles;

    public MockedIcebergTable(String metadataPath, String manifestListPath, List<IcebergSnapshotInfo.ManifestFileInfo> manifestFiles) {
      super(null);
      this.metadataPath = metadataPath;
      this.manifestListPath = manifestListPath;
      this.manifestFiles = manifestFiles;
    }

    @Override
    public IcebergSnapshotInfo getCurrentSnapshotInfo() {
      Long snapshotId = 0L;
      Instant timestamp  = Instant.ofEpochMilli(0L);
      return new IcebergSnapshotInfo(snapshotId, timestamp, metadataPath, manifestListPath, manifestFiles);
    }
  }

  private static class DestinationFileSystem {
    Map<Path, FileStatus> pathToFileStatus;

    public DestinationFileSystem() {
      this.pathToFileStatus = Maps.newHashMap();
    }

    public void addPath(String pathString) {
      if (StringUtils.isBlank(pathString)) {
        throw new IllegalArgumentException("Missing path value for the file system");
      }
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

