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

import com.google.api.client.util.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopyContext;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.PreserveAttributes;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.TableOperations;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.*;


public class IcebergDatasetTest {

  static final String METADATA_PATH = "/root/iceberg/test/metadata";
  static final String MANIFEST_PATH = "/root/iceberg/test/metadata/test_manifest";
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

    Map<Path, FileStatus> actual = icebergDataset.getFilePaths();
    Assert.assertEquals(actual, expected);
  }

  /**
   * Test case to copy all the file paths for a mocked iceberg table. This is a full copy overwriting everything on the destination
   */
  @Test
  public void testGenerateCopyEntitiesForTableFileSet() throws IOException, URISyntaxException {

    FileSystem fs = Mockito.mock(FileSystem.class);
    String test_db_name = "test_db_name";
    String test_table_name = "test_tbl_name";
    Set<String> setOfFilePaths = new HashSet<>(Arrays.asList(METADATA_PATH, MANIFEST_PATH, MANIFEST_FILE_PATH1, MANIFEST_FILE_PATH2));

    Properties properties = new Properties();
    properties.setProperty("data.publisher.final.dir", "/test");

    CopyConfiguration copyConfiguration = CopyConfiguration.builder(null, properties)
        .preserve(PreserveAttributes.fromMnemonicString(""))
        .copyContext(new CopyContext())
        .build();
    TableOperations tableOperations = Mockito.mock(TableOperations.class);

    IcebergTable icebergTable = new MockedIcebergTable(tableOperations);
    IcebergDataset icebergDataset = new IcebergDataset(test_db_name, test_table_name, icebergTable, new Properties(), fs);

    FileStatus fileStatus1 = new FileStatus();
    fileStatus1.setPath(new Path(METADATA_PATH));
    FileStatus fileStatus2 = new FileStatus();
    fileStatus2.setPath(new Path(MANIFEST_PATH));
    FileStatus fileStatus3 = new FileStatus();
    fileStatus3.setPath(new Path(MANIFEST_FILE_PATH1));
    FileStatus fileStatus4 = new FileStatus();
    fileStatus4.setPath(new Path(MANIFEST_FILE_PATH2));

    Path path1 = new Path(METADATA_PATH);
    Path path2 = new Path(MANIFEST_PATH);
    Path path3 = new Path(MANIFEST_FILE_PATH1);
    Path path4 = new Path(MANIFEST_FILE_PATH2);


    Mockito.when(fs.makeQualified(any(Path.class))).thenReturn(new Path("/root/iceberg/test/destination/sub_path_destination"));
    Mockito.when(fs.getFileStatus(path1)).thenReturn(fileStatus1);
    Mockito.when(fs.getFileStatus(path2)).thenReturn(fileStatus2);
    Mockito.when(fs.getFileStatus(path3)).thenReturn(fileStatus3);
    Mockito.when(fs.getFileStatus(path4)).thenReturn(fileStatus4);
    Mockito.when(fs.getUri()).thenReturn(new URI(null, null, "/root/iceberg/test/output", null));

    Collection<CopyEntity> copyEntities = icebergDataset.generateCopyEntitiesForTableFileSet(copyConfiguration);
    Assert.assertEquals(copyEntities.size(), setOfFilePaths.size());
    for (CopyEntity copyEntity : copyEntities) {
      String json = copyEntity.toString();
      JsonObject jsonObject = new Gson().fromJson(json, JsonObject.class);
      JsonObject objectData =
          jsonObject.getAsJsonObject("object-data").getAsJsonObject("origin").getAsJsonObject("object-data");
      JsonObject pathObject = objectData.getAsJsonObject("path").getAsJsonObject("object-data").getAsJsonObject("uri");
      String filepath = pathObject.getAsJsonPrimitive("object-data").getAsString();
      Assert.assertTrue(setOfFilePaths.contains(filepath));
      setOfFilePaths.remove(filepath);
    }
    Assert.assertTrue(setOfFilePaths.isEmpty());
  }

  private static class MockedIcebergTable extends IcebergTable {

    public MockedIcebergTable(TableOperations tableOps) {
      super(tableOps);
    }

    @Override
    public IcebergSnapshotInfo getCurrentSnapshotInfo() {
      Long snapshotId = 0L;
      Instant timestamp  = Instant.ofEpochMilli(0L);
      List<String> manifestFilePaths = Arrays.asList(MANIFEST_FILE_PATH1, MANIFEST_FILE_PATH2);
      List<List<String>> manifestListedFilePaths = new ArrayList<>();
      manifestListedFilePaths.add(manifestFilePaths);

      return new IcebergSnapshotInfo(snapshotId, timestamp, METADATA_PATH, MANIFEST_PATH, manifestFilePaths, manifestListedFilePaths);
    }
  }
}

