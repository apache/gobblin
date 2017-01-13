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

package gobblin.source.extractor.extract.google;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

/**
 * Test for GoogleDriveFileSystemTest. Most of tests are done via @see GoogleDriveFsHelperTest
 *
 */
@Test(groups = { "gobblin.source.extractor.google" })
public class GoogleDriveFileSystemTest {

  public void testQuery() throws IOException {
    GoogleDriveFileSystem fs = new GoogleDriveFileSystem();

    String folderId = "test_folder_id";
    String fileName = "test_file_name";

    Optional<String> query = fs.buildQuery(null, null);
    Assert.assertEquals(query, Optional.absent());

    query = fs.buildQuery(folderId, null);
    Assert.assertEquals(query, Optional.of("'test_folder_id' in parents"));

    query = fs.buildQuery(null, fileName);
    Assert.assertEquals(query, Optional.of("name contains 'test_file_name'"));

    query = fs.buildQuery(folderId, fileName);
    Assert.assertEquals(query, Optional.of("'test_folder_id' in parents and name contains 'test_file_name'"));

    fs.close();
  }

  public void toFileIdTest() {
    String fileId = "test";
    String fileIdWithSlash = "test2/test1";
    String fileIdWithTwoSlashes = "test3/test2/test1";
    String fileIdWithThreeSlashes = "test4/test3/test2/test1";

    Assert.assertEquals(GoogleDriveFileSystem.toFileId(new Path(fileId)), fileId);
    Assert.assertEquals(GoogleDriveFileSystem.toFileId(new Path(fileIdWithSlash)), fileIdWithSlash);
    Assert.assertEquals(GoogleDriveFileSystem.toFileId(new Path(fileIdWithTwoSlashes)), fileIdWithTwoSlashes);
    Assert.assertEquals(GoogleDriveFileSystem.toFileId(new Path(fileIdWithThreeSlashes)), fileIdWithThreeSlashes);
  }
}
