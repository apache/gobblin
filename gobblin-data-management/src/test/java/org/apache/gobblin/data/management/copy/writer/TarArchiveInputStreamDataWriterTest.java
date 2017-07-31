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
package org.apache.gobblin.data.management.copy.writer;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.CopyableDatasetMetadata;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.CopyableFileUtils;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.data.management.copy.OwnerAndPermission;
import org.apache.gobblin.data.management.copy.TestCopyableDataset;
import org.apache.gobblin.data.management.copy.converter.UnGzipConverter;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.TestUtils;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;
import com.google.common.io.Files;


public class TarArchiveInputStreamDataWriterTest {

  private FileSystem fs;
  private Path testTempPath;

  @BeforeClass
  public void setup() throws Exception {
    fs = FileSystem.getLocal(new Configuration());
    testTempPath =
        new Path(Files.createTempDir().getAbsolutePath(), "tarArchiveInputStreamDataWriterTest");
    fs.mkdirs(testTempPath);
  }

  @DataProvider(name = "testFileDataProvider")
  public static Object[][] fileDataProvider() {
    // {filePath, newFileName, expectedText}
    return new Object[][] { { "tarArchiveInputStreamDataWriterTest/archived.tar.gz", "archived.tar.gz", "text" },
        { "tarArchiveInputStreamDataWriterTest/archived.tgz", "archived_new_name", "text" } };
  }

  @Test(dataProvider = "testFileDataProvider")
  public void testWrite(final String filePath, final String newFileName, final String expectedText) throws Exception {

    String expectedFileContents = "text";
    String fileNameInArchive = "text.txt";

    WorkUnitState state = TestUtils.createTestWorkUnitState();
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(testTempPath, "staging").toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(testTempPath, "output").toString());
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, "writer_file_path_" + RandomStringUtils.randomAlphabetic(5));
    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")));
    CopySource.serializeCopyableDataset(state, metadata);

    FileAwareInputStream fileAwareInputStream = getCompressedInputStream(filePath, newFileName);
    CopySource.serializeCopyEntity(state, fileAwareInputStream.getFile());

    TarArchiveInputStreamDataWriter dataWriter = new TarArchiveInputStreamDataWriter(state, 1, 0);
    dataWriter.write(fileAwareInputStream);
    dataWriter.commit();

    // the archive file contains file test.txt
    Path unArchivedFilePath = new Path(fileAwareInputStream.getFile().getDestination(), fileNameInArchive);

    // Path at which the writer writes text.txt
    Path taskOutputFilePath =
        new Path(new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
            fileAwareInputStream.getFile().getDatasetAndPartition(metadata).identifier()),
            PathUtils.withoutLeadingSeparator(unArchivedFilePath));

    Assert.assertEquals(IOUtils.toString(new FileInputStream(taskOutputFilePath.toString())).trim(),
        expectedFileContents);
  }

  /**
   * Find the test compressed file <code><filePath/code> in classpath and read it as a {@link FileAwareInputStream}
   */
  private FileAwareInputStream getCompressedInputStream(final String filePath, final String newFileName) throws Exception {
    UnGzipConverter converter = new UnGzipConverter();

    FileSystem fs = FileSystem.getLocal(new Configuration());

    String fullPath = getClass().getClassLoader().getResource(filePath).getFile();
    FileStatus status = fs.getFileStatus(testTempPath);

    OwnerAndPermission ownerAndPermission =
        new OwnerAndPermission(status.getOwner(), status.getGroup(), new FsPermission(FsAction.ALL, FsAction.ALL,
            FsAction.ALL));
    CopyableFile cf =
        CopyableFileUtils.getTestCopyableFile(filePath, new Path(testTempPath, newFileName).toString(), newFileName,
            ownerAndPermission);

    FileAwareInputStream fileAwareInputStream = new FileAwareInputStream(cf, fs.open(new Path(fullPath)));

    Iterable<FileAwareInputStream> iterable =
        converter.convertRecord("outputSchema", fileAwareInputStream, new WorkUnitState());

    return Iterables.getFirst(iterable, null);
  }

  @AfterClass
  public void cleanup() {
    try {
      fs.delete(testTempPath, true);
    } catch (IOException e) {
      // ignore
    }
  }
}
