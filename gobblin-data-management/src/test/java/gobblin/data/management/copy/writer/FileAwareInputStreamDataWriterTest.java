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
package gobblin.data.management.copy.writer;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

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
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.crypto.EncryptionConfigParser;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableDatasetMetadata;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.CopyableFileUtils;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.data.management.copy.OwnerAndPermission;
import gobblin.data.management.copy.PreserveAttributes;
import gobblin.data.management.copy.TestCopyableDataset;
import gobblin.util.TestUtils;
import gobblin.util.io.StreamUtils;


public class FileAwareInputStreamDataWriterTest {

  FileSystem fs;
  Path testTempPath;

  @BeforeClass
  public void setup() throws Exception {
    fs = FileSystem.getLocal(new Configuration());
    testTempPath = new Path(Files.createTempDir().getAbsolutePath(), "InputStreamDataWriterTest");
    fs.mkdirs(testTempPath);
  }

  @Test
  public void testWrite() throws Exception {
    String streamString = "testContents";

    FileStatus status = fs.getFileStatus(testTempPath);
    OwnerAndPermission ownerAndPermission =
        new OwnerAndPermission(status.getOwner(), status.getGroup(), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    CopyableFile cf = CopyableFileUtils.getTestCopyableFile(ownerAndPermission);

    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")));

    WorkUnitState state = TestUtils.createTestWorkUnitState();
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(testTempPath, "staging").toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(testTempPath, "output").toString());
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, RandomStringUtils.randomAlphabetic(5));
    CopySource.serializeCopyEntity(state, cf);
    CopySource.serializeCopyableDataset(state, metadata);

    FileAwareInputStreamDataWriter dataWriter = new FileAwareInputStreamDataWriter(state, 1, 0);

    FileAwareInputStream fileAwareInputStream = new FileAwareInputStream(cf, StreamUtils.convertStream(IOUtils.toInputStream(streamString)));
    dataWriter.write(fileAwareInputStream);
    dataWriter.commit();
    Path writtenFilePath = new Path(new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
        cf.getDatasetAndPartition(metadata).identifier()), cf.getDestination());
    Assert.assertEquals(IOUtils.toString(new FileInputStream(writtenFilePath.toString())), streamString);
  }

  @Test
  public void testWriteWithEncryption() throws Exception {
    byte[] streamString = "testEncryptedContents".getBytes("UTF-8");
    byte[] expectedContents = new byte[streamString.length];
    for (int i = 0; i < streamString.length; i++) {
      expectedContents[i] = (byte)((streamString[i] + 1) % 256);
    }

    FileStatus status = fs.getFileStatus(testTempPath);
    OwnerAndPermission ownerAndPermission =
        new OwnerAndPermission(status.getOwner(), status.getGroup(), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    CopyableFile cf = CopyableFileUtils.getTestCopyableFile(ownerAndPermission);

    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")));

    WorkUnitState state = TestUtils.createTestWorkUnitState();
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(testTempPath, "staging").toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(testTempPath, "output").toString());
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, RandomStringUtils.randomAlphabetic(5));
    state.setProp(EncryptionConfigParser.ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "insecure_shift");

    CopySource.serializeCopyEntity(state, cf);
    CopySource.serializeCopyableDataset(state, metadata);

    FileAwareInputStreamDataWriter dataWriter = new FileAwareInputStreamDataWriter(state, 1, 0);

    FileAwareInputStream fileAwareInputStream = new FileAwareInputStream(cf, StreamUtils.convertStream(
        new ByteArrayInputStream(streamString)));
    dataWriter.write(fileAwareInputStream);
    dataWriter.commit();

    Path writtenFilePath = new Path(new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
        cf.getDatasetAndPartition(metadata).identifier()), cf.getDestination());
    Assert.assertTrue(writtenFilePath.getName().endsWith("insecure_shift"),
        "Expected encryption name to be appended to destination");
    Assert.assertEquals(IOUtils.toByteArray(new FileInputStream(writtenFilePath.toString())), expectedContents);
  }

  @Test
  public void testCommit() throws IOException {

    String destinationExistingToken = "destination";
    String destinationAdditionalTokens = "path";
    String fileName = "file";

    // Asemble destination paths
    Path destination = new Path(new Path(new Path("/", destinationExistingToken), destinationAdditionalTokens), fileName);
    Path destinationWithoutLeadingSeparator = new Path(new Path(destinationExistingToken, destinationAdditionalTokens), fileName);

    // Create temp directory
    File tmpFile = Files.createTempDir();
    tmpFile.deleteOnExit();
    Path tmpPath = new Path(tmpFile.getAbsolutePath());

    // create origin file
    Path originFile = new Path(tmpPath, fileName);
    this.fs.createNewFile(originFile);

    // create stating dir
    Path stagingDir = new Path(tmpPath, "staging");
    this.fs.mkdirs(stagingDir);

    // create output dir
    Path outputDir = new Path(tmpPath, "output");
    this.fs.mkdirs(outputDir);

    // create copyable file
    FileStatus status = this.fs.getFileStatus(originFile);
    FsPermission readWrite = new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE);
    FsPermission dirReadWrite = new FsPermission(FsAction.ALL, FsAction.READ_WRITE, FsAction.READ_WRITE);
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(status.getOwner(), status.getGroup(), readWrite);
    List<OwnerAndPermission> ancestorOwnerAndPermissions = Lists.newArrayList();
    ancestorOwnerAndPermissions.add(ownerAndPermission);
    ancestorOwnerAndPermissions.add(ownerAndPermission);
    ancestorOwnerAndPermissions.add(ownerAndPermission);
    ancestorOwnerAndPermissions.add(ownerAndPermission);

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/publisher");

    CopyableFile cf = CopyableFile.fromOriginAndDestination(this.fs, status, destination,
        CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).publishDir(new Path("/target"))
            .preserve(PreserveAttributes.fromMnemonicString("")).build())
        .destinationOwnerAndPermission(ownerAndPermission)
        .ancestorsOwnerAndPermission(ancestorOwnerAndPermissions)
        .build();

    // create work unit state
    WorkUnitState state = TestUtils.createTestWorkUnitState();
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, stagingDir.toUri().getPath());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, outputDir.toUri().getPath());
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, RandomStringUtils.randomAlphabetic(5));
    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")));
    CopySource.serializeCopyEntity(state, cf);
    CopySource.serializeCopyableDataset(state, metadata);

    // create writer
    FileAwareInputStreamDataWriter writer = new FileAwareInputStreamDataWriter(state, 1, 0);

    // create output of writer.write
    Path writtenFile = writer.getStagingFilePath(cf);
    this.fs.mkdirs(writtenFile.getParent());
    this.fs.createNewFile(writtenFile);

    // create existing directories in writer output
    Path outputRoot = FileAwareInputStreamDataWriter.getPartitionOutputRoot(outputDir, cf.getDatasetAndPartition(metadata));
    Path existingOutputPath = new Path(outputRoot, destinationExistingToken);
    this.fs.mkdirs(existingOutputPath);
    FileStatus fileStatus = this.fs.getFileStatus(existingOutputPath);
    FsPermission existingPathPermission = fileStatus.getPermission();

    // check initial state of the relevant directories
    Assert.assertTrue(this.fs.exists(existingOutputPath));
    Assert.assertEquals(this.fs.listStatus(existingOutputPath).length, 0);

    writer.actualProcessedCopyableFile = Optional.of(cf);

    // commit
    writer.commit();

    // check state of relevant paths after commit
    Path expectedOutputPath = new Path(outputRoot, destinationWithoutLeadingSeparator);
    Assert.assertTrue(this.fs.exists(expectedOutputPath));
    fileStatus = this.fs.getFileStatus(expectedOutputPath);
    Assert.assertEquals(fileStatus.getOwner(), ownerAndPermission.getOwner());
    Assert.assertEquals(fileStatus.getGroup(), ownerAndPermission.getGroup());
    Assert.assertEquals(fileStatus.getPermission(), readWrite);
    // parent should have permissions set correctly
    fileStatus = this.fs.getFileStatus(expectedOutputPath.getParent());
    Assert.assertEquals(fileStatus.getPermission(), dirReadWrite);
    // previously existing paths should not have permissions changed
    fileStatus = this.fs.getFileStatus(existingOutputPath);
    Assert.assertEquals(fileStatus.getPermission(), existingPathPermission);

    Assert.assertFalse(this.fs.exists(writer.stagingDir));
  }

  @Test
  public void testAddExecutePermission() {
    Assert.assertEquals(FileAwareInputStreamDataWriter.addExecutePermissionToOwner(new FsPermission("000")),
        new FsPermission("100"));
    Assert.assertEquals(FileAwareInputStreamDataWriter.addExecutePermissionToOwner(new FsPermission("100")),
        new FsPermission("100"));
    Assert.assertEquals(FileAwareInputStreamDataWriter.addExecutePermissionToOwner(new FsPermission("200")),
        new FsPermission("300"));
    Assert.assertEquals(FileAwareInputStreamDataWriter.addExecutePermissionToOwner(new FsPermission("400")),
        new FsPermission("500"));
    Assert.assertEquals(FileAwareInputStreamDataWriter.addExecutePermissionToOwner(new FsPermission("600")),
        new FsPermission("700"));
    Assert.assertEquals(FileAwareInputStreamDataWriter.addExecutePermissionToOwner(new FsPermission("700")),
        new FsPermission("700"));
    Assert.assertEquals(FileAwareInputStreamDataWriter.addExecutePermissionToOwner(new FsPermission("211")),
        new FsPermission("311"));
    Assert.assertEquals(FileAwareInputStreamDataWriter.addExecutePermissionToOwner(new FsPermission("250")),
        new FsPermission("350"));
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
