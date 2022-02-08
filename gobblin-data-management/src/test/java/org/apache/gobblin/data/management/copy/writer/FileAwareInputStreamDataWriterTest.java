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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.crypto.EncryptionConfigParser;
import org.apache.gobblin.crypto.GPGFileDecryptor;
import org.apache.gobblin.crypto.GPGFileEncryptor;
import org.apache.gobblin.crypto.GPGFileEncryptorTest;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.CopyableDatasetMetadata;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.CopyableFileUtils;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.data.management.copy.OwnerAndPermission;
import org.apache.gobblin.data.management.copy.PreserveAttributes;
import org.apache.gobblin.data.management.copy.TestCopyableDataset;
import org.apache.gobblin.data.management.copy.splitter.DistcpFileSplitter;
import org.apache.gobblin.util.TestUtils;
import org.apache.gobblin.util.WriterUtils;
import org.apache.gobblin.util.io.StreamUtils;

import static org.mockito.Mockito.*;


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
    String streamString1 = "testContents1";
    String streamString2 = "testContents2";
    String userDefStagingDir = System.getProperty("user.dir") + "/user_staging_dir";
    FileStatus status = fs.getFileStatus(testTempPath);
    OwnerAndPermission ownerAndPermission =
        new OwnerAndPermission(status.getOwner(), status.getGroup(), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    CopyableFile cf = CopyableFileUtils.getTestCopyableFile((long) streamString1.length(), ownerAndPermission);
    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")));
    WorkUnitState state = TestUtils.createTestWorkUnitState();
    state.setProp(ConfigurationKeys.USER_DEFINED_STAGING_DIR_FLAG,false);
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(testTempPath, "staging").toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(testTempPath, "output").toString());
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, RandomStringUtils.randomAlphabetic(5));
    CopySource.serializeCopyEntity(state, cf);
    CopySource.serializeCopyableDataset(state, metadata);
    FileAwareInputStreamDataWriter dataWriter = new FileAwareInputStreamDataWriter(state, 1, 0);
    FileAwareInputStream fileAwareInputStream = FileAwareInputStream.builder().file(cf)
        .inputStream(StreamUtils.convertStream(IOUtils.toInputStream(streamString1))).build();
    Assert.assertNotEquals(dataWriter.stagingDir,userDefStagingDir);
    dataWriter.write(fileAwareInputStream);
    dataWriter.commit();
    Path writtenFilePath = new Path(new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
        cf.getDatasetAndPartition(metadata).identifier()), cf.getDestination());
    Assert.assertEquals(IOUtils.toString(new FileInputStream(writtenFilePath.toString())), streamString1);

    //testing user defined staging directory
    WorkUnitState state2 = TestUtils.createTestWorkUnitState();
    state2.setProp(ConfigurationKeys.USER_DEFINED_STAGING_DIR_FLAG,true);
    state2.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(testTempPath, "staging").toString());
    state2.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(testTempPath, "output2").toString());
    state2.setProp(ConfigurationKeys.USER_DEFINED_STATIC_STAGING_DIR,userDefStagingDir);
    state2.setProp(ConfigurationKeys.WRITER_FILE_PATH, RandomStringUtils.randomAlphabetic(5));
    CopySource.serializeCopyEntity(state2, cf);
    CopySource.serializeCopyableDataset(state2, metadata);
    dataWriter = new FileAwareInputStreamDataWriter(state2, 1, 0);
    fileAwareInputStream = FileAwareInputStream.builder().file(cf)
        .inputStream(StreamUtils.convertStream(IOUtils.toInputStream(streamString2))).build();
    Assert.assertEquals(dataWriter.stagingDir.toUri().toString(),userDefStagingDir);
    dataWriter.write(fileAwareInputStream);
    dataWriter.commit();
    writtenFilePath = new Path(new Path(state2.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
        cf.getDatasetAndPartition(metadata).identifier()), cf.getDestination());
    Assert.assertEquals(IOUtils.toString(new FileInputStream(writtenFilePath.toString())), streamString2);
  }

  @Test
  public void testBlockWrite() throws Exception {
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
    state.setProp(DistcpFileSplitter.SPLIT_ENABLED, true);
    CopySource.serializeCopyEntity(state, cf);
    CopySource.serializeCopyableDataset(state, metadata);

    FileAwareInputStreamDataWriter dataWriter = new FileAwareInputStreamDataWriter(state, 1, 0);

    long splitLen = 4;
    int splits = (int) (streamString.length() / splitLen + 1);
    DistcpFileSplitter.Split split = new DistcpFileSplitter.Split(0, splitLen, 0, splits,
        String.format("%s.__PART%d__", cf.getDestination().getName(), 0));
    FSDataInputStream dataInputStream = StreamUtils.convertStream(IOUtils.toInputStream(streamString));
    dataInputStream.seek(split.getLowPosition());
    FileAwareInputStream fileAwareInputStream = FileAwareInputStream.builder().file(cf)
        .inputStream(dataInputStream)
        .split(Optional.of(split))
        .build();
    dataWriter.write(fileAwareInputStream);
    dataWriter.commit();
    Path writtenFilePath = new Path(new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
        cf.getDatasetAndPartition(metadata).identifier()), cf.getDestination());
    Assert.assertEquals(IOUtils.toString(new FileInputStream(writtenFilePath.toString())),
        streamString.substring(0, (int) splitLen));
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
    CopyableFile cf = CopyableFileUtils.getTestCopyableFile((long) streamString.length, ownerAndPermission);

    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")));

    WorkUnitState state = TestUtils.createTestWorkUnitState();
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(testTempPath, "staging").toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(testTempPath, "output").toString());
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, RandomStringUtils.randomAlphabetic(5));
    state.setProp("writer.encrypt." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "insecure_shift");

    CopySource.serializeCopyEntity(state, cf);
    CopySource.serializeCopyableDataset(state, metadata);

    FileAwareInputStreamDataWriter dataWriter = new FileAwareInputStreamDataWriter(state, 1, 0);

    FileAwareInputStream fileAwareInputStream = FileAwareInputStream.builder().file(cf)
        .inputStream(StreamUtils.convertStream(new ByteArrayInputStream(streamString))).build();
    dataWriter.write(fileAwareInputStream);
    dataWriter.commit();

    Path writtenFilePath = new Path(new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
        cf.getDatasetAndPartition(metadata).identifier()), cf.getDestination());
    Assert.assertTrue(writtenFilePath.getName().endsWith("insecure_shift"),
        "Expected encryption name to be appended to destination");
    Assert.assertEquals(IOUtils.toByteArray(new FileInputStream(writtenFilePath.toString())), expectedContents);
  }

  @Test
  public void testWriteWithGPGSymmetricEncryption() throws Exception {
    byte[] streamString = "testEncryptedContents".getBytes("UTF-8");

    FileStatus status = fs.getFileStatus(testTempPath);
    OwnerAndPermission ownerAndPermission =
        new OwnerAndPermission(status.getOwner(), status.getGroup(), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    CopyableFile cf = CopyableFileUtils.getTestCopyableFile((long) streamString.length, ownerAndPermission);

    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")));

    WorkUnitState state = TestUtils.createTestWorkUnitState();
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(testTempPath, "staging").toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(testTempPath, "output").toString());
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, RandomStringUtils.randomAlphabetic(5));
    state.setProp("writer.encrypt." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "gpg");
    state.setProp("writer.encrypt." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY, "testPassword");

    CopySource.serializeCopyEntity(state, cf);
    CopySource.serializeCopyableDataset(state, metadata);

    FileAwareInputStreamDataWriter dataWriter = new FileAwareInputStreamDataWriter(state, 1, 0);

    FileAwareInputStream fileAwareInputStream = FileAwareInputStream.builder().file(cf)
        .inputStream(StreamUtils.convertStream(new ByteArrayInputStream(streamString))).build();
    dataWriter.write(fileAwareInputStream);
    dataWriter.commit();

    Path writtenFilePath = new Path(new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
        cf.getDatasetAndPartition(metadata).identifier()), cf.getDestination());
    Assert.assertTrue(writtenFilePath.getName().endsWith("gpg"),
        "Expected encryption name to be appended to destination");
    byte[] encryptedContent = IOUtils.toByteArray(new FileInputStream(writtenFilePath.toString()));
    byte[] decryptedContent = new byte[streamString.length];
    IOUtils.readFully(GPGFileDecryptor.decryptFile(new FileInputStream(writtenFilePath.toString()), "testPassword"),
        decryptedContent);


    // encrypted string should not be the same as the plaintext
    Assert.assertNotEquals(encryptedContent, streamString);

    // decrypted string should be the same as the plaintext
    Assert.assertEquals(decryptedContent, streamString);

  }

  @Test
  public void testWriteWithGPGAsymmetricEncryption() throws Exception {
    byte[] streamString = "testEncryptedContents".getBytes("UTF-8");

    FileStatus status = fs.getFileStatus(testTempPath);
    OwnerAndPermission ownerAndPermission =
        new OwnerAndPermission(status.getOwner(), status.getGroup(), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    CopyableFile cf = CopyableFileUtils.getTestCopyableFile((long) streamString.length, ownerAndPermission);

    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")));

    WorkUnitState state = TestUtils.createTestWorkUnitState();
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(testTempPath, "staging").toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(testTempPath, "output").toString());
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, RandomStringUtils.randomAlphabetic(5));
    state.setProp("writer.encrypt." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "gpg");

    File publicKeyFile = new File(testTempPath.toString(), "public.key");

    FileUtils.copyInputStreamToFile(GPGFileEncryptor.class.getResourceAsStream(GPGFileEncryptorTest.PUBLIC_KEY),
        publicKeyFile);

    state.setProp("writer.encrypt." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY, publicKeyFile.getAbsolutePath());
    state.setProp("writer.encrypt." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY,
        GPGFileEncryptorTest.PASSPHRASE);
    state.setProp("writer.encrypt." + EncryptionConfigParser.ENCRYPTION_KEY_NAME,
        GPGFileEncryptorTest.KEY_ID);

    CopySource.serializeCopyEntity(state, cf);
    CopySource.serializeCopyableDataset(state, metadata);

    FileAwareInputStreamDataWriter dataWriter = new FileAwareInputStreamDataWriter(state, 1, 0);

    FileAwareInputStream fileAwareInputStream = FileAwareInputStream.builder().file(cf)
        .inputStream(StreamUtils.convertStream(new ByteArrayInputStream(streamString))).build();
    dataWriter.write(fileAwareInputStream);
    dataWriter.commit();

    Path writtenFilePath = new Path(new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
        cf.getDatasetAndPartition(metadata).identifier()), cf.getDestination());
    Assert.assertTrue(writtenFilePath.getName().endsWith("gpg"),
        "Expected encryption name to be appended to destination");
    byte[] encryptedContent = IOUtils.toByteArray(new FileInputStream(writtenFilePath.toString()));
    byte[] decryptedContent = new byte[streamString.length];
    IOUtils.readFully(GPGFileDecryptor.decryptFile(new FileInputStream(writtenFilePath.toString()),
        GPGFileEncryptor.class.getResourceAsStream(GPGFileEncryptorTest.PRIVATE_KEY),
        GPGFileEncryptorTest.PASSPHRASE), decryptedContent);


    // encrypted string should not be the same as the plaintext
    Assert.assertNotEquals(encryptedContent, streamString);

    // decrypted string should be the same as the plaintext
    Assert.assertEquals(decryptedContent, streamString);

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

  @Test
  public void testRetryingAfterFailure() throws Exception {
    String streamString1 = "testContents1";
    FileStatus status = fs.getFileStatus(testTempPath);
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(status.getOwner(), status.getGroup(),
        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    CopyableFile cf = CopyableFileUtils.getTestCopyableFile((long) streamString1.length(), ownerAndPermission);
    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")));
    WorkUnitState state = TestUtils.createTestWorkUnitState();
    state.setProp(ConfigurationKeys.USER_DEFINED_STAGING_DIR_FLAG, false);
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(testTempPath, "staging").toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(testTempPath, "output").toString());
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, RandomStringUtils.randomAlphabetic(5));
    CopySource.serializeCopyEntity(state, cf);
    CopySource.serializeCopyableDataset(state, metadata);

    FileSystem fileSystem = spy(FileSystem.get(URI.create("file:///"), WriterUtils.getFsConfiguration(state)));

    FileAwareInputStreamDataWriter dataWriter = new FileAwareInputStreamDataWriter(state, fileSystem, 1, 0, null);
    FileAwareInputStream fileAwareInputStream = FileAwareInputStream.builder()
        .file(cf)
        .inputStream(StreamUtils.convertStream(IOUtils.toInputStream(streamString1, StandardCharsets.UTF_8)))
        .build();

    doThrow(new AccessDeniedException("Test")).when(fileSystem).mkdirs(any());

    for (int i = 1; i <= 3; i++) {
      try {
        dataWriter.write(fileAwareInputStream);
        Assert.fail("Expected method to throw AccessDeniedException on call #" + i);
      } catch (AccessDeniedException e) {
        // expected exception
      }
    }

    doCallRealMethod().when(fileSystem).mkdirs(any());
    dataWriter.write(fileAwareInputStream);

    dataWriter.commit();
    Path writtenFilePath = new Path(
        new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR), cf.getDatasetAndPartition(metadata).identifier()),
        cf.getDestination());
    Assert.assertEquals(IOUtils.toString(new FileInputStream(writtenFilePath.toString()), StandardCharsets.UTF_8),
        streamString1);
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
