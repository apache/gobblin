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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.gobblin.policies.size.FileSizePolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
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
import org.apache.gobblin.data.management.copy.PreserveAttributes;
import org.apache.gobblin.data.management.copy.TestCopyableDataset;
import org.apache.gobblin.data.management.copy.splitter.DistcpFileSplitter;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.TestUtils;
import org.apache.gobblin.util.WriterUtils;
import org.apache.gobblin.util.filesystem.OwnerAndPermission;
import org.apache.gobblin.util.io.StreamUtils;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;


public class FileAwareInputStreamDataWriterTest {

  TestLocalFileSystem fs;
  Path testTempPath;

  @BeforeClass
  public void setup() throws Exception {
    fs = new TestLocalFileSystem();
    fs.initialize(URI.create("file:///"), new Configuration());
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
    Path destinationWithoutLeadingSeparator = new Path(new Path(destinationExistingToken, destinationAdditionalTokens), fileName);
    Path destination = new Path("/", destinationWithoutLeadingSeparator);

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
    WorkUnitState state = createWorkUnitState(stagingDir, outputDir, cf);
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
  public void testCommitWithAclPreservationWhenAncestorPathsAbsent() throws IOException {
    String fileName = "file";
    // Asemble destination paths
    String destinationExistingToken = "destination";
    String destinationAdditionalTokens = "path";
    Path destinationWithoutLeadingSeparator = new Path(new Path(destinationExistingToken, destinationAdditionalTokens), fileName);
    Path destination = new Path("/", destinationWithoutLeadingSeparator);

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
    CopyableFile cf = createCopyableFile(originFile, destination, "a");

    // create work unit state
    WorkUnitState state = createWorkUnitState(stagingDir, outputDir, cf);
    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")));
    CopySource.serializeCopyEntity(state, cf);
    CopySource.serializeCopyableDataset(state, metadata);
    // create writer
    FileAwareInputStreamDataWriter writer = new FileAwareInputStreamDataWriter(state, fs,1, 0, null);

    // create output of writer.write
    Path writtenFile = writer.getStagingFilePath(cf);
    this.fs.mkdirs(writtenFile.getParent());
    this.fs.createNewFile(writtenFile);
    Path outputRoot = FileAwareInputStreamDataWriter.getPartitionOutputRoot(outputDir, cf.getDatasetAndPartition(metadata));
    Path expectedOutputPath = new Path(outputRoot, destinationWithoutLeadingSeparator);
    // check ancestor path is absent before commit
    Assert.assertFalse(this.fs.exists(expectedOutputPath.getParent().getParent()));
    writer.actualProcessedCopyableFile = Optional.of(cf);
    // commit
    writer.commit();
    // check ancestor path was created as part of commit
    Assert.assertTrue(this.fs.exists(expectedOutputPath.getParent().getParent()));
    verifyAclEntries(writer, this.fs.getPathToAclEntries(), expectedOutputPath, outputDir);
  }

  @Test
  public void testCommitWithAclPreservationWhenAncestorPathsPresent() throws IOException {
    String fileName = "file";
    // Asemble destination paths
    String destinationExistingToken = "destination";
    String destinationAdditionalTokens = "path";
    Path destinationWithoutLeadingSeparator = new Path(new Path(destinationExistingToken, destinationAdditionalTokens), fileName);
    Path destination = new Path("/", destinationWithoutLeadingSeparator);

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
    CopyableFile cf = createCopyableFile(originFile, destination, "a");

    // create work unit state
    WorkUnitState state = createWorkUnitState(stagingDir, outputDir, cf);
    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")));
    CopySource.serializeCopyEntity(state, cf);
    CopySource.serializeCopyableDataset(state, metadata);
    // create writer
    FileAwareInputStreamDataWriter writer = new FileAwareInputStreamDataWriter(state, fs,1, 0, null);

    // create output of writer.write
    Path writtenFile = writer.getStagingFilePath(cf);
    this.fs.mkdirs(writtenFile.getParent());
    this.fs.createNewFile(writtenFile);
    // create existing directories in writer output
    Path outputRoot = FileAwareInputStreamDataWriter.getPartitionOutputRoot(outputDir, cf.getDatasetAndPartition(metadata));
    Path existingOutputPath = new Path(outputRoot, destinationExistingToken);
    Path expectedOutputPath = new Path(outputRoot, destinationWithoutLeadingSeparator);
    // create output path and check ancestor path is present
    this.fs.mkdirs(existingOutputPath);
    Assert.assertTrue(this.fs.exists(expectedOutputPath.getParent().getParent()));
    writer.actualProcessedCopyableFile = Optional.of(cf);
    // commit
    writer.commit();
    verifyAclEntries(writer, this.fs.getPathToAclEntries(), expectedOutputPath, existingOutputPath);
  }

  private void verifyAclEntries(FileAwareInputStreamDataWriter writer, ImmutableMap pathToAclEntries, Path expectedOutputPath, Path ancestorRoot) {
    // fetching and preparing file paths from FileAwareInputStreamDataWriter object
    Path outputDir = writer.outputDir;
    String[] splitExpectedOutputPath = expectedOutputPath.toString().split("output");
    Path dstOutputPath = new Path(outputDir.toString().concat(splitExpectedOutputPath[1])).getParent();

    OwnerAndPermission destinationOwnerAndPermission = writer.actualProcessedCopyableFile.get().getDestinationOwnerAndPermission();
    List<AclEntry> actual = destinationOwnerAndPermission.getAclEntries();
    while (!dstOutputPath.equals(ancestorRoot)) {
      List<AclEntry> expected = (List<AclEntry>) pathToAclEntries.get(dstOutputPath);
      Assert.assertEquals(actual, expected);
      dstOutputPath = dstOutputPath.getParent();
    }
  }

  private WorkUnitState createWorkUnitState(Path stagingDir, Path outputDir, CopyableFile cf) {
    WorkUnitState state = TestUtils.createTestWorkUnitState();
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, stagingDir.toUri().getPath());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, outputDir.toUri().getPath());
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, RandomStringUtils.randomAlphabetic(5));
    return state;
  }

  private CopyableFile createCopyableFile(Path originFile, Path destination, String preserveAttrs) throws IOException {
    FileStatus status = this.fs.getFileStatus(originFile);
    FsPermission readWrite = new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE);
    AclEntry aclEntry = new AclEntry.Builder()
        .setPermission(FsAction.READ_WRITE)
        .setName("test-acl")
        .setScope(AclEntryScope.DEFAULT)
        .setType(AclEntryType.GROUP)
        .build();

    List<AclEntry> aclEntryList = Lists.newArrayList();
    aclEntryList.add(aclEntry);

    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(status.getOwner(), status.getGroup(), readWrite, aclEntryList);
    List<OwnerAndPermission> ancestorOwnerAndPermissions = Lists.newArrayList();
    ancestorOwnerAndPermissions.add(ownerAndPermission);
    ancestorOwnerAndPermissions.add(ownerAndPermission);
    ancestorOwnerAndPermissions.add(ownerAndPermission);

    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/publisher");
    CopyableFile cf = CopyableFile.fromOriginAndDestination(this.fs, status, destination,
            CopyConfiguration.builder(FileSystem.getLocal(new Configuration()), properties).publishDir(new Path("/target"))
                .preserve(PreserveAttributes.fromMnemonicString(preserveAttrs)).build())
        .destinationOwnerAndPermission(ownerAndPermission)
        .ancestorsOwnerAndPermission(ancestorOwnerAndPermissions)
        .build();
    return cf;
  }

  @Test
  public void testAddExecutePermission() {
    String[] setPermissions = {"000", "100", "200", "400", "600", "700", "211", "250"};
    String[] expectPermissions = {"100", "100", "300", "500", "700", "700", "311", "350"};
    String[] stickyBit = {"" ,"1"};
    for (String bit : stickyBit) {
      for (int index = 0; index < setPermissions.length; ++index) {
        Assert.assertEquals(HadoopUtils.addExecutePermissionToOwner(new FsPermission(bit + setPermissions[index])),
                new FsPermission(bit + expectPermissions[index]));
      }
    }
  }

  @Test
  public void testRetryingAfterFailure() throws Exception {
    String streamString1 = "testContents1";
    FileStatus status = fs.getFileStatus(testTempPath);
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(status.getOwner(), status.getGroup(),
        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL), Lists.newArrayList());
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

  @Test
  public void testStateUpdatesAfterWrite() throws Exception {
    // Create test data
    String testContent = "testContentsForStateUpdate";
    byte[] contentBytes = testContent.getBytes(StandardCharsets.UTF_8);
    long expectedSourceSize = contentBytes.length;

    FileStatus status = fs.getFileStatus(testTempPath);
    OwnerAndPermission ownerAndPermission =
        new OwnerAndPermission(status.getOwner(), status.getGroup(), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    CopyableFile cf = CopyableFileUtils.getTestCopyableFile(expectedSourceSize, ownerAndPermission);
    CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(new TestCopyableDataset(new Path("/source")));

    WorkUnitState state = TestUtils.createTestWorkUnitState();
    state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(testTempPath, "staging").toString());
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(testTempPath, "output").toString());
    state.setProp(ConfigurationKeys.WRITER_FILE_PATH, RandomStringUtils.randomAlphabetic(5));
    CopySource.serializeCopyEntity(state, cf);
    CopySource.serializeCopyableDataset(state, metadata);

    FileAwareInputStreamDataWriter dataWriter = new FileAwareInputStreamDataWriter(state, 1, 0);
    FileAwareInputStream fileAwareInputStream = FileAwareInputStream.builder()
        .file(cf)
        .inputStream(new ByteArrayInputStream(contentBytes))
        .build();

    // Write data and verify state updates
    dataWriter.write(fileAwareInputStream);

    // Verify source file size in state
    String sourceSizeKey = FileSizePolicy.BYTES_READ_KEY;
    Assert.assertEquals(state.getPropAsLong(sourceSizeKey), expectedSourceSize,
        "Source file size should be recorded in state");

    // Verify bytes written in state
    String destinationSizeKey = FileSizePolicy.BYTES_WRITTEN_KEY;
    Assert.assertEquals(state.getPropAsLong(destinationSizeKey), expectedSourceSize,
        "Bytes written should match source file size");

  }

  @AfterClass
  public void cleanup() {
    try {
      fs.delete(testTempPath, true);
    } catch (IOException e) {
      // ignore
    }
  }

  /**
   * Test that setPathPermission with removeExistingAcls=true removes existing ACLs before adding new ones.
   */
  @Test
  public void testSetPathPermissionRemovesExistingAcls() throws Exception {
    Path testPath = new Path(testTempPath, "testAclReplacement");
    fs.mkdirs(testPath);
    FileStatus fileStatus = fs.getFileStatus(testPath);
    testPath = fileStatus.getPath();

    // Simulate existing ACLs on target directory
    List<AclEntry> existingAcls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:olduser:rwx", true),
        AclEntry.parseAclEntry("user:anotheruser:r-x", true)
    );
    fs.modifyAclEntries(testPath, existingAcls);
    Assert.assertEquals(existingAcls, fs.getAclEntries(testPath));

    // Set new ACLs with removeExistingAcls=true
    List<AclEntry> newAcls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:newuser:rw-", true)
    );
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(null, null,
        FsPermission.valueOf("drwxr-xr-x"), newAcls);

    FileAwareInputStreamDataWriter.setPathPermission(fs, fileStatus, ownerAndPermission, false, true);

    // Verify old ACLs were removed and only new ACLs exist
    List<AclEntry> resultAcls = fs.getAclEntries(testPath);
    Assert.assertEquals(1, resultAcls.size());
    Assert.assertEquals(newAcls, resultAcls);
    Assert.assertTrue(fs.wasRemoveAclCalled(testPath));
  }

  /**
   * Test that setPathPermission with removeExistingAcls=false does NOT remove existing ACLs.
   * This tests the default behavior where ACLs are added without removal.
   */
  @Test
  public void testSetPathPermissionDoesNotRemoveExistingAcls() throws Exception {
    Path testPath = new Path(testTempPath, "testAclAddition");
    fs.mkdirs(testPath);
    FileStatus fileStatus = fs.getFileStatus(testPath);
    testPath = fileStatus.getPath();

    // Set initial ACLs
    List<AclEntry> initialAcls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:existinguser:rwx", true)
    );
    fs.modifyAclEntries(testPath, initialAcls);

    // Add new ACLs with removeExistingAcls=false
    List<AclEntry> newAcls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:newuser:r-x", true)
    );
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(null, null,
        FsPermission.valueOf("drwxr-xr-x"), newAcls);

    FileAwareInputStreamDataWriter.setPathPermission(fs, fileStatus, ownerAndPermission, false, false);

    // Verify removeAcl was NOT called
    Assert.assertFalse(fs.wasRemoveAclCalled(testPath));
    // Verify new ACLs were added
    List<AclEntry> resultAcls = fs.getAclEntries(testPath);
    Assert.assertNotNull(resultAcls);
    // Should have old as well as new acls
    initialAcls.addAll(newAcls);
    Assert.assertEquals(initialAcls, resultAcls);
  }

  /**
   * Test that setPathPermission with default parameter (no removeExistingAcls) defaults to false.
   */
  @Test
  public void testSetPathPermissionDefaultBehaviorDoesNotRemoveAcls() throws Exception {
    Path testPath = new Path(testTempPath, "testDefaultBehavior");
    fs.mkdirs(testPath);
    FileStatus fileStatus = fs.getFileStatus(testPath);
    testPath = fileStatus.getPath();

    List<AclEntry> acls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:testuser:rwx", true)
    );
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(null, null,
        FsPermission.valueOf("drwxr-xr-x"), acls);

    // Call without removeExistingAcls parameter (should default to false)
    FileAwareInputStreamDataWriter.setPathPermission(fs, fileStatus, ownerAndPermission, false);

    // Verify removeAcl was NOT called (default is false)
    Assert.assertFalse(fs.wasRemoveAclCalled(testPath));
    // Verify ACLs were set (if filesystem supports it)
    List<AclEntry> resultAcls = fs.getAclEntries(testPath);
    if (resultAcls != null) {
      Assert.assertEquals(acls, resultAcls);
    }
  }

  /**
   * Test that setPathPermission works correctly for files (not just directories).
   */
  @Test
  public void testSetPathPermissionForFile() throws Exception {
    Path testPath = new Path(testTempPath, "testFile.txt");
    fs.create(testPath).close();
    FileStatus fileStatus = fs.getFileStatus(testPath);
    testPath = fileStatus.getPath();

    // Set initial ACLs on file
    List<AclEntry> initialAcls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:olduser:rwx", true)
    );
    fs.modifyAclEntries(testPath, initialAcls);

    // Replace with new ACLs
    List<AclEntry> newAcls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:newuser:r--", true)
    );
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(null, null,
        FsPermission.valueOf("-rw-r--r--"), newAcls);

    FileAwareInputStreamDataWriter.setPathPermission(fs, fileStatus, ownerAndPermission, false, true);

    // Verify ACLs were replaced
    Assert.assertTrue(fs.wasRemoveAclCalled(testPath));
    List<AclEntry> resultAcls = fs.getAclEntries(testPath);
    Assert.assertNotNull(resultAcls);
    Assert.assertEquals(newAcls, resultAcls);
  }

  /**
   * Test that setPathPermission handles multiple ACL entries correctly.
   */
  @Test
  public void testSetPathPermissionWithMultipleAclEntries() throws Exception {
    Path testPath = new Path(testTempPath, "testMultipleAcls");
    fs.mkdirs(testPath);
    FileStatus fileStatus = fs.getFileStatus(testPath);
    testPath = fileStatus.getPath();

    // Create multiple ACL entries
    List<AclEntry> multipleAcls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:alice:rwx", true),
        AclEntry.parseAclEntry("user:bob:r-x", true),
        AclEntry.parseAclEntry("user:charlie:rw-", true),
        AclEntry.parseAclEntry("group:developers:rwx", true)
    );
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(null, null,
        FsPermission.valueOf("drwxr-xr-x"), multipleAcls);

    FileAwareInputStreamDataWriter.setPathPermission(fs, fileStatus, ownerAndPermission, false, true);

    // Verify all ACLs were set
    Assert.assertTrue(fs.wasRemoveAclCalled(testPath));
    List<AclEntry> resultAcls = fs.getAclEntries(testPath);
    Assert.assertNotNull(resultAcls);
    Assert.assertEquals(4, resultAcls.size());
    Assert.assertEquals(multipleAcls, resultAcls);
  }

  /**
   * Test that setPathPermission works with directory execute bit handling.
   * Directories without owner execute bit get it added automatically.
   */
  @Test
  public void testSetPathPermissionWithDirectoryExecuteBit() throws Exception {
    Path testPath = new Path(testTempPath, "testDirExecuteBit");
    fs.mkdirs(testPath);
    FileStatus fileStatus = fs.getFileStatus(testPath);
    testPath = fileStatus.getPath();

    List<AclEntry> acls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:testuser:rw-", true)
    );
    // Directory permission without owner execute bit
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(null, null,
        FsPermission.valueOf("drw-rw-rw-"), acls);

    FileAwareInputStreamDataWriter.setPathPermission(fs, fileStatus, ownerAndPermission, false, true);

    // Verify ACLs were set even with execute bit handling
    Assert.assertTrue(fs.wasRemoveAclCalled(testPath));
    List<AclEntry> resultAcls = fs.getAclEntries(testPath);
    Assert.assertNotNull(resultAcls);
    Assert.assertEquals(acls, resultAcls);
  }

  /**
   * Test that setPathPermission correctly handles the scenario where a directory
   * has existing ACLs that need to be completely replaced.
   */
  @Test
  public void testSetPathPermissionReplacesAllExistingAcls() throws Exception {
    Path testPath = new Path(testTempPath, "testCompleteReplacement");
    fs.mkdirs(testPath);
    FileStatus fileStatus = fs.getFileStatus(testPath);
    testPath = fileStatus.getPath();

    // Set multiple existing ACLs
    List<AclEntry> existingAcls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:user1:rwx", true),
        AclEntry.parseAclEntry("user:user2:r-x", true),
        AclEntry.parseAclEntry("group:group1:rwx", true)
    );
    fs.modifyAclEntries(testPath, existingAcls);

    // Replace with completely different ACLs
    List<AclEntry> newAcls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:differentuser:r--", true)
    );
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(null, null,
        FsPermission.valueOf("drwxr-xr-x"), newAcls);

    FileAwareInputStreamDataWriter.setPathPermission(fs, fileStatus, ownerAndPermission, false, true);

    // Verify complete replacement
    Assert.assertTrue(fs.wasRemoveAclCalled(testPath));
    List<AclEntry> resultAcls = fs.getAclEntries(testPath);
    Assert.assertNotNull(resultAcls);
    Assert.assertEquals(1, resultAcls.size());
    Assert.assertEquals(newAcls, resultAcls);
    // Verify none of the old ACLs exist
    for (AclEntry oldAcl : existingAcls) {
      Assert.assertFalse(resultAcls.contains(oldAcl));
    }
  }

  /**
   * Test that permissions are still set correctly when ACL operations are performed.
   */
  @Test
  public void testSetPathPermissionSetsPermissionsCorrectly() throws Exception {
    Path testPath = new Path(testTempPath, "testPermissions");
    fs.mkdirs(testPath);
    FileStatus fileStatus = fs.getFileStatus(testPath);
    testPath = fileStatus.getPath();

    List<AclEntry> acls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:testuser:rwx", true)
    );
    FsPermission permission = FsPermission.valueOf("drwxr-x---");
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(null, null, permission, acls);

    FileAwareInputStreamDataWriter.setPathPermission(fs, fileStatus, ownerAndPermission, false, true);

    // Verify ACLs were set
    Assert.assertTrue(fs.wasRemoveAclCalled(testPath));
    Assert.assertNotNull(fs.getAclEntries(testPath));

    // Verify permissions were also set (LocalFileSystem adds execute bit for directories)
    FileStatus updatedStatus = fs.getFileStatus(testPath);
    Assert.assertTrue(updatedStatus.getPermission().getUserAction().implies(FsAction.EXECUTE));
  }

  /**
   * Test that when destination has ACLs but source has no ACLs (empty list),
   * all ACLs are removed from destination when removeExistingAcls=true.
   * This ensures destination matches source exactly (no ACLs).
   */
  @Test
  public void testSetPathPermissionRemovesDestinationAclsWhenSourceHasNoAcls() throws Exception {
    Path testPath = new Path(testTempPath, "testEmptySourceAcls");
    fs.mkdirs(testPath);
    FileStatus fileStatus = fs.getFileStatus(testPath);
    testPath = fileStatus.getPath();

    // Destination has existing ACLs
    List<AclEntry> existingAcls = Lists.newArrayList(
        AclEntry.parseAclEntry("user:user1:rwx", true),
        AclEntry.parseAclEntry("user:user2:r-x", true),
        AclEntry.parseAclEntry("group:group1:rw-", true)
    );
    fs.modifyAclEntries(testPath, existingAcls);
    Assert.assertEquals(existingAcls, fs.getAclEntries(testPath));

    // Source has no ACLs (empty list)
    List<AclEntry> emptyAcls = Lists.newArrayList();
    OwnerAndPermission ownerAndPermission = new OwnerAndPermission(null, null,
        FsPermission.valueOf("drwxr-xr-x"), emptyAcls);

    // Call with removeExistingAcls=true
    FileAwareInputStreamDataWriter.setPathPermission(fs, fileStatus, ownerAndPermission, false, true);

    // Verify removeAcl was called to clear all ACLs
    Assert.assertTrue(fs.wasRemoveAclCalled(testPath));
    // Verify no ACLs exist on destination (matches source which has no ACLs)
    List<AclEntry> resultAcls = fs.getAclEntries(testPath);
    Assert.assertNull(resultAcls,
        "Destination should have no ACLs when source has no ACLs and removeExistingAcls=true");
  }

  /**
   * Enhanced TestLocalFileSystem to support ACL testing with tracking of removeAcl calls.
   */
  protected class TestLocalFileSystem extends LocalFileSystem {
    private final ConcurrentHashMap<Path, List<AclEntry>> pathToAclEntries = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Path, Boolean> removeAclCalls = new ConcurrentHashMap<>();

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclEntries) {
      // Use computeIfAbsent to reflect true behavior: add to existing ACLs, don't replace
      pathToAclEntries.computeIfAbsent(path, k -> Lists.newArrayList()).addAll(aclEntries);
    }

    @Override
    public void removeAcl(Path path) {
      removeAclCalls.put(path, true);
      pathToAclEntries.remove(path);
    }

    /**
     * Get ACL entries for a given path.
     * @return List of ACL entries, or null if no ACLs are set
     */
    public List<AclEntry> getAclEntries(Path path) {
      return pathToAclEntries.get(path);
    }

    /**
     * Check if removeAcl was called for a given path.
     * @return true if removeAcl was called, false otherwise
     */
    public boolean wasRemoveAclCalled(Path path) {
      return removeAclCalls.getOrDefault(path, false);
    }

    public ImmutableMap<Path, List<AclEntry>> getPathToAclEntries() {
      return ImmutableMap.copyOf(pathToAclEntries);
    }
  }
}