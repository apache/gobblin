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

package org.apache.gobblin.data.management.copy.converter;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jasypt.util.text.BasicTextEncryptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.crypto.EncryptionConfigParser;
import org.apache.gobblin.data.management.copy.CopyableFileUtils;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;


/**
 * Unit tests for {@link DecryptConverter}.
 */
@Test(groups = { "gobblin.data.management.copy.converter", "disabledOnTravis" })
public class DecryptConverterTest {

  private final File masterPwdFile = new File("masterPwd");

  @Test
  public void testConvertGpgRecord() throws Exception {
    final String expectedFileContents = "123456789";
    final String passphrase = "12";
    DecryptConverter converter = new DecryptConverter();
    WorkUnitState workUnitState = new WorkUnitState();

    try {
      setEncryptedPassphrase(passphrase, workUnitState);
      converter.init(workUnitState);

      FileSystem fs = FileSystem.getLocal(new Configuration());

      URL url = getClass().getClassLoader().getResource("decryptConverterTest/decrypt-test.txt.gpg");
      Assert.assertNotNull(url);

      String gpgFilePath = url.getFile();
      try (FSDataInputStream gpgFileInput = fs.open(new Path(gpgFilePath))) {
	      FileAwareInputStream fileAwareInputStream =
	          new FileAwareInputStream(CopyableFileUtils.getTestCopyableFile(), gpgFileInput);

	      Iterable<FileAwareInputStream> iterable =
	          converter.convertRecord("outputSchema", fileAwareInputStream, workUnitState);
	      fileAwareInputStream = Iterables.getFirst(iterable, null);
	      Assert.assertNotNull(fileAwareInputStream);

	      String actual = IOUtils.toString(fileAwareInputStream.getInputStream(), Charsets.UTF_8);
	      Assert.assertEquals(actual, expectedFileContents);
      }
    } finally {
      deleteMasterPwdFile();
      converter.close();
    }
  }

  @Test
  public void testConvertDifferentEncryption()
      throws IOException, DataConversionException {
    final String expectedFileContents = "2345678";

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.getJobState()
        .setProp("converter.encrypt." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "insecure_shift");

    try (DecryptConverter converter = new DecryptConverter()) {
      converter.init(workUnitState);
      FileSystem fs = FileSystem.getLocal(new Configuration());

      URL url = getClass().getClassLoader().getResource("decryptConverterTest/decrypt-test.txt.insecure_shift");
      Assert.assertNotNull(url);

      String testFilePath = url.getFile();
      try (FSDataInputStream testFileInput = fs.open(new Path(testFilePath))) {
        FileAwareInputStream fileAwareInputStream =
            new FileAwareInputStream(CopyableFileUtils.getTestCopyableFile(), testFileInput);
        fileAwareInputStream.getFile().setDestination(new Path("file:///tmp/decrypt-test.txt.insecure_shift"));
        Iterable<FileAwareInputStream> iterable =
            converter.convertRecord("outputSchema", fileAwareInputStream, workUnitState);
        FileAwareInputStream decryptedStream = Iterables.getFirst(iterable, null);
        Assert.assertNotNull(decryptedStream);

        String actual = IOUtils.toString(decryptedStream.getInputStream(), Charsets.UTF_8);
        Assert.assertEquals(actual, expectedFileContents);
        Assert.assertEquals(decryptedStream.getFile().getDestination().getName(), "decrypt-test.txt");
      }
    }
  }

  private void setEncryptedPassphrase(String plainPassphrase, State state) throws IOException {
    String masterPassword = UUID.randomUUID().toString();
    createMasterPwdFile(masterPassword);
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, this.masterPwdFile.toString());
    state.setProp(ConfigurationKeys.ENCRYPT_USE_STRONG_ENCRYPTOR, false);
    BasicTextEncryptor encryptor = new BasicTextEncryptor();
    encryptor.setPassword(masterPassword);
    String encrypted = encryptor.encrypt(plainPassphrase);
    state.setProp("converter.decrypt.passphrase", "ENC(" + encrypted + ")");
  }

  private void createMasterPwdFile(String masterPwd) throws IOException {
    Assert.assertTrue(this.masterPwdFile.createNewFile());

    Files.write(masterPwd, this.masterPwdFile, Charset.defaultCharset());
  }

  private void deleteMasterPwdFile() {
    Assert.assertTrue(this.masterPwdFile.delete());
  }

}
