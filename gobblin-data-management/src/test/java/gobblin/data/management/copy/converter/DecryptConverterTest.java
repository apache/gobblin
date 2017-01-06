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

package gobblin.data.management.copy.converter;

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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopyableFileUtils;
import gobblin.data.management.copy.FileAwareInputStream;


/**
 * Unit tests for {@link DecryptConverter}.
 */
@Test(groups = { "gobblin.data.management.copy.converter", "disabledOnTravis" })
public class DecryptConverterTest {

  private final File masterPwdFile = new File("masterPwd");

  @Test
  public void testConvertRecord() throws Exception {

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

      String gpgFilePath =url.getFile();
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
