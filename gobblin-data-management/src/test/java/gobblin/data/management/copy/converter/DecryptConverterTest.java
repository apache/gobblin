/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy.converter;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jasypt.util.text.StrongTextEncryptor;
import org.testng.Assert;
import org.testng.annotations.Test;

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
@Test(groups = { "gobblin.data.management.copy.converter" })
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
      if (url == null) {
        Assert.fail();
      }

      String gpgFilePath =url.getFile();
      FileAwareInputStream fileAwareInputStream =
          new FileAwareInputStream(CopyableFileUtils.getTestCopyableFile(), fs.open(new Path(gpgFilePath)));

      Iterable<FileAwareInputStream> iterable =
          converter.convertRecord("outputSchema", fileAwareInputStream, workUnitState);
      fileAwareInputStream = Iterables.getFirst(iterable, null);
      if (fileAwareInputStream == null) {
        Assert.fail();
      }

      String actual = IOUtils.toString(fileAwareInputStream.getInputStream());
      Assert.assertEquals(actual, expectedFileContents);
    } finally {
      deleteMasterPwdFile();
    }
  }

  private void setEncryptedPassphrase(String plainPassphrase, State state) throws IOException {
    String masterPassword = UUID.randomUUID().toString();
    createMasterPwdFile(masterPassword);
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, this.masterPwdFile.toString());
    state.setProp(ConfigurationKeys.ENCRYPT_USE_STRONG_ENCRYPTOR, true);
    StrongTextEncryptor encryptor = new StrongTextEncryptor();
    encryptor.setPassword(masterPassword);
    String encrypted = encryptor.encrypt(plainPassphrase);
    state.setProp("converter.decrypt.passphrase", "ENC(" + encrypted + ")");
  }

  private void createMasterPwdFile(String masterPwd) throws IOException {
    if (!this.masterPwdFile.createNewFile()) {
      Assert.fail();
    }

    Files.write(masterPwd, this.masterPwdFile, Charset.defaultCharset());
  }

  private void deleteMasterPwdFile() {
    if (!this.masterPwdFile.delete()) {
      Assert.fail();
    }
  }
}
