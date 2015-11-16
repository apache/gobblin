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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.CopyableFileUtils;
import gobblin.data.management.copy.FileAwareInputStream;

import java.io.File;
import java.io.IOException;
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


public class DecryptConverterTest {

  @Test
  public void testConvertRecord() throws Exception {

    final String expectedFileContents = "123456789";
    final String passphrase = "12";
    DecryptConverter converter = new DecryptConverter();
    WorkUnitState workUnitState = new WorkUnitState();
    setEncyptedPassphrase(passphrase, workUnitState);
    converter.init(workUnitState);

    FileSystem fs = FileSystem.getLocal(new Configuration());

    String gpgFilePath = getClass().getClassLoader().getResource("decryptConverterTest/decrypt-test.txt.gpg").getFile();
    FileAwareInputStream fileAwareInputStream = new FileAwareInputStream(CopyableFileUtils.getTestCopyableFile(), fs.open(new Path(gpgFilePath)));

    Iterable<FileAwareInputStream> iterable =
        converter.convertRecord("outputSchema", fileAwareInputStream, workUnitState);

    String actual = IOUtils.toString(Iterables.getFirst(iterable, null).getInputStream());
    Assert.assertEquals(actual, expectedFileContents);

  }

  private void setEncyptedPassphrase(String plainPassphrase, State state) throws IOException {
    String masterPassword = UUID.randomUUID().toString();
    File masterPwdFile = getMasterPwdFile(masterPassword);
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, masterPwdFile.toString());
    state.setProp(ConfigurationKeys.ENCRYPT_USE_STRONG_ENCRYPTOR, true);
    StrongTextEncryptor encryptor = new StrongTextEncryptor();
    encryptor.setPassword(masterPassword);
    String encrypted = encryptor.encrypt(plainPassphrase);
    state.setProp("converter.decrypt.passphrase", "ENC(" + encrypted + ")");
  }

  private File getMasterPwdFile(String masterPwd) throws IOException {
    File masterPwdFile = new File("masterPwd");
    masterPwdFile.createNewFile();
    Files.write(masterPwd, masterPwdFile, Charset.defaultCharset());
    return masterPwdFile;
  }
}
