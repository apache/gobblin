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

package gobblin.data.management.copy.recovery;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.io.Files;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.PreserveAttributes;
import gobblin.util.guid.Guid;


public class RecoveryHelperTest {

  private File tmpDir;

  @BeforeMethod public void setUp() throws Exception {
    this.tmpDir = Files.createTempDir();
    this.tmpDir.deleteOnExit();
  }

  @Test public void testGetPersistDir() throws Exception {

    State state = new State();

    Assert.assertFalse(RecoveryHelper.getPersistDir(state).isPresent());

    state.setProp(RecoveryHelper.PERSIST_DIR_KEY, this.tmpDir.getAbsolutePath());

    Assert.assertTrue(RecoveryHelper.getPersistDir(state).isPresent());
    Assert.assertTrue(RecoveryHelper.getPersistDir(state).get().toUri().getPath().
        startsWith(this.tmpDir.getAbsolutePath()));

  }

  @Test public void testPersistFile() throws Exception {

    String content = "contents";

    File stagingDir = Files.createTempDir();
    stagingDir.deleteOnExit();

    File file = new File(stagingDir, "file");
    OutputStream os = new FileOutputStream(file);
    IOUtils.write(content, os);
    os.close();

    Assert.assertEquals(stagingDir.listFiles().length, 1);

    State state = new State();
    state.setProp(RecoveryHelper.PERSIST_DIR_KEY, this.tmpDir.getAbsolutePath());
    state.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/publisher");

    File recoveryDir = new File(RecoveryHelper.getPersistDir(state).get().toUri().getPath());

    FileSystem fs = FileSystem.getLocal(new Configuration());

    CopyableFile copyableFile = CopyableFile.builder(fs,
        new FileStatus(0, false, 0, 0, 0, new Path("/file")), new Path("/dataset"),
        CopyConfiguration.builder(fs, state.getProperties()).preserve(PreserveAttributes.fromMnemonicString("")).build()).build();

    CopySource.setWorkUnitGuid(state, Guid.fromHasGuid(copyableFile));

    RecoveryHelper recoveryHelper = new RecoveryHelper(FileSystem.getLocal(new Configuration()), state);

    recoveryHelper.persistFile(state, copyableFile, new Path(file.getAbsolutePath()));

    Assert.assertEquals(stagingDir.listFiles().length, 0);
    Assert.assertEquals(recoveryDir.listFiles().length, 1);

    File fileInRecovery = recoveryDir.listFiles()[0];
    Assert.assertEquals(IOUtils.readLines(new FileInputStream(fileInRecovery)).get(0), content);

    Optional<FileStatus> fileToRecover =
        recoveryHelper.findPersistedFile(state, copyableFile, Predicates.<FileStatus>alwaysTrue());
    Assert.assertTrue(fileToRecover.isPresent());
    Assert.assertEquals(fileToRecover.get().getPath().toUri().getPath(), fileInRecovery.getAbsolutePath());

    fileToRecover =
        recoveryHelper.findPersistedFile(state, copyableFile, Predicates.<FileStatus>alwaysFalse());
    Assert.assertFalse(fileToRecover.isPresent());

  }

  @Test
  public void testPurge() throws Exception {
    String content = "contents";

    File persistDirBase = Files.createTempDir();
    persistDirBase.deleteOnExit();

    State state = new State();
    state.setProp(RecoveryHelper.PERSIST_DIR_KEY, persistDirBase.getAbsolutePath());
    state.setProp(RecoveryHelper.PERSIST_RETENTION_KEY, "1");

    RecoveryHelper recoveryHelper = new RecoveryHelper(FileSystem.getLocal(new Configuration()), state);
    File persistDir = new File(RecoveryHelper.getPersistDir(state).get().toString());
    persistDir.mkdir();

    File file = new File(persistDir, "file1");
    OutputStream os = new FileOutputStream(file);
    IOUtils.write(content, os);
    os.close();
    file.setLastModified(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2));

    File file2 = new File(persistDir, "file2");
    OutputStream os2 = new FileOutputStream(file2);
    IOUtils.write(content, os2);
    os2.close();

    Assert.assertEquals(persistDir.listFiles().length, 2);

    recoveryHelper.purgeOldPersistedFile();

    Assert.assertEquals(persistDir.listFiles().length, 1);
  }

  @Test public void testShortenPathName() throws Exception {

    Assert.assertEquals(RecoveryHelper.shortenPathName(new Path("/test"), 10), "_test");
    Assert.assertEquals(RecoveryHelper.shortenPathName(new Path("/relatively/long/path"), 9), "_re...ath");

  }
}
