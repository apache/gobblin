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

package gobblin.util;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

import gobblin.configuration.State;


@Test(groups = { "gobblin.util" })
public class HadoopUtilsTest {

  @Test
  public void fsShortSerializationTest() {
    State state = new State();
    short mode = 420;
    FsPermission perms = new FsPermission(mode);

    HadoopUtils.serializeWriterFilePermissions(state, 0, 0, perms);
    FsPermission deserializedPerms = HadoopUtils.deserializeWriterFilePermissions(state, 0, 0);
    Assert.assertEquals(mode, deserializedPerms.toShort());
  }

  @Test
  public void fsOctalSerializationTest() {
    State state = new State();
    String mode = "0755";

    HadoopUtils.setWriterFileOctalPermissions(state, 0, 0, mode);
    FsPermission deserializedPerms = HadoopUtils.deserializeWriterFilePermissions(state, 0, 0);
    Assert.assertEquals(Integer.parseInt(mode, 8), deserializedPerms.toShort());
  }

  @Test
  public void testRenameRecursively() throws Exception {

    final Path hadoopUtilsTestDir = new Path(Files.createTempDir().getAbsolutePath(), "HadoopUtilsTestDir");
    FileSystem fs = FileSystem.getLocal(new Configuration());
    try {
      fs.mkdirs(hadoopUtilsTestDir);

      fs.mkdirs(new Path(hadoopUtilsTestDir, "testRename/a/b/c"));

      fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c"));
      fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c/e"));
      fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c/t1.txt"));
      fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging/a/b/c/e/t2.txt"));

      HadoopUtils.renameRecursively(fs, new Path(hadoopUtilsTestDir, "testRenameStaging"), new Path(hadoopUtilsTestDir, "testRename"));

      Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testRename/a/b/c/t1.txt")));
      Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testRename/a/b/c/e/t2.txt")));
    } finally {
      fs.delete(hadoopUtilsTestDir, true);
    }

  }

  @Test(groups = { "performance" })
  public void testRenamePerformance() throws Exception {

    FileSystem fs = Mockito.mock(FileSystem.class);

    Path sourcePath = new Path("/source");
    Path s1 = new Path(sourcePath, "d1");

    FileStatus[] sourceStatuses = new FileStatus[10000];
    FileStatus[] targetStatuses = new FileStatus[1000];

    for (int i = 0; i < sourceStatuses.length; i++) {
      sourceStatuses[i] = getFileStatus(new Path(s1, "path" + i), false);
    }
    for (int i = 0; i < targetStatuses.length; i++) {
      targetStatuses[i] = getFileStatus(new Path(s1, "path" + i), false);
    }

    Mockito.when(fs.getUri()).thenReturn(new URI("file:///"));
    Mockito.when(fs.getFileStatus(sourcePath)).thenAnswer(getDelayedAnswer(getFileStatus(sourcePath, true)));
    Mockito.when(fs.exists(sourcePath)).thenAnswer(getDelayedAnswer(true));
    Mockito.when(fs.listStatus(sourcePath)).thenAnswer(getDelayedAnswer(new FileStatus[]{getFileStatus(s1, true)}));
    Mockito.when(fs.exists(s1)).thenAnswer(getDelayedAnswer(true));
    Mockito.when(fs.listStatus(s1)).thenAnswer(getDelayedAnswer(sourceStatuses));

    Path target = new Path("/target");
    Path s1Target = new Path(target, "d1");
    Mockito.when(fs.exists(target)).thenAnswer(getDelayedAnswer(true));
    Mockito.when(fs.exists(s1Target)).thenAnswer(getDelayedAnswer(true));

    Mockito.when(fs.mkdirs(Mockito.any(Path.class))).thenAnswer(getDelayedAnswer(true));
    Mockito.when(fs.rename(Mockito.any(Path.class), Mockito.any(Path.class))).thenAnswer(getDelayedAnswer(true));

    HadoopUtils.renameRecursively(fs, sourcePath, target);
  }

  private <T> Answer<T> getDelayedAnswer(final T result) throws Exception {
    return new Answer<T>() {
      @Override
      public T answer(InvocationOnMock invocation)
          throws Throwable {
        Thread.sleep(50);
        return result;
      }
    };
  }

  private FileStatus getFileStatus(Path path, boolean dir) {
    return new FileStatus(1, dir, 1, 1, 1, path);
  }

  @Test
  public void testSafeRenameRecursively() throws Exception {
    final Logger log = LoggerFactory.getLogger("HadoopUtilsTest.testSafeRenameRecursively");

    final Path hadoopUtilsTestDir = new Path(Files.createTempDir().getAbsolutePath(), "HadoopUtilsTestDir");
    final FileSystem fs = FileSystem.getLocal(new Configuration());
    try {
      // do many iterations to catch rename race conditions
      for (int i = 0; i < 100; i++) {
        fs.mkdirs(hadoopUtilsTestDir);
        fs.mkdirs(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c"));

        fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging1/a/b/c"));
        fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging1/a/b/c/e"));
        fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging1/a/b/c/t1.txt"));
        fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging1/a/b/c/e/t2.txt"));

        fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging2/a/b/c"));
        fs.mkdirs(new Path(hadoopUtilsTestDir, "testRenameStaging2/a/b/c/e"));
        fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging2/a/b/c/t3.txt"));
        fs.create(new Path(hadoopUtilsTestDir, "testRenameStaging2/a/b/c/e/t4.txt"));

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        final Throwable[] runnableErrors = {null, null};

        Future<?> renameFuture = executorService.submit(new Runnable() {

          @Override
          public void run() {
            try {
              HadoopUtils.renameRecursively(fs, new Path(hadoopUtilsTestDir, "testRenameStaging1"), new Path(
                  hadoopUtilsTestDir, "testSafeRename"));
            } catch (Throwable e) {
              log.error("Rename error: " + e, e);
              runnableErrors[0] = e;
            }
          }
        });

        Future<?> safeRenameFuture = executorService.submit(new Runnable() {

          @Override
          public void run() {
            try {
              HadoopUtils.safeRenameRecursively(fs, new Path(hadoopUtilsTestDir, "testRenameStaging2"), new Path(
                  hadoopUtilsTestDir, "testSafeRename"));
            } catch (Throwable e) {
              log.error("Safe rename error: " + e, e);
              runnableErrors[1] = e;
            }
          }
        });

        // Wait for the executions to complete
        renameFuture.get(10, TimeUnit.SECONDS);
        safeRenameFuture.get(10, TimeUnit.SECONDS);

        executorService.shutdownNow();

        Assert.assertNull(runnableErrors[0], "Runnable 0 error: " + runnableErrors[0]);
        Assert.assertNull(runnableErrors[1], "Runnable 1 error: " + runnableErrors[1]);

        Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/t1.txt")));
        Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/t3.txt")));
        Assert.assertTrue(!fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/e/e/t2.txt")));
        Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/e/t2.txt")));
        Assert.assertTrue(fs.exists(new Path(hadoopUtilsTestDir, "testSafeRename/a/b/c/e/t4.txt")));
        fs.delete(hadoopUtilsTestDir, true);
      }
    } finally {
      fs.delete(hadoopUtilsTestDir, true);
    }

  }

  @Test
  public void testSanitizePath() throws Exception {
    Assert.assertEquals(HadoopUtils.sanitizePath("/A:B/::C:::D\\", "abc"), "/AabcB/abcabcCabcabcabcDabc");
    Assert.assertEquals(HadoopUtils.sanitizePath(":\\:\\/", ""), "/");
    try {
      HadoopUtils.sanitizePath("/A:B/::C:::D\\", "a:b");
      throw new RuntimeException();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("substitute contains illegal characters"));
    }
  }

  @Test
  public void testStateToConfiguration() throws IOException {
    Map<String, String> vals = Maps.newHashMap();
    vals.put("test_key1", "test_val1");
    vals.put("test_key2", "test_val2");

    Configuration expected = HadoopUtils.newConfiguration();
    State state = new State();
    for (Map.Entry<String, String> entry : vals.entrySet()) {
      state.setProp(entry.getKey(), entry.getValue());
      expected.set(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(HadoopUtils.getConfFromState(state), expected);
    Assert.assertEquals(HadoopUtils.getConfFromState(state, Optional.<String>absent()), expected);
    Assert.assertEquals(HadoopUtils.getConfFromState(state, Optional.of("dummy")), expected);
  }

  @Test
  public void testEncryptedStateToConfiguration() throws IOException {
    Map<String, String> vals = Maps.newHashMap();
    vals.put("test_key1", "test_val1");
    vals.put("test_key2", "test_val2");

    State state = new State();
    for (Map.Entry<String, String> entry : vals.entrySet()) {
      state.setProp(entry.getKey(), entry.getValue());
    }

    Map<String, String> encryptedVals = Maps.newHashMap();
    encryptedVals.put("key1", "val1");
    encryptedVals.put("key2", "val2");

    final String encryptedPath = "encrypted.name.space";
    for (Map.Entry<String, String> entry : encryptedVals.entrySet()) {
      state.setProp(encryptedPath + "." + entry.getKey(), entry.getValue());
    }

    Configuration configuration = HadoopUtils.getConfFromState(state, Optional.of(encryptedPath));

    for (Map.Entry<String, String> entry : vals.entrySet()) {
      String val = configuration.get(entry.getKey());
      Assert.assertEquals(val, entry.getValue());
    }

    for (Map.Entry<String, String> entry : encryptedVals.entrySet()) {
      Assert.assertNotNull(configuration.get(entry.getKey())); //Verify key with child path exist as decryption is unit tested in ConfigUtil.
    }
  }
}
