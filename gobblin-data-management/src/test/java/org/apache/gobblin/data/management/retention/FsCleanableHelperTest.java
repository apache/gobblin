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
package org.apache.gobblin.data.management.retention;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.data.management.retention.dataset.FsCleanableHelper;
import org.apache.gobblin.data.management.trash.MockTrash;
import org.apache.gobblin.data.management.version.FileSystemDatasetVersion;
import org.apache.gobblin.dataset.FileSystemDataset;


@Slf4j
@Test(groups = { "gobblin.data.management.retention" })
public class FsCleanableHelperTest {
  private FileSystem fs;
  private Path testTempPath;

  @BeforeClass
  public void setup() throws Exception {
    this.fs = FileSystem.getLocal(new Configuration());
    this.testTempPath = new Path(Files.createTempDir().getAbsolutePath(), "FsCleanableHelperTest");
    this.fs.mkdirs(this.testTempPath);
  }

  @AfterClass
  public void cleanUp() {
    try {
      fs.delete(this.testTempPath, true);
    } catch (Exception e) {
      // ignore
    }
  }

  @Test
  public void testSimulateTrash() throws Exception {

    Properties props = new Properties();
    props.setProperty(FsCleanableHelper.SIMULATE_KEY, Boolean.toString(true));
    FsCleanableHelper fsCleanableHelper = new FsCleanableHelper(this.fs, props, ConfigFactory.empty(), log);

    assertThat(fsCleanableHelper.getTrash(), instanceOf(MockTrash.class));
  }

  @Test
  public void testDeleteEmptyDirs() throws Exception {
    Properties props = new Properties();
    props.setProperty(FsCleanableHelper.SKIP_TRASH_KEY, Boolean.toString(true));
    FsCleanableHelper fsCleanableHelper = new FsCleanableHelper(this.fs, props, ConfigFactory.empty(), log);
    FileSystemDataset fsDataset = mock(FileSystemDataset.class);
    Path datasetRoot = new Path(testTempPath, "dataset1");
    when(fsDataset.datasetRoot()).thenReturn(datasetRoot);

    // To delete
    Path deleted1 = new Path(datasetRoot, "2016/01/01/13");
    Path deleted2 = new Path(datasetRoot, "2016/01/01/14");
    Path deleted3 = new Path(datasetRoot, "2016/01/02/15");

    // Do not delete
    Path notDeleted1 = new Path(datasetRoot, "2016/01/02/16");

    this.fs.mkdirs(deleted1);
    this.fs.mkdirs(deleted2);
    this.fs.mkdirs(deleted3);
    this.fs.mkdirs(notDeleted1);

    // Make sure all paths are created
    Assert.assertTrue(this.fs.exists(deleted1));
    Assert.assertTrue(this.fs.exists(deleted2));
    Assert.assertTrue(this.fs.exists(deleted3));
    Assert.assertTrue(this.fs.exists(notDeleted1));

    List<FileSystemDatasetVersion> deletableVersions = ImmutableList.<FileSystemDatasetVersion> of(
            new MockFileSystemDatasetVersion(deleted1),
            new MockFileSystemDatasetVersion(deleted2),
            new MockFileSystemDatasetVersion(deleted3));

    fsCleanableHelper.clean(deletableVersions, fsDataset);

    // Verify versions are deleted
    Assert.assertFalse(this.fs.exists(deleted1));
    Assert.assertFalse(this.fs.exists(deleted2));
    Assert.assertFalse(this.fs.exists(deleted3));

    // Verify versions are not deleted
    Assert.assertTrue(this.fs.exists(notDeleted1));

    // Verify empty parent dir "2016/01/01" is deleted
    Assert.assertFalse(this.fs.exists(deleted1.getParent()));

    // Verify non empty parent dir "2016/01/02" exists
    Assert.assertTrue(this.fs.exists(notDeleted1.getParent()));
  }

  @AllArgsConstructor
  private static class MockFileSystemDatasetVersion implements FileSystemDatasetVersion {

    @Getter
    private final Path path;

    @Override
    public Object getVersion() {
      return null;
    }

    @Override
    public int compareTo(FileSystemDatasetVersion o) {
      return 0;
    }

    @Override
    public Set<Path> getPaths() {
      return Sets.newHashSet(this.path);
    }

  }
}
