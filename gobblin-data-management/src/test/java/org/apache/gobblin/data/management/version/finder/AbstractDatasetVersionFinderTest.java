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
package org.apache.gobblin.data.management.version.finder;

import com.typesafe.config.ConfigFactory;

import java.io.IOException;

import org.apache.gobblin.data.management.version.TimestampedDatasetVersion;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


@Test(groups = {"gobblin.data.management.version"})
public class AbstractDatasetVersionFinderTest {

  private FileSystem fs = Mockito.mock(FileSystem.class);

  private AbstractDatasetVersionFinder _abstractDatasetVersionFinder =
      new GlobModTimeDatasetVersionFinder(fs, ConfigFactory.empty());

  @Test
  void testGetDatasetVersionIterator()
      throws IOException {
    Path root = new Path("/root");
    String globPattern = "/root/.*/.*";

    FileStatus dir1 = new FileStatus(0, true, 1, 0, 123141, new Path("/root/dir1"));
    FileStatus dir2 = new FileStatus(0, true, 1, 0, 124124, new Path("/root/dir1/dir2"));

    RemoteIterator<FileStatus> rootIterator = Mockito.mock(RemoteIterator.class);
    when(fs.listStatusIterator(root)).thenReturn(rootIterator);
    when(rootIterator.hasNext()).thenReturn(true, false);
    when(rootIterator.next()).thenReturn(dir1);

    RemoteIterator<FileStatus> dir1Iterator = Mockito.mock(RemoteIterator.class);
    when(fs.listStatusIterator(dir1.getPath())).thenReturn(dir1Iterator);
    when(dir1Iterator.hasNext()).thenReturn(true, false);
    when(dir1Iterator.next()).thenReturn(dir2);

    RemoteIterator<FileStatus> dir2Iterator = Mockito.mock(RemoteIterator.class);
    when(fs.listStatusIterator(dir2.getPath())).thenReturn(dir2Iterator);
    when(dir2Iterator.hasNext()).thenReturn(false);

    when(fs.getFileStatus(dir1.getPath())).thenReturn(dir1);
    when(fs.getFileStatus(dir2.getPath())).thenReturn(dir2);

    RemoteIterator<TimestampedDatasetVersion> resultIterator =
        _abstractDatasetVersionFinder.getDatasetVersionIterator(root, globPattern);

    // Verify result
    Assert.assertTrue(resultIterator.hasNext());
    Assert.assertEquals(dir2.getPath(), resultIterator.next().getPath());
    Assert.assertFalse(resultIterator.hasNext());

    // Verify interactions with mocks
    verify(fs, times(1)).listStatusIterator(root);
    verify(rootIterator, times(2)).hasNext();
    verify(rootIterator, times(1)).next();
  }
}
